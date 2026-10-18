package redis

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// scriptedHookServer is a minimal RESP2 server whose reply to a command can
// be scripted per (command name, global receipt index). The special reply
// hookReplyClose tears down the connection right after the command was read,
// simulating a network failure with no reply. Every command name received is
// recorded (indexed globally across connections).
const hookReplyClose = "__close__"

type scriptedHookServer struct {
	t  *testing.T
	ln net.Listener

	mu      sync.Mutex
	started bool
	seq     map[string]int
	log     []string
	conns   []net.Conn

	reply func(name string, args []string, nth int) string

	wg sync.WaitGroup
}

func newScriptedHookServer(t *testing.T, reply func(name string, args []string, nth int) string) *scriptedHookServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &scriptedHookServer{t: t, ln: ln, seq: map[string]int{}, reply: reply}
	s.start()
	return s
}

func (s *scriptedHookServer) start() {
	s.mu.Lock()
	if s.started {
		s.mu.Unlock()
		return
	}
	s.started = true
	s.mu.Unlock()

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			c, err := s.ln.Accept()
			if err != nil {
				return
			}
			s.mu.Lock()
			s.conns = append(s.conns, c)
			s.mu.Unlock()
			s.wg.Add(1)
			go func() {
				defer s.wg.Done()
				s.serve(c)
			}()
		}
	}()
}

func (s *scriptedHookServer) addr() string { return s.ln.Addr().String() }

// dialAddr advertises the listener as localhost:port. An artificial cluster
// built via ClusterSlots with no origin rewrites literal loopback IPs, while
// hostnames are left untouched.
func (s *scriptedHookServer) dialAddr() string {
	_, port, _ := net.SplitHostPort(s.ln.Addr().String())
	return "localhost:" + port
}

func (s *scriptedHookServer) close() {
	_ = s.ln.Close()
	s.mu.Lock()
	for _, c := range s.conns {
		_ = c.Close()
	}
	s.mu.Unlock()
	s.wg.Wait()
}

func (s *scriptedHookServer) count(name string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.seq[strings.ToUpper(name)]
}

func (s *scriptedHookServer) debugLog() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, len(s.log))
	copy(out, s.log)
	return out
}

func (s *scriptedHookServer) serve(c net.Conn) {
	defer c.Close()
	r := bufio.NewReader(c)
	inMulti := false
	var queued [][]string
	for {
		args, err := readRESPCommand(r)
		if err != nil {
			return
		}
		if len(args) == 0 {
			continue
		}
		name := strings.ToUpper(args[0])

		s.mu.Lock()
		s.seq[name]++
		nth := s.seq[name]
		s.log = append(s.log, strings.Join(upperArgs(args), " "))
		s.mu.Unlock()

		if name == "HELLO" {
			// RESP2 fallback: HELLO is rejected with a Redis error, init
			// continues in RESP2.
			if _, err := io.WriteString(c, "-ERR unknown command 'HELLO'\r\n"); err != nil {
				return
			}
			continue
		}

		// Transaction framing: MULTI -> +OK; queued commands -> +QUEUED;
		// EXEC -> array with one +OK reply per queued command.
		if inMulti {
			if name == "EXEC" {
				var b strings.Builder
				fmt.Fprintf(&b, "*%d\r\n", len(queued))
				for range queued {
					b.WriteString("+OK\r\n")
				}
				if _, err := io.WriteString(c, b.String()); err != nil {
					return
				}
				inMulti = false
				queued = nil
				continue
			}
			if name == "DISCARD" {
				inMulti = false
				queued = nil
				if _, err := io.WriteString(c, "+OK\r\n"); err != nil {
					return
				}
				continue
			}
			// The scripted reply applies even to a queued command: a close
			// reply tears the connection down while it is inside MULTI.
			if reply := s.reply(name, args, nth); reply == hookReplyClose {
				return
			}
			queued = append(queued, args)
			if _, err := io.WriteString(c, "+QUEUED\r\n"); err != nil {
				return
			}
			continue
		}
		if name == "MULTI" {
			inMulti = true
			if _, err := io.WriteString(c, "+OK\r\n"); err != nil {
				return
			}
			continue
		}

		reply := s.reply(name, args, nth)
		if reply == hookReplyClose {
			// Close without replying: the client read fails with EOF.
			return
		}
		if _, err := io.WriteString(c, reply); err != nil {
			return
		}
	}
}

func upperArgs(args []string) []string {
	out := make([]string, len(args))
	for i, a := range args {
		out[i] = strings.ToUpper(a)
	}
	return out
}

func bulkString(s string) string {
	return "$" + strconv.Itoa(len(s)) + "\r\n" + s + "\r\n"
}

func newHookRetryClient(t *testing.T, addr string, opts ...func(*Options)) *Client {
	t.Helper()
	opt := &Options{
		Addr:            addr,
		Protocol:        2,
		DisableIdentity: true,
		MaxRetries:      0,
		MinRetryBackoff: time.Millisecond,
		MaxRetryBackoff: 2 * time.Millisecond,
		PoolSize:        1,
	}
	for _, fn := range opts {
		fn(opt)
	}
	return NewClient(opt)
}

// retryDecisionHook is a ProcessHook that asks the driver for a hook retry
// (by returning *RetryableError) while decide returns true. It records every
// hook entry and FIFO start/end events so tests can assert that retries pass
// through the hook chain again.
type retryDecisionHook struct {
	mu      sync.Mutex
	entries int
	events  []string
	decide  func(nextErr error, entry int) bool
}

func (h *retryDecisionHook) DialHook(next DialHook) DialHook { return next }

func (h *retryDecisionHook) ProcessPipelineHook(next ProcessPipelineHook) ProcessPipelineHook {
	return next
}

func (h *retryDecisionHook) ProcessHook(next ProcessHook) ProcessHook {
	return func(ctx context.Context, cmd Cmder) error {
		h.mu.Lock()
		h.entries++
		entry := h.entries
		h.events = append(h.events, fmt.Sprintf("start-%d", entry))
		h.mu.Unlock()

		err := next(ctx, cmd)

		h.mu.Lock()
		h.events = append(h.events, fmt.Sprintf("end-%d", entry))
		decide := h.decide
		h.mu.Unlock()

		if decide != nil && decide(err, entry) {
			return NewRetryableError(err)
		}
		return err
	}
}

func (h *retryDecisionHook) snapshot() (int, []string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	events := make([]string, len(h.events))
	copy(events, h.events)
	return h.entries, events
}

// Always-retry pipeline/tx hook: returns RetryableError after every batch.
type retryPipelineHook struct {
	entries atomic.Int32
}

func (h *retryPipelineHook) DialHook(next DialHook) DialHook { return next }
func (h *retryPipelineHook) ProcessHook(next ProcessHook) ProcessHook {
	return next
}
func (h *retryPipelineHook) ProcessPipelineHook(next ProcessPipelineHook) ProcessPipelineHook {
	return func(ctx context.Context, cmds []Cmder) error {
		h.entries.Add(1)
		err := next(ctx, cmds)
		return NewRetryableError(err)
	}
}

// retryPipelineHookOnError wraps only a failed batch in RetryableError; a
// successful batch stays successful.
type retryPipelineHookOnError struct{}

func (*retryPipelineHookOnError) DialHook(next DialHook) DialHook { return next }
func (*retryPipelineHookOnError) ProcessHook(next ProcessHook) ProcessHook {
	return next
}
func (*retryPipelineHookOnError) ProcessPipelineHook(next ProcessPipelineHook) ProcessPipelineHook {
	return func(ctx context.Context, cmds []Cmder) error {
		if err := next(ctx, cmds); err != nil {
			return NewRetryableError(err)
		}
		return nil
	}
}

// TestHookRetryableRetriesOnceThroughHooks: a Redis error reply that the
// driver does not retry itself is turned into a retry by the hook; the
// command is sent exactly one more time, through the hooks again, and the
// second reply is adopted.
func TestHookRetryableRetriesOnceThroughHooks(t *testing.T) {
	srv := newScriptedHookServer(t, func(name string, args []string, nth int) string {
		if name == "GET" && nth == 1 {
			return "-ERR synthetic transient failure\r\n"
		}
		if name == "GET" {
			return bulkString("v2")
		}
		return "+OK\r\n"
	})
	defer srv.close()

	c := newHookRetryClient(t, srv.addr())
	defer c.Close()
	// Initialize the pooled connection (handshake) before installing the
	// hook, so handshake commands are not counted as hook entries.
	if err := c.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("warmup: %v", err)
	}

	hook := &retryDecisionHook{decide: func(err error, entry int) bool {
		// Retry on any error reply from the first pass; adopt the second.
		return err != nil && entry == 1
	}}
	c.AddHook(hook)

	ctx := context.Background()
	got := c.Get(ctx, "k")
	if got.Err() != nil {
		t.Fatalf("command should succeed on the hook retry, got %v", got.Err())
	}
	if got.Val() != "v2" {
		t.Fatalf("only the last execution result must be adopted, got %q", got.Val())
	}
	if srv.count("GET") != 2 {
		t.Fatalf("command must be sent exactly twice, got %d GET receipts", srv.count("GET"))
	}
	entries, events := hook.snapshot()
	if entries != 2 {
		t.Fatalf("the retry must pass through the hook again: got %d hook entries, events=%v wire=%v",
			entries, events, srv.debugLog())
	}
	wantEvents := []string{"start-1", "end-1", "start-2", "end-2"}
	if len(events) != len(wantEvents) {
		t.Fatalf("hook events = %v, want %v", events, wantEvents)
	}
	for i := range wantEvents {
		if events[i] != wantEvents[i] {
			t.Fatalf("hook events = %v, want %v", events, wantEvents)
		}
	}
}

// TestHookRetryableShortCircuitFirstCall: a hook that never calls next but
// returns RetryableError causes the command to be executed once; the retry
// pass calls next and the command runs.
func TestHookRetryableShortCircuitFirstCall(t *testing.T) {
	srv := newScriptedHookServer(t, func(name string, _ []string, _ int) string {
		if name == "GET" {
			return bulkString("value")
		}
		return "+OK\r\n"
	})
	defer srv.close()

	c := newHookRetryClient(t, srv.addr())
	defer c.Close()
	if err := c.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("warmup: %v", err)
	}
	c.AddHook(&shortCircuitRetryHook{})

	ctx := context.Background()
	got := c.Get(ctx, "k")
	if got.Err() != nil {
		t.Fatalf("got %v", got.Err())
	}
	if got.Val() != "value" {
		t.Fatalf("got %q", got.Val())
	}
	if srv.count("GET") != 1 {
		t.Fatalf("exactly one wire execution expected, got %d", srv.count("GET"))
	}
}

type shortCircuitRetryHook struct {
	// The hook chain is rebuilt (ProcessHook factory re-invoked) for every
	// driver pass, so the pass counter must live on the hook itself rather
	// than in a factory closure.
	calls atomic.Int32
}

func (*shortCircuitRetryHook) DialHook(next DialHook) DialHook { return next }
func (*shortCircuitRetryHook) ProcessPipelineHook(next ProcessPipelineHook) ProcessPipelineHook {
	return next
}
func (h *shortCircuitRetryHook) ProcessHook(next ProcessHook) ProcessHook {
	return func(ctx context.Context, cmd Cmder) error {
		if h.calls.Add(1) == 1 {
			// Deliberately terminate without calling next; ask the driver to
			// run the command once.
			return NewRetryableError(nil)
		}
		return next(ctx, cmd)
	}
}

// TestHookRetryableRefusedAfterSuccess: once a successful reply was read, a
// hook returning RetryableError must never cause a second send; the success
// is kept.
func TestHookRetryableRefusedAfterSuccess(t *testing.T) {
	srv := newScriptedHookServer(t, func(name string, _ []string, _ int) string {
		if name == "GET" {
			return bulkString("ok")
		}
		return "+OK\r\n"
	})
	defer srv.close()

	c := newHookRetryClient(t, srv.addr())
	defer c.Close()
	if err := c.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("warmup: %v", err)
	}
	c.AddHook(&retryDecisionHook{decide: func(error, int) bool { return true }})

	got := c.Get(context.Background(), "k")
	if got.Err() != nil {
		t.Fatalf("successful result must be adopted as-is, got %v", got.Err())
	}
	if got.Val() != "ok" {
		t.Fatalf("got %q", got.Val())
	}
	if srv.count("GET") != 1 {
		t.Fatalf("no resend after a successful reply, got %d GET receipts", srv.count("GET"))
	}
}

// TestHookRetryableNilReplyIsSuccess: redis.Nil is a complete negative
// reply, not a trigger for another send.
func TestHookRetryableNilReplyIsSuccess(t *testing.T) {
	srv := newScriptedHookServer(t, func(name string, _ []string, _ int) string {
		if name == "GET" {
			return "$-1\r\n"
		}
		return "+OK\r\n"
	})
	defer srv.close()

	c := newHookRetryClient(t, srv.addr())
	defer c.Close()
	if err := c.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("warmup: %v", err)
	}
	c.AddHook(&retryDecisionHook{decide: func(error, int) bool { return true }})

	err := c.Get(context.Background(), "missing").Err()
	if !errors.Is(err, Nil) {
		t.Fatalf("want redis.Nil, got %v", err)
	}
	if srv.count("GET") != 1 {
		t.Fatalf("redis.Nil must not be retried, got %d GET receipts", srv.count("GET"))
	}
}

// TestHookRetryableGrantsExactlyOneExtraExecution: a hook that keeps asking
// gets at most one extra execution; the result of the second execution is
// final even if the hook asks again.
func TestHookRetryableGrantsExactlyOneExtraExecution(t *testing.T) {
	srv := newScriptedHookServer(t, func(name string, _ []string, _ int) string {
		if name == "GET" {
			return "-ERR always failing\r\n"
		}
		return "+OK\r\n"
	})
	defer srv.close()

	c := newHookRetryClient(t, srv.addr())
	defer c.Close()
	if err := c.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("warmup: %v", err)
	}
	hook := &retryDecisionHook{decide: func(error, int) bool { return true }}
	c.AddHook(hook)

	err := c.Get(context.Background(), "k").Err()
	if err == nil {
		t.Fatal("expected the second execution's error")
	}
	var redisErr Error
	if !errors.As(err, &redisErr) {
		t.Fatalf("the original Redis error must remain visible through wrapping, got %T %v", err, err)
	}
	if !HasErrorPrefix(err, "always failing") {
		t.Fatalf("want the second execution's redis error, got %v", err)
	}
	if srv.count("GET") != 2 {
		t.Fatalf("exactly two executions (original + one retry), got %d", srv.count("GET"))
	}
	if entries, _ := hook.snapshot(); entries != 2 {
		t.Fatalf("hook runs once per pass, got %d", entries)
	}
}

// TestHookRetryableContextCancelStops: when the context is canceled by the
// hook that asks for a retry, the driver stops and reports the cancellation
// without another send.
func TestHookRetryableContextCancelStops(t *testing.T) {
	srv := newScriptedHookServer(t, func(name string, _ []string, nth int) string {
		if name == "GET" && nth == 1 {
			return "-ERR fail then cancel\r\n"
		}
		return bulkString("never")
	})
	defer srv.close()

	c := newHookRetryClient(t, srv.addr())
	defer c.Close()
	if err := c.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("warmup: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	c.AddHook(&cancelAndRetryHook{cancel: cancel})

	err := c.Get(ctx, "k").Err()
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
	if srv.count("GET") != 1 {
		t.Fatalf("canceled retry must not send again, got %d GET receipts", srv.count("GET"))
	}
}

type cancelAndRetryHook struct {
	cancel context.CancelFunc
}

func (*cancelAndRetryHook) DialHook(next DialHook) DialHook { return next }
func (*cancelAndRetryHook) ProcessPipelineHook(next ProcessPipelineHook) ProcessPipelineHook {
	return next
}
func (h *cancelAndRetryHook) ProcessHook(next ProcessHook) ProcessHook {
	return func(ctx context.Context, cmd Cmder) error {
		if err := next(ctx, cmd); err != nil {
			h.cancel()
			return NewRetryableError(err)
		}
		return nil
	}
}

// TestHookRetryableDoesNotMultiplyNetworkRetries: MaxRetries stays the
// budget for network failures. After the network budget is spent, a hook
// retry adds exactly ONE more wire execution — not a second MaxRetries
// suite. With MaxRetries=2 and the first three receipts failing with EOF,
// total receipts must be four (3 budgeted attempts + 1 hook execution); the
// fourth reply is adopted.
func TestHookRetryableDoesNotMultiplyNetworkRetries(t *testing.T) {
	srv := newScriptedHookServer(t, func(name string, _ []string, nth int) string {
		if name == "GET" {
			if nth <= 3 {
				return hookReplyClose
			}
			return bulkString("fourth")
		}
		return "+OK\r\n"
	})
	defer srv.close()

	c := newHookRetryClient(t, srv.addr(), func(o *Options) {
		o.MaxRetries = 2
		o.PoolSize = 8
	})
	defer c.Close()
	// Wrap every failure (including the exhausted EOF) as hook-retryable.
	c.AddHook(&retryDecisionHook{decide: func(err error, _ int) bool { return err != nil }})

	got := c.Get(context.Background(), "k")
	if got.Err() != nil {
		t.Fatalf("fourth attempt must succeed, got %v", got.Err())
	}
	if got.Val() != "fourth" {
		t.Fatalf("got %q", got.Val())
	}
	if n := srv.count("GET"); n != 4 {
		t.Fatalf("network MaxRetries (3 sends) + exactly one hook execution = 4, got %d", n)
	}
}

// TestHookRetryablePipelineFailsWholeBatch: a pipeline hook returning
// RetryableError fails the whole batch; the batch is not resent and no single
// command is retried.
func TestHookRetryablePipelineFailsWholeBatch(t *testing.T) {
	srv := newScriptedHookServer(t, func(name string, _ []string, _ int) string {
		return "+OK\r\n"
	})
	defer srv.close()

	c := newHookRetryClient(t, srv.addr())
	defer c.Close()
	ph := &retryPipelineHook{}
	c.AddHook(ph)

	ctx := context.Background()
	_, err := c.Pipelined(ctx, func(pipe Pipeliner) error {
		pipe.Set(ctx, "a", "1", 0)
		pipe.Set(ctx, "b", "2", 0)
		return nil
	})
	if err == nil || !isHookRetryableError(err) {
		t.Fatalf("Exec must fail with the hook retryable error, got %v", err)
	}
	if ph.entries.Load() != 1 {
		t.Fatalf("the batch hook runs once; failing batch must not re-enter it, got %d", ph.entries.Load())
	}
	if srv.count("SET") != 2 {
		t.Fatalf("batch must be sent exactly once (2 SET), got %d", srv.count("SET"))
	}
}

// TestHookRetryableTxPipelineFailsWholeBatch: a transaction hook returning
// RetryableError fails the whole transaction and never re-sends MULTI/EXEC.
func TestHookRetryableTxPipelineFailsWholeBatch(t *testing.T) {
	srv := newScriptedHookServer(t, func(name string, _ []string, _ int) string {
		return "+OK\r\n"
	})
	defer srv.close()

	c := newHookRetryClient(t, srv.addr())
	defer c.Close()
	ph := &retryPipelineHook{}
	c.AddHook(ph)

	ctx := context.Background()
	_, err := c.TxPipelined(ctx, func(pipe Pipeliner) error {
		pipe.Set(ctx, "a", "1", 0)
		pipe.Set(ctx, "b", "2", 0)
		return nil
	})
	if err == nil || !isHookRetryableError(err) {
		t.Fatalf("Tx Exec must fail with the hook retryable error, got %v", err)
	}
	if srv.count("MULTI") != 1 || srv.count("EXEC") != 1 {
		t.Fatalf("transaction sent once, got MULTI=%d EXEC=%d", srv.count("MULTI"), srv.count("EXEC"))
	}
	if srv.count("SET") != 2 {
		t.Fatalf("queued SETs sent once, got %d", srv.count("SET"))
	}
}

// TestHookRetryableTxBatchConnDiedInMultiIsDiscarded: when the connection
// drops between MULTI and EXEC, the transaction batch fails, the hook's
// RetryableError is not turned into a resend, and the dead connection is not
// returned to the pool — a later batch runs cleanly on a fresh connection.
func TestHookRetryableTxBatchConnDiedInMultiIsDiscarded(t *testing.T) {
	srv := newScriptedHookServer(t, func(name string, _ []string, nth int) string {
		// The first SET is the one queued inside MULTI on the first
		// connection: tear the connection down so the client reads EOF while
		// still inside MULTI.
		if name == "SET" && nth == 1 {
			return hookReplyClose
		}
		return "+OK\r\n"
	})
	defer srv.close()

	c := newHookRetryClient(t, srv.addr())
	defer c.Close()
	// Only ask for (refused) batch retry when the batch actually failed, so
	// the follow-up successful batch stays successful.
	c.AddHook(&retryPipelineHookOnError{})

	ctx := context.Background()
	_, err := c.TxPipelined(ctx, func(pipe Pipeliner) error {
		pipe.Set(ctx, "a", "1", 0)
		return nil
	})
	if err == nil {
		t.Fatal("the transaction must fail")
	}
	if !isHookRetryableError(err) {
		t.Fatalf("hook error must surface, got %v", err)
	}
	if srv.count("MULTI") != 1 || srv.count("EXEC") != 0 {
		t.Fatalf("MULTI sent once, EXEC never sent; got MULTI=%d EXEC=%d",
			srv.count("MULTI"), srv.count("EXEC"))
	}

	// A subsequent batch must succeed: the conn that died in MULTI was
	// removed, not pooled.
	_, err = c.Pipelined(ctx, func(pipe Pipeliner) error {
		pipe.Set(ctx, "b", "2", 0)
		return nil
	})
	if err != nil {
		t.Fatalf("batch after failed transaction must succeed, got %v", err)
	}
	if srv.count("MULTI") != 1 {
		t.Fatalf("the transaction must never be re-sent, got MULTI=%d", srv.count("MULTI"))
	}
}

// newScriptedCluster builds a ClusterClient whose topology comes from the
// supplied function (called on every state (re)load), pointing at scripted
// in-process servers.
func newScriptedCluster(t *testing.T, topology func() []ClusterSlot) *ClusterClient {
	t.Helper()
	c := NewClusterClient(&ClusterOptions{
		ClusterSlots:    func(context.Context) ([]ClusterSlot, error) { return topology(), nil },
		Protocol:        2,
		DisableIdentity: true,
		MaxRetries:      1,
		MinRetryBackoff: time.Millisecond,
		MaxRetryBackoff: 2 * time.Millisecond,
	})
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func clusterSlotsAll(addr string) []ClusterSlot {
	return []ClusterSlot{{
		Start: 0,
		End:   16383,
		Nodes: []ClusterNode{{Addr: addr}},
	}}
}

// TestHookRetryableClusterRetriesAtCurrentSlot: on a ClusterClient the hook
// retry re-selects the node from the slot and goes through the hooks again;
// the result of the last execution is the only one adopted.
func TestHookRetryableClusterRetriesAtCurrentSlot(t *testing.T) {
	srv := newScriptedHookServer(t, func(name string, _ []string, nth int) string {
		if name == "GET" && nth == 1 {
			return "-ERR synthetic cluster failure\r\n"
		}
		if name == "GET" {
			return bulkString("from-node")
		}
		return "+OK\r\n"
	})
	defer srv.close()

	c := newScriptedCluster(t, func() []ClusterSlot { return clusterSlotsAll(srv.dialAddr()) })
	hook := &retryDecisionHook{decide: func(err error, entry int) bool {
		return err != nil && entry == 1
	}}
	c.AddHook(hook)

	got := c.Get(context.Background(), "k")
	if got.Err() != nil {
		t.Fatalf("hook retry must succeed on the second execution, got %v", got.Err())
	}
	if got.Val() != "from-node" {
		t.Fatalf("got %q", got.Val())
	}
	if srv.count("GET") != 2 {
		t.Fatalf("cluster hook retry sends exactly one more execution, got %d GET", srv.count("GET"))
	}
	if entries, _ := hook.snapshot(); entries != 2 {
		t.Fatalf("cluster hook must run again on retry, got %d entries", entries)
	}
}

// TestHookRetryableClusterRefusedAfterSuccess: a successful reply on a
// cluster node ends the command even if the hook asks for another execution.
func TestHookRetryableClusterRefusedAfterSuccess(t *testing.T) {
	srv := newScriptedHookServer(t, func(name string, _ []string, _ int) string {
		if name == "GET" {
			return bulkString("only-once")
		}
		return "+OK\r\n"
	})
	defer srv.close()

	c := newScriptedCluster(t, func() []ClusterSlot { return clusterSlotsAll(srv.dialAddr()) })
	c.AddHook(&retryDecisionHook{decide: func(error, int) bool { return true }})

	got := c.Get(context.Background(), "k")
	if got.Err() != nil {
		t.Fatalf("got %v", got.Err())
	}
	if got.Val() != "only-once" {
		t.Fatalf("got %q", got.Val())
	}
	if srv.count("GET") != 1 {
		t.Fatalf("no resend after success, got %d GET", srv.count("GET"))
	}
}

// TestHookRetryableClusterMigrationFollowsMoved: during the hook-retry pass,
// the old node answers MOVED to the slot's new owner. The driver follows the
// redirect exactly as usual and adopts only the new node's reply; the failed
// execution on the old node is not adopted a second time.
func TestHookRetryableClusterMigrationFollowsMoved(t *testing.T) {
	newNode := newScriptedHookServer(t, func(name string, _ []string, _ int) string {
		if name == "GET" {
			return bulkString("migrated")
		}
		return "+OK\r\n"
	})
	defer newNode.close()
	oldNode := newScriptedHookServer(t, func(name string, _ []string, nth int) string {
		if name != "GET" {
			return "+OK\r\n"
		}
		if nth == 1 {
			// The original pass: failed execution on the old owner.
			return "-ERR slot is busy\r\n"
		}
		// The hook-retry pass still routes here first; redirect to the new
		// owner.
		return "-MOVED 8499 " + newNode.dialAddr() + "\r\n"
	})
	defer oldNode.close()

	// Keep the old node as slot owner in the topology: the MOVED redirect,
	// not a topology reload, drives routing to the new owner.
	c := newScriptedCluster(t, func() []ClusterSlot { return clusterSlotsAll(oldNode.dialAddr()) })

	c.AddHook(&retryDecisionHook{decide: func(err error, entry int) bool {
		return err != nil && entry == 1
	}})

	got := c.Get(context.Background(), "k")
	if got.Err() != nil {
		t.Fatalf("the redirected retry must succeed, got %v", got.Err())
	}
	if got.Val() != "migrated" {
		t.Fatalf("only the new owner's result must be adopted, got %q", got.Val())
	}
	if oldNode.count("GET") != 2 {
		t.Fatalf("old node sees original failure + redirect attempt, got %d GET", oldNode.count("GET"))
	}
	if newNode.count("GET") != 1 {
		t.Fatalf("new node executes exactly once, got %d GET", newNode.count("GET"))
	}
}

// TestRetryableErrorTypeAndWrapping pins the public API and the typed
// classification: detection is by errors.As, never by message.
func TestRetryableErrorTypeAndWrapping(t *testing.T) {
	cause := errors.New("boom")
	e := NewRetryableError(cause)
	if !errors.Is(e, cause) {
		t.Fatal("Unwrap must expose the cause for errors.Is")
	}
	if !isHookRetryableError(fmt.Errorf("outer: %w", e)) {
		t.Fatal("wrapped retryable errors must be detected via errors.As")
	}
	if isHookRetryableError(errors.New("redis: hook requested a command retry: nope")) {
		t.Fatal("matching the message must not trigger a retry")
	}
	if NewRetryableError(nil).Unwrap() != nil {
		t.Fatal("nil cause should unwrap to nil")
	}
	var target *RetryableError
	if !errors.As(fmt.Errorf("x: %w", NewRetryableError(cause)), &target) {
		t.Fatal("errors.As must find *RetryableError")
	}
}
