package redis

import (
	"context"
	"errors"
	"sync/atomic"
)

// RetryableError is the driver-defined error a ProcessHook can return to ask
// go-redis to execute a single command one more time.
//
// The decision to retry is made on the error TYPE (errors.As), never on its
// message: wrap the cause with NewRetryableError and return it from a
// ProcessHook, and the driver re-runs the command through the full ProcessHook
// chain inside its own command-send loop.
//
// Safety rules enforced by the driver:
//
//   - The retry is granted at most ONCE per command. It performs exactly one
//     additional command execution; it does not add a second retry budget, so
//     a failure already retried through MaxRetries can never be multiplied by
//     a second, independent retry loop.
//   - The command is re-sent only while it has not yet received a successful
//     reply. Once a success reply has been read, a RetryableError returned by
//     a hook is ignored and the successful result is kept — the command is
//     never sent again.
//   - The retry goes through the hooks again (the whole ProcessHook chain is
//     re-entered), and it may land on another connection. On a ClusterClient
//     the node is re-selected from the command's current slot; MOVED and ASK
//     keep their existing redirect handling, and only the reply of the last
//     actual execution is adopted.
//   - A canceled context stops the retry; the context error is returned.
//
// RetryableError only applies to a single command processed through
// ProcessHook. Returning it from a ProcessPipelineHook (a pipeline or a
// MULTI/EXEC transaction) fails the whole batch: a command that is part of a
// batch already being sent, or of a transaction, is never extracted and
// retried on its own. PubSub, subscriptions and MONITOR are not retried.
//
// A hook that asks for a retry after the command produced an error reply is
// asking for the command to be executed a second time, so only return this
// error when that is safe for the command.
type RetryableError struct {
	err error
}

// NewRetryableError wraps err so the driver executes the command once more.
// err may be nil when the hook asks for the first execution without having
// called next.
func NewRetryableError(err error) *RetryableError {
	return &RetryableError{err: err}
}

// Error implements the error interface.
func (e *RetryableError) Error() string {
	if e.err != nil {
		return "redis: hook requested a command retry: " + e.err.Error()
	}
	return "redis: hook requested a command retry"
}

// Unwrap returns the wrapped cause so errors.Is/errors.As keep working
// through the hook boundary.
func (e *RetryableError) Unwrap() error {
	return e.err
}

// isHookRetryableError reports whether err is (or wraps) a RetryableError.
// Typed classification only — the error message is never inspected.
func isHookRetryableError(err error) bool {
	var target *RetryableError
	return errors.As(err, &target)
}

// hookRetryStateKey carries the per-Process retry state through context. The
// state is created by the outermost Process entry that owns the hook-retry
// decision; the send layer below the hook chain marks successful replies on
// it. Presence of a state also tells nested clients (a cluster node client
// reached from the cluster loop) that an outer loop already owns retry.
type hookRetryStateKey struct{}

// hookRetryState tracks whether the current hook pass has already delivered a
// successful reply. It is touched by the command goroutine and, for cluster
// fan-out commands, by the per-node goroutines, so the flag is atomic.
type hookRetryState struct {
	replySucceeded atomic.Bool
}

func withHookRetryContext(ctx context.Context) (context.Context, *hookRetryState) {
	state := &hookRetryState{}
	return context.WithValue(ctx, hookRetryStateKey{}, state), state
}

func hookRetryStateFromContext(ctx context.Context) *hookRetryState {
	state, _ := ctx.Value(hookRetryStateKey{}).(*hookRetryState)
	return state
}

// beginPass resets the success marker for one invocation of the hook chain.
func (s *hookRetryState) beginPass() {
	s.replySucceeded.Store(false)
}

// markReplySucceeded records that the command received a successful reply
// during the current hook pass. After this, the command must not be sent
// again.
func (s *hookRetryState) markReplySucceeded() {
	s.replySucceeded.Store(true)
}

// markHookReplySucceeded marks the command successful at the send layer. It
// is a no-op when no hook-retry owner is present (FD divert, internal
// pipelines, init traffic), so those paths are unaffected.
func markHookReplySucceeded(ctx context.Context) {
	if state := hookRetryStateFromContext(ctx); state != nil {
		state.markReplySucceeded()
	}
}

// runHookRetryLoop invokes the hook chain around first and, when a hook
// returns a RetryableError while the command has not received a successful
// reply yet, re-enters the hook chain exactly once around once — the
// driver-level single re-execution of the command.
//
// first is the normal terminal of the hook chain (the client's usual send
// path, including its existing network-retry loop). once is the terminal of
// the extra pass: it performs exactly one command execution and must not run
// another network-retry loop, so the hook-retry can never stack a second
// retry budget on top of MaxRetries.
//
// The ctx must already carry a hookRetryState.
func (hs *hooksMixin) runHookRetryLoop(
	ctx context.Context, cmd Cmder, first ProcessHook, once ProcessHook,
) error {
	state := hookRetryStateFromContext(ctx)
	terminal := first
	for pass := 0; ; pass++ {
		state.beginPass()
		err := hs.withProcessHook(ctx, cmd, terminal)

		if !isHookRetryableError(err) {
			return err
		}
		// A successful reply was already read during this pass. The command
		// must never be sent again; its actual result wins. Return the cause
		// the hook wrapped (nil for an ordinary success, redis.Nil for a
		// complete negative reply) instead of the retry request itself.
		if state.replySucceeded.Load() {
			var re *RetryableError
			if errors.As(err, &re) {
				return re.Unwrap()
			}
			return err
		}
		// A canceled or expired context stops the retry.
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		// At most one extra execution, and never a second execution of a
		// command that forbids replay (a possible partial write).
		if pass > 0 || cmd.NoRetry() {
			return err
		}
		// Present a fresh, not-yet-executed state to the retry pass' hooks;
		// Process will stamp the final error when this returns.
		cmd.SetErr(nil)
		terminal = once
	}
}
