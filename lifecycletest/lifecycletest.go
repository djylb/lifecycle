// Package lifecycletest helps test code built on lifecycle: it checks that a
// scope tree is empty, finds leaked goroutines from runtime stack dumps, and
// runs the standard start, close and verify cycle. Where the runtime has the
// goroutineleak profile, leak reports add it: it marks the goroutines blocked
// forever on something no running goroutine can reach.
package lifecycletest

import (
	"context"
	"testing"
	"time"

	"github.com/djylb/lifecycle"
)

// AssertEmpty fails the test if s or a descendant still has running tasks,
// registered resources or unfinished child scopes.
func AssertEmpty(tb testing.TB, s *lifecycle.Scope) {
	tb.Helper()
	if st := s.Stats(); st.Tasks != 0 || st.Resources != 0 || st.Scopes != 0 {
		tb.Errorf("scope not empty: %d task(s), %d resource(s), %d child scope(s)", st.Tasks, st.Resources, st.Scopes)
	}
}

// RunCycle runs the standard lifecycle test: it creates a root scope with
// the CancelOnError policy, calls start to fill it, closes it, waits up to
// timeout for it to finish, asserts that it is empty, and checks that no
// goroutine started during the cycle is left. It fails the test if start
// fails, the scope does not finish in time, or something is left, and
// returns the scope's result for the caller to check. On a wait timeout it
// reports the failure and returns the wait error. Cleanup also runs if start
// panics or calls runtime.Goexit. Like CheckGoroutines, it must not run in
// parallel with other tests.
func RunCycle(tb testing.TB, timeout time.Duration, start func(s *lifecycle.Scope) error) (result error) {
	tb.Helper()
	baseline := Goroutines()
	s := lifecycle.New(context.Background(), lifecycle.CancelOnError())
	defer func() {
		s.Close(nil)
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		finished, err := s.WaitResult(ctx)
		if !finished {
			st := s.Stats()
			result = err
			tb.Errorf("scope did not finish within %v: %d task(s), %d child scope(s) left", timeout, st.Tasks, st.Scopes)
			return
		}
		AssertEmpty(tb, s)
		if leaked := Leaks(baseline, timeout); len(leaked) > 0 {
			tb.Error(format(leaked))
		}
		result = err
	}()
	if err := start(s); err != nil {
		tb.Errorf("start: %v", err)
	}
	return nil
}
