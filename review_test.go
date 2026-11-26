package lifecycle_test

import (
	"context"
	"errors"
	"fmt"
	"math"
	"runtime"
	"testing"
	"testing/synctest"
	"time"

	"github.com/djylb/lifecycle"
)

type closeFunc func() error

func (f closeFunc) Close() error { return f() }

func TestCleanupAfterGoexit(t *testing.T) {
	p := lifecycle.New(t.Context(), lifecycle.CancelOnError())
	c, err := p.Child("child", lifecycle.CancelOnError().Propagate())
	if err != nil {
		t.Fatal(err)
	}
	ran := false
	mustAdd(t, c, closeFunc(func() error { ran = true; return nil }))
	mustAdd(t, c, closeFunc(func() error { runtime.Goexit(); return nil }))
	p.Close(nil)
	if err := wait(t, p); !errors.Is(err, lifecycle.ErrGoexit) || !ran || !c.Stats().Finished {
		t.Fatalf("cleanup: %v, ran %v", err, ran)
	}
}

func TestJoinedShutdownError(t *testing.T) {
	disk := errors.New("disk")
	for _, policy := range []lifecycle.Policy{lifecycle.CancelOnError(), lifecycle.CancelOnError().Expected(func(e error) bool { return errors.Is(e, lifecycle.ErrClosed) })} {
		s := lifecycle.New(t.Context(), policy)
		mustGo(t, s, "add", func(ctx context.Context) error {
			<-ctx.Done()
			_, err := s.Add(closeFunc(func() error { return disk }))
			return fmt.Errorf("register: %w", err)
		})
		s.Close(nil)
		if err := wait(t, s); !errors.Is(err, disk) {
			t.Fatalf("lost close error: %v", err)
		}
	}
}

func TestRunCause(t *testing.T) {
	disk := errors.Join(lifecycle.ErrClosed, errors.New("disk"))
	if got := lifecycle.Run(t.Context(), func(s *lifecycle.Scope) error { s.Close(disk); return nil }); !errors.Is(got, disk) {
		t.Fatalf("explicit cause %v", got)
	}
	ctx, cancel := context.WithCancelCause(t.Context())
	defer cancel(nil)
	if got := lifecycle.Run(ctx, func(*lifecycle.Scope) error { cancel(disk); return nil }); !errors.Is(got, disk) {
		t.Fatalf("parent cause %v", got)
	}
}

func TestInheritedDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cause := errors.New("deadline cause")
		ctx, cancel := context.WithTimeoutCause(t.Context(), time.Second, cause)
		defer cancel()
		root := lifecycle.New(ctx, lifecycle.CancelOnError())
		child, err := root.Child("child", lifecycle.CancelOnError())
		if err != nil {
			t.Fatal(err)
		}
		<-root.Finished()
		for _, s := range []*lifecycle.Scope{root, child} {
			if !errors.Is(s.Context().Err(), context.DeadlineExceeded) || !errors.Is(context.Cause(s.Context()), cause) {
				t.Fatal(s.Context().Err(), s.Cause())
			}
		}
	})
}

func TestSameKeyWaiters(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q := newKeyed(t, lifecycle.KeyedLatestConfig[int, int]{Capacity: 1, Full: lifecycle.Block})
		defer mustCloseC(t, q)
		if err := q.Set(t.Context(), 1, 1); err != nil {
			t.Fatal(err)
		}
		results := make(chan error, 2)
		for i := range 2 {
			go func() { results <- q.Set(t.Context(), 2, i) }()
		}
		synctest.Wait()
		q.TryPop()
		synctest.Wait()
		if len(results) != 2 {
			t.Fatalf("completed %d sets", len(results))
		}
		for range 2 {
			if err := <-results; err != nil {
				t.Fatal(err)
			}
		}
	})
}

func TestMailboxKeyContract(t *testing.T) {
	ctx := t.Context()
	q := newKeyed(t, lifecycle.KeyedLatestConfig[any, int]{Capacity: 1, Full: lifecycle.Reject})
	invalid := []any{math.NaN(), [1]float64{math.NaN()}, []int{1}, struct{ K any }{[]int{1}}, [0]struct{ Bad []int }{}}
	for _, key := range invalid {
		if err := q.Set(ctx, key, 0); !errors.Is(err, lifecycle.ErrInvalidKey) {
			t.Fatal(err)
		}
	}
	if err := q.Set(t.Context(), "ok", 1); err != nil {
		t.Fatal(err)
	}
	mustCloseC(t, q)
	for _, key := range invalid {
		if err := q.Set(ctx, key, 0); !errors.Is(err, lifecycle.ErrInvalidKey) {
			t.Fatalf("closed Set(%T): %v", key, err)
		}
	}
	type composite struct {
		bytes [256]byte
		tag   any
	}
	big := newKeyed(t, lifecycle.KeyedLatestConfig[composite, int]{Capacity: 256, Full: lifecycle.Reject})
	k := composite{tag: "large"}
	if alloc := testing.AllocsPerRun(20, func() {
		for i := range 256 {
			k.bytes[0] = byte(i)
			if err := big.Set(t.Context(), k, i); err != nil {
				t.Fatal(err)
			}
		}
		for range 256 {
			big.TryPop()
		}
		k.bytes[1]++
	}); alloc != 0 {
		t.Fatalf("large keys: %v allocations", alloc)
	}
}

type blockingErrContext struct {
	unregisterContext
	release <-chan struct{}
}

func (c *blockingErrContext) Err() error {
	select {
	case <-c.done:
		<-c.release
		return context.Canceled
	default:
		return nil
	}
}

func TestIdleParentCauseOffCloseStack(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		ctx := &blockingErrContext{unregisterContext{done: make(chan struct{})}, release}
		root := lifecycle.New(ctx, lifecycle.CancelOnError())
		child, err := root.Child("last", lifecycle.CancelOnError())
		if err != nil {
			t.Fatal(err)
		}
		root.CloseWhenIdle()
		close(ctx.done)
		synctest.Wait() // the watcher is inside the parent's Err
		returned := make(chan struct{})
		go func() { child.Close(nil); close(returned) }()
		synctest.Wait()
		select {
		case <-returned:
		default:
			t.Error("child Close waited for the parent's Err")
		}
		close(release)
		if err := wait(t, root); err != nil || !errors.Is(root.Cause(), context.Canceled) {
			t.Fatal(err, root.Cause())
		}
	})
}

type taskExitError struct{ reason any }

func (taskExitError) Error() string { return "task exit" }
func (taskExitError) Is(target error) bool {
	_, ok := target.(taskExitError)
	return ok
}

func TestDynamicCauseComparison(t *testing.T) {
	cause := taskExitError{reason: []int{1}}
	s := lifecycle.New(t.Context(), lifecycle.CancelOnError())
	mustGo(t, s, "worker", func(ctx context.Context) error { <-ctx.Done(); return fmt.Errorf("wrapped: %w", cause) })
	s.Close(cause)
	if err := wait(t, s); err != nil {
		t.Fatal(err)
	}
}

func TestDeadlinePredicateReplacement(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		for _, fn := range []func(error) bool{nil, lifecycle.IsExpectedExit} {
			ctx, cancel := context.WithTimeoutCause(t.Context(), time.Second, errors.New("deadline cause"))
			s := lifecycle.New(ctx, lifecycle.CancelOnError().Expected(fn))
			mustGo(t, s, "worker", func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() })
			err := wait(t, s)
			cancel()
			if (fn == nil && err != nil) || (fn != nil && !errors.Is(err, context.DeadlineExceeded)) {
				t.Fatalf("custom predicate %t: %v", fn != nil, err)
			}
		}
	})
}

func TestPolicyFailure(t *testing.T) {
	const predicate = "predicate"
	taskErr := errors.New("task")
	policyErr := errors.New("policy")
	for _, tc := range []struct {
		fail func()
		want error
	}{{func() { panic(policyErr) }, policyErr}, {runtime.Goexit, lifecycle.ErrGoexit}} {
		for _, mode := range []string{"handler", "propagate", predicate} {
			policy := lifecycle.Isolate(func(error) { tc.fail() })
			if mode == predicate {
				policy = lifecycle.CancelOnError().Expected(func(error) bool { tc.fail(); return false })
			}
			s := lifecycle.New(t.Context(), policy)
			target := s
			if mode == "propagate" {
				var err error
				target, err = s.Child("child", lifecycle.CancelOnError().Propagate())
				if err != nil {
					t.Fatal(err)
				}
			}
			mustGo(t, target, "task", func(ctx context.Context) error {
				if mode == predicate {
					<-ctx.Done()
				}
				return taskErr
			})
			if mode == predicate {
				s.Close(nil)
			}
			if err := wait(t, s); !errors.Is(err, taskErr) || !errors.Is(err, tc.want) || s.Stats().Scopes != 0 {
				t.Fatalf("%s: %v", mode, err)
			}
		}
	}
}

func TestDeepShutdown(t *testing.T) {
	for _, drain := range []bool{false, true} {
		root := lifecycle.New(t.Context(), lifecycle.CancelOnError())
		s := root
		for range 128 {
			var err error
			s, err = s.Child("child", lifecycle.CancelOnError())
			if err != nil {
				t.Fatal(err)
			}
		}
		if root.Stats().Scopes != 128 {
			t.Fatal("incomplete tree")
		}
		if drain {
			root.Drain()
		} else {
			root.Close(nil)
		}
		if err := wait(t, root); err != nil || root.Stats().Scopes != 0 {
			t.Fatalf("shutdown %v", err)
		}
	}
}

func TestConnectionScopeLifetime(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		root := lifecycle.New(t.Context(), lifecycle.CancelOnError())
		conn, err := root.Child("connection", lifecycle.Isolate(func(err error) { t.Error(err) }))
		if err != nil {
			t.Fatal(err)
		}
		socket := &closer{}
		mustAdd(t, conn, socket)
		mustGo(t, conn, "serve", func(context.Context) error { return nil })
		conn.CloseWhenIdle()
		synctest.Wait()
		if st := root.Stats(); st.Scopes != 0 || st.Closed || socket.n.Load() != 1 {
			t.Fatalf("after request: %+v, closes %d", st, socket.n.Load())
		}
		root.Close(nil)
	})
}

func TestCompletedWaitWithTimeoutError(t *testing.T) {
	s := lifecycle.New(t.Context(), lifecycle.CancelOnError())
	mustAdd(t, s, &closer{err: lifecycle.ErrNotFinished})
	s.Close(nil)
	if finished, err := s.WaitResult(t.Context()); !finished || !errors.Is(err, lifecycle.ErrNotFinished) {
		t.Fatalf("wait (%v,%v)", finished, err)
	}
}

func TestDropCallbackPanicAfterCommit(t *testing.T) {
	q, err := lifecycle.NewQueue(lifecycle.QueueConfig[int]{Capacity: 2, Full: lifecycle.DropOldest, OnDrop: func(v int) { panic(v) }})
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range []int{1, 2} {
		if err := q.Push(t.Context(), v); err != nil {
			t.Fatal(err)
		}
	}
	func() {
		defer func() {
			if r := recover(); r != 1 {
				t.Fatalf("evicted %v", r)
			}
		}()
		_ = q.Push(t.Context(), 3)
	}()
	if st := q.Stats(); st.Pushed != 3 || st.Dropped != 1 {
		t.Fatal(st)
	}
	for _, want := range []int{2, 3} {
		if v, ok := q.TryPop(); !ok || v != want {
			t.Fatalf("popped %v, want %v", v, want)
		}
	}
}

type unregisterContext struct {
	done  chan struct{}
	hooks int
}

func (*unregisterContext) Err() error                  { return nil }
func (*unregisterContext) Value(any) any               { return nil }
func (*unregisterContext) Deadline() (time.Time, bool) { return time.Time{}, false }

func (c *unregisterContext) Done() <-chan struct{} { return c.done }
func (c *unregisterContext) AfterFunc(func()) func() bool {
	c.hooks++
	return func() bool { panic("synchronous unregister") }
}

func TestCloseWithCustomParent(t *testing.T) {
	ctx := &unregisterContext{done: make(chan struct{})}
	s := lifecycle.New(ctx, lifecycle.CancelOnError())
	s.Close(nil)
	select {
	case <-s.Done():
	default:
		t.Fatal("context still live")
	}
	if err := wait(t, s); err != nil || ctx.hooks != 0 {
		t.Fatalf("cleanup %v, external registrations %d", err, ctx.hooks)
	}
}

type emptyBranchesError struct{}

func (emptyBranchesError) Error() string   { return "empty branches" }
func (emptyBranchesError) Unwrap() []error { return nil }

func TestReportedBoundaries(t *testing.T) {
	cause := errors.Join(errors.New("a"), errors.New("b"))
	t.Run("finished", func(t *testing.T) {
		got := lifecycle.Run(t.Context(), func(s *lifecycle.Scope) error { s.Close(nil); <-s.Finished(); return cause })
		if !errors.Is(got, cause) {
			t.Fatal(got)
		}
	})
	for _, tc := range []struct {
		name string
		task error
		want error
	}{
		{"joined", fmt.Errorf("stop: %w", cause), nil}, {"empty", emptyBranchesError{}, emptyBranchesError{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := lifecycle.New(t.Context(), lifecycle.CancelOnError())
			mustGo(t, s, "worker", func(ctx context.Context) error { <-ctx.Done(); return tc.task })
			s.Close(cause)
			if got := wait(t, s); !errors.Is(got, tc.want) {
				t.Fatal(got)
			}
		})
	}
	t.Run("deadline", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctx, cancel := context.WithTimeoutCause(t.Context(), time.Second, cause)
			defer cancel()
			got := lifecycle.Run(ctx, func(s *lifecycle.Scope) error {
				child, stop := context.WithCancel(s.Context())
				defer stop()
				mustGo(t, s, "worker", func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() })
				<-child.Done()
				if !errors.Is(child.Err(), context.DeadlineExceeded) || !errors.Is(context.Cause(child), cause) {
					t.Error(child.Err(), context.Cause(child))
				}
				return nil
			})
			if !errors.Is(got, cause) {
				t.Fatal(got)
			}
		})
	})
	t.Run("alloc", func(t *testing.T) {
		q := newKeyed(t, lifecycle.KeyedLatestConfig[any, int]{Capacity: 1, Full: lifecycle.Reject})
		var key any = []int{1}
		ctx := t.Context()
		if n := testing.AllocsPerRun(20, func() {
			if !errors.Is(q.Set(ctx, key, 0), lifecycle.ErrInvalidKey) {
				t.Fatal("accepted key")
			}
		}); n != 0 {
			t.Fatal(n)
		}
	})
}

func TestWaitResultInterrupted(t *testing.T) {
	s := lifecycle.New(t.Context(), lifecycle.CancelOnError())
	defer s.Close(nil)
	ctx, cancel := context.WithCancelCause(t.Context())
	cause := errors.New("stop waiting")
	cancel(cause)
	if done, err := s.WaitResult(ctx); done || !errors.Is(err, cause) || !errors.Is(err, lifecycle.ErrNotFinished) {
		t.Fatal(done, err)
	}
}

func TestUnconstructedScope(t *testing.T) {
	var s lifecycle.Scope
	for name, call := range map[string]func(){
		"Name":          func() { s.Name() },
		"Context":       func() { s.Context() },
		"Done":          func() { s.Done() },
		"Draining":      func() { s.Draining() },
		"Cause":         func() { _ = s.Cause() },
		"Err":           func() { _ = s.Err() },
		"Wait":          func() { _ = s.Wait(t.Context()) },
		"WaitResult":    func() { _, _ = s.WaitResult(t.Context()) },
		"Finished":      func() { s.Finished() },
		"Go":            func() { _ = s.Go("", nil) },
		"Enter":         func() { _, _ = s.Enter() },
		"Add":           func() { _, _ = s.Add(nil) },
		"Child":         func() { _, _ = s.Child("", lifecycle.CancelOnError()) },
		"Close":         func() { s.Close(nil) },
		"Drain":         s.Drain,
		"CloseWhenIdle": s.CloseWhenIdle,
		"Stats":         func() { s.Stats() },
	} {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if recover() != "lifecycle: unconstructed Scope" {
					t.Fatal("missing construction check")
				}
			}()
			call()
		})
	}
}
