package lifecycle_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/djylb/lifecycle"
	"github.com/djylb/lifecycle/lifecycletest"
)

func TestMain(m *testing.M) { lifecycletest.VerifyMain(m) }

const waitLimit = 5 * time.Second

func wait(t *testing.T, s *lifecycle.Scope) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), waitLimit)
	defer cancel()
	err := s.Wait(ctx)
	if errors.Is(err, lifecycle.ErrNotFinished) {
		t.Fatalf("scope did not finish: %+v", s.Stats())
	}
	return err
}

func mustGo(t testing.TB, s *lifecycle.Scope, name string, fn func(context.Context) error) {
	t.Helper()
	if err := s.Go(name, fn); err != nil {
		t.Fatalf("Go %q: %v", name, err)
	}
}

func mustAdd(t testing.TB, s *lifecycle.Scope, c io.Closer) {
	t.Helper()
	if _, err := s.Add(c); err != nil {
		t.Fatalf("Add: %v", err)
	}
}

func waitNil(t *testing.T, s *lifecycle.Scope) {
	t.Helper()
	if err := wait(t, s); err != nil {
		t.Fatalf("result %v", err)
	}
}

type closer struct {
	name  string
	log   *log
	err   error
	panic any
	block chan struct{}
	n     atomic.Int32
}

func (c *closer) Close() error {
	c.n.Add(1)
	if c.block != nil {
		<-c.block
	}
	if c.log != nil {
		c.log.add(c.name)
	}
	if c.panic != nil {
		panic(c.panic)
	}
	return c.err
}

type log struct {
	mu  sync.Mutex
	got []string
}

func (l *log) add(s string) {
	l.mu.Lock()
	l.got = append(l.got, s)
	l.mu.Unlock()
}

func (l *log) list() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.got)
}

func TestCancelOnError(t *testing.T) {
	s := lifecycle.New(context.Background(), lifecycle.CancelOnError())
	boom := errors.New("boom")
	if err := s.Go("waiter", func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }); err != nil {
		t.Fatal(err)
	}
	if err := s.Go("failer", func(context.Context) error { return boom }); err != nil {
		t.Fatal(err)
	}
	err := wait(t, s)
	var te *lifecycle.TaskError
	if !errors.As(err, &te) || te.Name != "failer" || !errors.Is(err, boom) {
		t.Fatalf("result %v", err)
	}
	if strings.Contains(err.Error(), "waiter") {
		t.Fatalf("normal exit reported: %v", err)
	}
	if !errors.Is(s.Cause(), boom) || !errors.Is(context.Cause(s.Context()), boom) {
		t.Fatalf("cause %v", s.Cause())
	}
}

func TestIsolate(t *testing.T) {
	var mu sync.Mutex
	var got []error
	s := lifecycle.New(context.Background(), lifecycle.Isolate(func(err error) {
		mu.Lock()
		got = append(got, err)
		mu.Unlock()
	}))
	done := make(chan struct{})
	for i := range 3 {
		mustGo(t, s, "t", func(context.Context) error {
			defer func() { done <- struct{}{} }()
			if i == 1 {
				return nil
			}
			return errors.New("fail")
		})
	}
	for range 3 {
		<-done
	}
	if s.Cause() != nil {
		t.Fatal("isolated scope closed by task error")
	}
	mustGo(t, s, "exit", func(ctx context.Context) error { <-ctx.Done(); return context.Cause(ctx) })
	s.Close(nil)
	if err := wait(t, s); err != nil {
		t.Fatalf("result %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(got) != 2 {
		t.Fatalf("handler got %v", got)
	}
}

func TestPanic(t *testing.T) {
	s := lifecycle.New(context.Background(), lifecycle.CancelOnError())
	mustGo(t, s, "bad", func(context.Context) error { panic("oops") })
	err := wait(t, s)
	var te *lifecycle.TaskError
	var pe *lifecycle.PanicError
	if !errors.As(err, &te) || te.Name != "bad" || !errors.As(err, &pe) || pe.Value != "oops" {
		t.Fatalf("result %v", err)
	}
	if !strings.Contains(string(pe.Stack), "TestPanic") {
		t.Fatalf("stack misses the panicking function:\n%s", pe.Stack)
	}
}

func TestPanicAfterCloseReported(t *testing.T) {
	s := lifecycle.New(context.Background(), lifecycle.CancelOnError())
	mustGo(t, s, "bad", func(ctx context.Context) error { <-ctx.Done(); panic(context.Canceled) })
	s.Close(nil)
	var pe *lifecycle.PanicError
	if err := wait(t, s); !errors.As(err, &pe) {
		t.Fatalf("panic after close not reported: %v", err)
	}
}

func TestGoexit(t *testing.T) {
	s := lifecycle.New(context.Background(), lifecycle.CancelOnError())
	mustGo(t, s, "exit", func(context.Context) error { runtime.Goexit(); return nil })
	if err := wait(t, s); !errors.Is(err, lifecycle.ErrGoexit) {
		t.Fatalf("result %v", err)
	}
}

func TestResourcesReverseOrderAndErrors(t *testing.T) {
	l := &log{}
	errB := errors.New("b failed")
	s := lifecycle.New(context.Background(), lifecycle.CancelOnError())
	a := &closer{name: "a", log: l}
	b := &closer{name: "b", log: l, err: errB}
	c := &closer{name: "c", log: l, panic: "c panicked"}
	d := &closer{name: "d", log: l}
	for _, r := range []*closer{a, b, c, d} {
		if _, err := s.Add(r); err != nil {
			t.Fatal(err)
		}
	}
	s.Close(nil)
	err := wait(t, s)
	if got := l.list(); !slices.Equal(got, []string{"d", "c", "b", "a"}) {
		t.Fatalf("close order %v", got)
	}
	var pe *lifecycle.PanicError
	if !errors.Is(err, errB) || !errors.As(err, &pe) || pe.Value != "c panicked" {
		t.Fatalf("result %v", err)
	}
	if errors.Is(err, lifecycle.ErrClosed) {
		t.Fatal("close cause is part of the result")
	}
}

func TestRemove(t *testing.T) {
	s := lifecycle.New(context.Background(), lifecycle.CancelOnError())
	a, b, c := &closer{}, &closer{}, &closer{}
	_, _ = s.Add(a)
	rmB, _ := s.Add(b)
	rmC, _ := s.Add(c)
	if !rmB() || rmB() {
		t.Fatal("remove must succeed once")
	}
	if st := s.Stats(); st.Resources != 2 {
		t.Fatalf("stats %+v", st)
	}
	s.Close(nil)
	waitNil(t, s)
	if rmC() {
		t.Fatal("remove after close succeeded")
	}
	if a.n.Load() != 1 || b.n.Load() != 0 || c.n.Load() != 1 {
		t.Fatalf("closes a=%d b=%d c=%d", a.n.Load(), b.n.Load(), c.n.Load())
	}
}

func TestClosedScope(t *testing.T) {
	s := lifecycle.New(context.Background(), lifecycle.CancelOnError())
	if !s.Close(nil) || s.Close(errors.New("again")) {
		t.Fatal("Close must start closing exactly once")
	}
	if !errors.Is(s.Cause(), lifecycle.ErrClosed) {
		t.Fatalf("cause %v", s.Cause())
	}
	ran := false
	if err := s.Go("x", func(context.Context) error { ran = true; return nil }); !errors.Is(err, lifecycle.ErrClosed) || ran {
		t.Fatalf("Go on closed scope: %v, ran %v", err, ran)
	}
	errC := errors.New("close failed")
	c := &closer{err: errC}
	rm, err := s.Add(c)
	if !errors.Is(err, lifecycle.ErrClosed) || !errors.Is(err, errC) || c.n.Load() != 1 || rm() {
		t.Fatalf("Add on closed scope: %v, closes %d", err, c.n.Load())
	}
	p := &closer{panic: "p"}
	var pe *lifecycle.PanicError
	if _, err := s.Add(p); !errors.As(err, &pe) {
		t.Fatalf("Add panicking closer on closed scope: %v", err)
	}
	if k, err := s.Child("k", lifecycle.CancelOnError()); k != nil || !errors.Is(err, lifecycle.ErrClosed) {
		t.Fatalf("Child on closed scope: %v", err)
	}
	if _, err := s.Add(nil); err == nil {
		t.Fatal("Add(nil) accepted")
	}
	if err := wait(t, s); err != nil {
		t.Fatal(err)
	}
}

func TestDoneMeansClosed(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	s := lifecycle.New(ctx, lifecycle.CancelOnError())
	k, _ := s.Child("k", lifecycle.CancelOnError())
	cancel()
	<-k.Done()
	if err := k.Go("x", func(context.Context) error { return nil }); !errors.Is(err, lifecycle.ErrClosed) {
		t.Fatalf("Go after Done: %v", err)
	}
	if !errors.Is(s.Cause(), context.Canceled) || !errors.Is(k.Cause(), context.Canceled) {
		t.Fatalf("causes %v %v", s.Cause(), k.Cause())
	}
	if err := wait(t, s); err != nil {
		t.Fatal(err)
	}
}

func TestParentContext(t *testing.T) {
	type key struct{}
	deadline := time.Now().Add(time.Hour)
	ctx, cancel := context.WithDeadline(context.WithValue(context.Background(), key{}, "v"), deadline)
	defer cancel()
	s := lifecycle.New(ctx, lifecycle.CancelOnError())
	if d, ok := s.Context().Deadline(); !ok || !d.Equal(deadline) {
		t.Fatalf("deadline %v %v", d, ok)
	}
	if s.Context().Value(key{}) != "v" {
		t.Fatal("value lost")
	}
	s.Close(nil)
	waitNil(t, s)

	// A cancelled parent closes the scope at once, without a goroutine.
	s = lifecycle.New(ctx, lifecycle.CancelOnError())
	cancel()
	<-s.Done()
	s = lifecycle.New(ctx, lifecycle.CancelOnError())
	if s.Cause() == nil {
		t.Fatal("scope of a done context is open")
	}
	waitNil(t, s)
}

func TestCloseDoesNotBlock(t *testing.T) {
	release := make(chan struct{})
	c := &closer{block: release}
	s := lifecycle.New(context.Background(), lifecycle.CancelOnError())
	mustAdd(t, s, c)
	mustGo(t, s, "task", func(ctx context.Context) error { <-ctx.Done(); return nil })
	s.Close(nil)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := s.Wait(ctx); !errors.Is(err, lifecycle.ErrNotFinished) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Wait with blocked closer: %v", err)
	}
	close(release)
	if err := wait(t, s); err != nil {
		t.Fatal(err)
	}
	<-s.Finished()
}

func TestWaitDoesNotClose(t *testing.T) {
	s := lifecycle.New(context.Background(), lifecycle.CancelOnError())
	mustGo(t, s, "quick", func(context.Context) error { return nil })
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := s.Wait(ctx); !errors.Is(err, lifecycle.ErrNotFinished) {
		t.Fatalf("Wait on open scope: %v", err)
	}
	if s.Err() != nil || s.Stats().Finished {
		t.Fatal("open scope finished")
	}
	s.Close(nil)
	waitNil(t, s)
}

func TestChildrenCloseFirst(t *testing.T) {
	l := &log{}
	s := lifecycle.New(context.Background(), lifecycle.CancelOnError())
	mustAdd(t, s, &closer{name: "parent-1", log: l})
	k, _ := s.Child("k", lifecycle.CancelOnError())
	g, _ := k.Child("g", lifecycle.CancelOnError())
	mustAdd(t, g, &closer{name: "grandchild", log: l})
	mustAdd(t, k, &closer{name: "child", log: l})
	mustAdd(t, s, &closer{name: "parent-2", log: l})
	if st := s.Stats(); st.Scopes != 2 || st.Resources != 4 {
		t.Fatalf("stats %+v", st)
	}
	s.Close(nil)
	if k.Cause() == nil || g.Cause() == nil {
		t.Fatal("descendants open after Close returned")
	}
	if err := wait(t, s); err != nil {
		t.Fatal(err)
	}
	if got := l.list(); !slices.Equal(got, []string{"grandchild", "child", "parent-2", "parent-1"}) {
		t.Fatalf("close order %v", got)
	}
	if !k.Stats().Finished || !g.Stats().Finished {
		t.Fatal("parent finished before its children")
	}
	lifecycletest.AssertEmpty(t, s)
}

func TestParentWaitsForChildTasks(t *testing.T) {
	s := lifecycle.New(context.Background(), lifecycle.CancelOnError())
	k, _ := s.Child("k", lifecycle.CancelOnError())
	var exited atomic.Bool
	mustGo(t, k, "slow", func(ctx context.Context) error {
		<-ctx.Done()
		time.Sleep(10 * time.Millisecond)
		exited.Store(true)
		return nil
	})
	s.Close(nil)
	waitNil(t, s)
	if !exited.Load() {
		t.Fatal("parent finished before a child task")
	}
}

func TestChildErrorReachesParent(t *testing.T) {
	boom := errors.New("boom")

	// By default a failed child stays alone.
	s := lifecycle.New(context.Background(), lifecycle.CancelOnError())
	k, _ := s.Child("worker", lifecycle.CancelOnError())
	mustGo(t, k, "job", func(context.Context) error { return boom })
	if err := wait(t, k); !errors.Is(err, boom) {
		t.Fatalf("child result %v", err)
	}
	if s.Cause() != nil {
		t.Fatal("parent closed by a child without Propagate")
	}
	s.Close(nil)
	if err := wait(t, s); err != nil {
		t.Fatalf("parent result %v", err)
	}

	// Propagating child under a CancelOnError parent: it closes the parent.
	s = lifecycle.New(context.Background(), lifecycle.CancelOnError())
	k, _ = s.Child("worker", lifecycle.CancelOnError().Propagate())
	mustGo(t, k, "job", func(context.Context) error { return boom })
	err := wait(t, s)
	var te *lifecycle.TaskError
	if !errors.As(err, &te) || te.Name != "worker" || !errors.Is(err, boom) {
		t.Fatalf("parent result %v", err)
	}

	// Isolate parent: the child's result goes to the handler.
	got := make(chan error, 1)
	s = lifecycle.New(context.Background(), lifecycle.Isolate(func(err error) { got <- err }))
	k, _ = s.Child("conn", lifecycle.CancelOnError().Propagate())
	mustAdd(t, k, &closer{err: boom})
	k.Close(nil)
	if err := <-got; !errors.Is(err, boom) || !strings.Contains(err.Error(), `"conn"`) {
		t.Fatalf("handler got %v", err)
	}
	if s.Cause() != nil {
		t.Fatal("isolated parent closed")
	}
	if st := s.Stats(); st.Scopes != 0 {
		t.Fatalf("finished child still linked: %+v", st)
	}
	s.Close(nil)
	if err := wait(t, s); err != nil {
		t.Fatal(err)
	}
}

func TestHandlerBeforeWait(t *testing.T) {
	var calls atomic.Int32
	s := lifecycle.New(context.Background(), lifecycle.Isolate(func(error) {
		time.Sleep(5 * time.Millisecond)
		calls.Add(1)
	}))
	for range 4 {
		mustGo(t, s, "t", func(ctx context.Context) error { <-ctx.Done(); return errors.New("late") })
	}
	s.Close(nil)
	waitNil(t, s)
	if calls.Load() != 4 {
		t.Fatalf("handler calls after Wait: %d of 4 done", calls.Load())
	}
}

func TestExpected(t *testing.T) {
	// The default accepts net.ErrClosed, also wrapped.
	s := lifecycle.New(context.Background(), lifecycle.CancelOnError())
	mustGo(t, s, "conn", func(ctx context.Context) error {
		<-ctx.Done()
		return fmt.Errorf("read: %w", net.ErrClosed)
	})
	s.Close(nil)
	if err := wait(t, s); err != nil {
		t.Fatalf("default expected: %v", err)
	}

	// A custom function replaces the default; the cause stays expected.
	errStop := errors.New("stopped")
	cause := errors.New("shutdown")
	s = lifecycle.New(context.Background(), lifecycle.CancelOnError().Expected(func(err error) bool {
		return errors.Is(err, errStop)
	}))
	for _, e := range []error{errStop, cause, context.Canceled} {
		mustGo(t, s, "t", func(ctx context.Context) error { <-ctx.Done(); return e })
	}
	s.Close(cause)
	if err := wait(t, s); !errors.Is(err, context.Canceled) || errors.Is(err, errStop) || errors.Is(err, cause) {
		t.Fatalf("custom expected: %v", err)
	}

	// Before the scope closes nothing is expected.
	s = lifecycle.New(context.Background(), lifecycle.CancelOnError())
	mustGo(t, s, "t", func(context.Context) error { return context.Canceled })
	if err := wait(t, s); !errors.Is(err, context.Canceled) {
		t.Fatalf("error before close: %v", err)
	}
}

// TestExpectedMayUseScope: the Expected callback runs without the scope
// lock, so it may call back into the scope.
func TestExpectedMayUseScope(t *testing.T) {
	var s *lifecycle.Scope
	s = lifecycle.New(context.Background(), lifecycle.CancelOnError().Expected(func(err error) bool {
		_ = s.Stats()
		return s.Cause() != nil && errors.Is(err, context.Canceled)
	}))
	mustGo(t, s, "t", func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() })
	s.Close(nil)
	waitNil(t, s)
}

func TestCloseWhenIdle(t *testing.T) {
	s := lifecycle.New(context.Background(), lifecycle.CancelOnError())
	release := make(chan struct{})
	var ran atomic.Int32
	mustGo(t, s, "outer", func(context.Context) error {
		<-release
		return s.Go("inner", func(context.Context) error { ran.Add(1); return nil })
	})
	k, _ := s.Child("k", lifecycle.CancelOnError())
	s.CloseWhenIdle()
	close(release)
	time.Sleep(5 * time.Millisecond)
	if s.Cause() != nil {
		t.Fatal("closed while a child was alive")
	}
	k.CloseWhenIdle() // an idle child closes at once
	if err := wait(t, s); err != nil || ran.Load() != 1 {
		t.Fatalf("result %v, inner ran %d", err, ran.Load())
	}
	if !errors.Is(s.Cause(), lifecycle.ErrClosed) {
		t.Fatalf("cause %v", s.Cause())
	}
}

func TestRun(t *testing.T) {
	ctx := context.Background()
	var n atomic.Int32
	err := lifecycle.Run(ctx, func(s *lifecycle.Scope) error {
		for range 5 {
			mustGo(t, s, "t", func(context.Context) error { n.Add(1); return nil })
		}
		return nil
	})
	if err != nil || n.Load() != 5 {
		t.Fatalf("run: %v, %d tasks", err, n.Load())
	}

	boom := errors.New("boom")
	err = lifecycle.Run(ctx, func(s *lifecycle.Scope) error {
		mustGo(t, s, "waiter", func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() })
		mustGo(t, s, "failer", func(context.Context) error { return boom })
		return nil
	})
	var te *lifecycle.TaskError
	if !errors.As(err, &te) || te.Name != "failer" || !errors.Is(err, boom) {
		t.Fatalf("first error %v", err)
	}
	if err := lifecycle.Run(ctx, func(*lifecycle.Scope) error { return boom }); !errors.Is(err, boom) {
		t.Fatalf("fn error %v", err)
	}

	cctx, cancel := context.WithCancelCause(ctx)
	stop := errors.New("stop")
	err = lifecycle.Run(cctx, func(s *lifecycle.Scope) error {
		mustGo(t, s, "waiter", func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() })
		cancel(stop)
		return nil
	})
	if !errors.Is(err, stop) {
		t.Fatalf("cancelled run: %v", err)
	}

	var closed atomic.Bool
	func() {
		defer func() { _ = recover() }()
		err := lifecycle.Run(ctx, func(s *lifecycle.Scope) error {
			mustGo(t, s, "waiter", func(ctx context.Context) error { <-ctx.Done(); closed.Store(true); return nil })
			panic("fn")
		})
		t.Errorf("Run returned %v; want its panic re-raised", err)
	}()
	for deadline := time.Now().Add(waitLimit); !closed.Load() && time.Now().Before(deadline); {
		time.Sleep(time.Millisecond)
	}
	if !closed.Load() {
		t.Fatal("panic in fn left the scope open")
	}
}

func TestInvalidPolicy(t *testing.T) {
	for name, fn := range map[string]func(){
		"zero":        func() { lifecycle.New(context.Background(), lifecycle.Policy{}) },
		"nil handler": func() { lifecycle.New(context.Background(), lifecycle.Isolate(nil)) },
		"nil context": func() { lifecycle.New(nil, lifecycle.CancelOnError()) }, //nolint:staticcheck
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("%s: no panic", name)
				}
			}()
			fn()
		}()
	}
}

func TestRunCycle(t *testing.T) {
	q, _ := lifecycle.NewQueue(lifecycle.QueueConfig[int]{Capacity: 4, Full: lifecycle.Block})
	err := lifecycletest.RunCycle(t, waitLimit, func(s *lifecycle.Scope) error {
		if _, err := s.Add(q); err != nil {
			return err
		}
		k, err := s.Child("consumers", lifecycle.CancelOnError())
		if err != nil {
			return err
		}
		for range 2 {
			mustGo(t, k, "consumer", func(ctx context.Context) error {
				for {
					if _, err := q.Pop(ctx); err != nil {
						return err
					}
				}
			})
		}
		return s.Go("producer", func(ctx context.Context) error {
			for i := 0; ; i++ {
				if err := q.Push(ctx, i); err != nil {
					return err
				}
			}
		})
	})
	if err != nil {
		t.Fatal(err)
	}
}
