package lifecycletest_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/djylb/lifecycle"
	"github.com/djylb/lifecycle/lifecycletest"
)

func TestMain(m *testing.M) { lifecycletest.VerifyMain(m) }

func mustGo(t testing.TB, s *lifecycle.Scope, name string, fn func(context.Context) error) {
	t.Helper()
	if err := s.Go(name, fn); err != nil {
		t.Fatalf("Go %q: %v", name, err)
	}
}

// recorder captures failures instead of failing the test.
type recorder struct {
	testing.TB
	failed []string
}

func (r *recorder) Helper() {}

func (r *recorder) Errorf(format string, args ...any) {
	r.failed = append(r.failed, fmt.Sprintf(format, args...))
}

func (r *recorder) Error(args ...any) { r.failed = append(r.failed, fmt.Sprint(args...)) }

func TestLeaks(t *testing.T) {
	base := lifecycletest.Goroutines()
	stop := make(chan struct{})
	go func() { <-stop }()
	leaked := lifecycletest.Leaks(base, 10*time.Millisecond)
	if len(leaked) != 1 || leaked[0].State != "chan receive" || !strings.Contains(leaked[0].Stack, "TestLeaks") {
		t.Fatalf("leaks %v", leaked)
	}
	if got := lifecycletest.Leaks(base, 0, "TestLeaks"); len(got) != 0 {
		t.Fatalf("ignored goroutine reported: %v", got)
	}
	close(stop)
	if got := lifecycletest.Leaks(base, time.Second); len(got) != 0 {
		t.Fatalf("ended goroutine reported: %v", got)
	}
}

func TestCheckGoroutines(t *testing.T) {
	lifecycletest.CheckGoroutines(t)
	done := make(chan struct{})
	go func() { close(done) }()
	<-done
}

func TestAssertEmpty(t *testing.T) {
	s := lifecycle.New(context.Background(), lifecycle.CancelOnError())
	mustGo(t, s, "t", func(ctx context.Context) error { <-ctx.Done(); return nil })
	r := &recorder{TB: t}
	lifecycletest.AssertEmpty(r, s)
	if len(r.failed) != 1 {
		t.Fatalf("busy scope passed: %v", r.failed)
	}
	s.Close(nil)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	lifecycletest.AssertEmpty(t, s)
}

func TestRunCycle(t *testing.T) {
	boom := errors.New("boom")
	err := lifecycletest.RunCycle(t, 5*time.Second, func(s *lifecycle.Scope) error {
		mustGo(t, s, "server", func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() })
		mustGo(t, s, "cleanup", func(ctx context.Context) error { <-ctx.Done(); return boom })
		return nil
	})
	if !errors.Is(err, boom) {
		t.Fatalf("result %v", err)
	}

	// A goroutine outside the scope that outlives the cycle is reported.
	r := &recorder{TB: t}
	stop := make(chan struct{})
	defer close(stop)
	err = lifecycletest.RunCycle(r, 20*time.Millisecond, func(s *lifecycle.Scope) error {
		go func() { <-stop }()
		return nil
	})
	if err != nil || len(r.failed) != 1 || !strings.Contains(r.failed[0], "leaked") {
		t.Fatalf("leak not reported: %v", r.failed)
	}
}
