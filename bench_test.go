package lifecycle_test

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/djylb/lifecycle"
)

func noop(context.Context) error { return nil }

// BenchmarkParentGo starts short tasks on one parent from every P.
func BenchmarkParentGo(b *testing.B) {
	s := lifecycle.New(context.Background(), lifecycle.CancelOnError())
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := s.Go("t", noop); err != nil {
				b.Error(err)
				return
			}
		}
	})
	s.Close(nil)
	waitB(b, s)
}

// BenchmarkParentChild opens and closes child scopes of one parent from
// every P.
func BenchmarkParentChild(b *testing.B) {
	s := lifecycle.New(context.Background(), lifecycle.CancelOnError())
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			k, err := s.Child("c", lifecycle.CancelOnError())
			if err != nil {
				b.Error(err)
				return
			}
			k.Close(nil)
		}
	})
	s.Close(nil)
	waitB(b, s)
}

func waitB(b *testing.B, s *lifecycle.Scope) {
	b.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), waitLimit)
	defer cancel()
	if err := s.Wait(ctx); err != nil {
		b.Fatal(err)
	}
}

// BenchmarkChildren10 is the small end: one goroutine makes a scope with
// ten children and a task each, then closes it.
func BenchmarkChildren10(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		s := lifecycle.New(context.Background(), lifecycle.CancelOnError())
		for range 10 {
			k, err := s.Child("c", lifecycle.CancelOnError())
			if err != nil {
				b.Fatal(err)
			}
			if err := k.Go("t", noop); err != nil {
				b.Fatal(err)
			}
		}
		s.Close(nil)
		waitB(b, s)
	}
}

// BenchmarkChildren1M is the large end: every P adds children to one
// parent until it holds a million, then the parent is closed and waited
// for. ns/op is per child.
func BenchmarkChildren1M(b *testing.B) {
	const n = 1 << 20
	for b.Loop() {
		s := lifecycle.New(context.Background(), lifecycle.CancelOnError())
		var left atomic.Int64
		left.Store(n)
		var wg sync.WaitGroup
		for range runtime.GOMAXPROCS(0) {
			wg.Go(func() {
				for left.Add(-1) >= 0 {
					if _, err := s.Child("c", lifecycle.CancelOnError()); err != nil {
						b.Error(err)
						return
					}
				}
			})
		}
		wg.Wait()
		s.Close(nil)
		waitB(b, s)
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*n), "ns/child")
}
