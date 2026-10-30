package lifecycle_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/djylb/lifecycle"
	"github.com/djylb/lifecycle/lifecycletest"
)

type countCloser struct{ n atomic.Int32 }

func (c *countCloser) Close() error { c.n.Add(1); return nil }

// race runs ops against a scope tree while another goroutine closes it,
// and checks the guarantees: a task accepted by Go runs exactly once and is
// joined, a resource handed to Add is closed exactly once unless it was
// removed, and the tree ends empty.
func race(t *testing.T, ops []byte, closeAfter int) {
	root := lifecycle.New(context.Background(), lifecycle.Isolate(func(error) {}))
	scopes := []*lifecycle.Scope{root}
	var smu sync.Mutex
	pick := func(b byte) *lifecycle.Scope {
		smu.Lock()
		defer smu.Unlock()
		return scopes[int(b)%len(scopes)]
	}
	var accepted, ran atomic.Int32
	var closers []*countCloser
	var removed []bool
	var cmu sync.Mutex

	start := make(chan struct{})
	var wg sync.WaitGroup
	for w := range 4 {
		wg.Go(func() {
			<-start
			for i := w; i+1 < len(ops); i += 4 {
				op, arg := ops[i], ops[i+1]
				s := pick(arg)
				switch op % 6 {
				case 0, 1:
					if s.Go("t", func(ctx context.Context) error {
						ran.Add(1)
						if arg&1 == 1 {
							<-ctx.Done()
						}
						if arg&2 == 2 {
							return errors.New("task error")
						}
						return nil
					}) == nil {
						accepted.Add(1)
					}
				case 2:
					c := &countCloser{}
					rm, _ := s.Add(c)
					rem := arg&1 == 1 && rm()
					cmu.Lock()
					closers = append(closers, c)
					removed = append(removed, rem)
					cmu.Unlock()
				case 3:
					policy := lifecycle.CancelOnError()
					if arg&1 == 1 {
						policy = lifecycle.Isolate(func(error) {})
					}
					if arg&4 == 4 {
						policy = policy.Propagate()
					}
					if k, err := s.Child("k", policy); err == nil {
						smu.Lock()
						scopes = append(scopes, k)
						smu.Unlock()
					}
				case 4:
					s.Close(nil)
				case 5:
					s.CloseWhenIdle()
				}
			}
		})
	}
	close(start)
	if closeAfter > 0 {
		time.Sleep(time.Duration(closeAfter) * time.Microsecond)
	}
	root.Close(nil)
	wg.Wait()
	ctx, cancel := context.WithTimeout(context.Background(), waitLimit)
	defer cancel()
	if err := root.Wait(ctx); errors.Is(err, lifecycle.ErrNotFinished) {
		t.Fatalf("tree did not finish: %+v", root.Stats())
	}
	if accepted.Load() != ran.Load() {
		t.Fatalf("accepted %d tasks, ran %d", accepted.Load(), ran.Load())
	}
	for i, c := range closers {
		want := int32(1)
		if removed[i] {
			want = 0
		}
		if got := c.n.Load(); got != want {
			t.Fatalf("closer %d (removed %v) closed %d times", i, removed[i], got)
		}
	}
	lifecycletest.AssertEmpty(t, root)
}

func TestStressCloseRaces(t *testing.T) {
	deadline := time.Now().Add(2 * time.Second)
	if testing.Short() {
		deadline = time.Now().Add(200 * time.Millisecond)
	}
	ops := make([]byte, 512)
	for round := 0; time.Now().Before(deadline); round++ {
		for i := range ops {
			ops[i] = byte(i*7 + round*13 + i*i*round)
		}
		race(t, ops, round%50)
	}
}

func FuzzCloseRaces(f *testing.F) {
	f.Add([]byte{0, 1, 2, 3, 3, 0, 4, 1, 0, 2}, uint8(0))
	f.Add([]byte{3, 0, 3, 1, 0, 1, 2, 1, 1, 3, 4, 2}, uint8(10))
	f.Fuzz(func(t *testing.T, ops []byte, closeAfter uint8) {
		if len(ops) > 4096 {
			ops = ops[:4096]
		}
		race(t, ops, int(closeAfter%64))
	})
}
