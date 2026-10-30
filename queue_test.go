package lifecycle_test

import (
	"context"
	"errors"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/djylb/lifecycle"
)

func newQueue[T any](t *testing.T, cfg lifecycle.QueueConfig[T]) *lifecycle.Queue[T] {
	t.Helper()
	q, err := lifecycle.NewQueue(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return q
}

func mustPush[T any](t testing.TB, q *lifecycle.Queue[T], v T) {
	t.Helper()
	if err := q.Push(context.Background(), v); err != nil {
		t.Fatalf("push: %v", err)
	}
}

func mustClose[T any](t testing.TB, q *lifecycle.Queue[T]) {
	t.Helper()
	if err := q.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func TestQueueConfig(t *testing.T) {
	drop := func(int) {}
	for _, cfg := range []lifecycle.QueueConfig[int]{
		{},
		{Capacity: 1},
		{Full: lifecycle.Reject},
		{Capacity: -1, Full: lifecycle.Reject},
		{Capacity: 1, Full: lifecycle.Block + 1},
		{Capacity: 1, Full: lifecycle.DropOldest},
		{Capacity: 1, Full: lifecycle.Reject, OnDrop: drop},
		{Capacity: 1, Full: lifecycle.Reject, BlockTimeout: time.Second},
		{Capacity: 1, Full: lifecycle.Block, BlockTimeout: -1},
	} {
		if _, err := lifecycle.NewQueue(cfg); err == nil {
			t.Errorf("accepted %+v", cfg)
		}
	}
}

func TestQueueReject(t *testing.T) {
	q := newQueue(t, lifecycle.QueueConfig[int]{Capacity: 2, Full: lifecycle.Reject})
	ctx := context.Background()
	for i := range 3 {
		err := q.Push(ctx, i)
		if (i < 2) != (err == nil) || (i == 2 && !errors.Is(err, lifecycle.ErrFull)) {
			t.Fatalf("push %d: %v", i, err)
		}
	}
	if v, _ := q.Pop(ctx); v != 0 {
		t.Fatalf("pop %d", v)
	}
	if st := q.Stats(); st != (lifecycle.QueueStats{Pushed: 2, Rejected: 1, Len: 1, Cap: 2}) {
		t.Fatalf("stats %+v", st)
	}
}

func TestQueueDropOldest(t *testing.T) {
	var dropped []int
	q := newQueue(t, lifecycle.QueueConfig[int]{Capacity: 2, Full: lifecycle.DropOldest, OnDrop: func(v int) { dropped = append(dropped, v) }})
	for i := range 5 {
		if err := q.Push(context.Background(), i); err != nil {
			t.Fatal(err)
		}
	}
	var got []int
	q.Drain(func(v int) { got = append(got, v) })
	if !slices.Equal(dropped, []int{0, 1, 2}) || !slices.Equal(got, []int{3, 4}) {
		t.Fatalf("dropped %v, kept %v", dropped, got)
	}
	if st := q.Stats(); st.Pushed != 5 || st.Dropped != 3 || st.Len != 0 {
		t.Fatalf("stats %+v", st)
	}
}

func TestQueueBlock(t *testing.T) {
	q := newQueue(t, lifecycle.QueueConfig[int]{Capacity: 1, Full: lifecycle.Block, BlockTimeout: 10 * time.Millisecond})
	ctx := context.Background()
	mustPush(t, q, 1)
	if err := q.Push(ctx, 2); !errors.Is(err, lifecycle.ErrFull) {
		t.Fatalf("timeout push: %v", err)
	}
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if err := q.Push(cctx, 2); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled push: %v", err)
	}

	// A waiting push completes once a pop makes room.
	q2 := newQueue(t, lifecycle.QueueConfig[int]{Capacity: 1, Full: lifecycle.Block})
	mustPush(t, q2, 1)
	res := make(chan error)
	go func() { res <- q2.Push(ctx, 2) }()
	for q2.Stats().Waited == 0 {
		time.Sleep(time.Millisecond)
	}
	if v, _ := q2.Pop(ctx); v != 1 {
		t.Fatalf("pop %d", v)
	}
	if err := <-res; err != nil {
		t.Fatal(err)
	}
	if st := q.Stats(); st.Waited != 2 || st.Rejected != 2 || st.Pushed != 1 {
		t.Fatalf("stats %+v", st)
	}
}

func TestQueueCloseWakesAndDrains(t *testing.T) {
	ctx := context.Background()
	q := newQueue(t, lifecycle.QueueConfig[int]{Capacity: 1, Full: lifecycle.Block})
	mustPush(t, q, 7)
	pushed := make(chan error)
	go func() { pushed <- q.Push(ctx, 8) }()
	for q.Stats().Waited == 0 {
		time.Sleep(time.Millisecond)
	}
	mustClose(t, q)
	mustClose(t, q)
	if err := <-pushed; !errors.Is(err, lifecycle.ErrQueueClosed) {
		t.Fatalf("waiting push: %v", err)
	}
	if err := q.Push(ctx, 9); !errors.Is(err, lifecycle.ErrQueueClosed) {
		t.Fatalf("push after close: %v", err)
	}
	if v, err := q.Pop(ctx); v != 7 || err != nil {
		t.Fatalf("pop after close: %d %v", v, err)
	}
	if _, err := q.Pop(ctx); !errors.Is(err, lifecycle.ErrQueueClosed) {
		t.Fatalf("pop of empty closed queue: %v", err)
	}

	e := newQueue(t, lifecycle.QueueConfig[int]{Capacity: 1, Full: lifecycle.Reject})
	popped := make(chan error)
	go func() { _, err := e.Pop(ctx); popped <- err }()
	time.Sleep(5 * time.Millisecond)
	mustClose(t, e)
	if err := <-popped; !errors.Is(err, lifecycle.ErrQueueClosed) {
		t.Fatalf("waiting pop: %v", err)
	}
	cctx, cancel := context.WithTimeout(ctx, time.Millisecond)
	defer cancel()
	o := newQueue(t, lifecycle.QueueConfig[int]{Capacity: 1, Full: lifecycle.Reject})
	if _, err := o.Pop(cctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("pop with deadline: %v", err)
	}
}

// TestQueueConservation checks under concurrency that every accepted item is
// popped, dropped or drained exactly once, also across Close.
func TestQueueConservation(t *testing.T) {
	for _, full := range []lifecycle.FullPolicy{lifecycle.Reject, lifecycle.DropOldest, lifecycle.Block} {
		t.Run(full.String(), func(t *testing.T) {
			const producers, perProducer = 4, 2000
			var mu sync.Mutex
			seen := make(map[int]int)
			mark := func(v int) { mu.Lock(); seen[v]++; mu.Unlock() }
			cfg := lifecycle.QueueConfig[int]{Capacity: 8, Full: full}
			if full == lifecycle.DropOldest {
				cfg.OnDrop = mark
			}
			if full == lifecycle.Block {
				cfg.BlockTimeout = time.Millisecond
			}
			q := newQueue(t, cfg)
			ctx := context.Background()
			var accepted sync.Map
			var wg sync.WaitGroup
			for p := range producers {
				wg.Go(func() {
					for i := range perProducer {
						v := p*perProducer + i
						if q.Push(ctx, v) == nil {
							accepted.Store(v, true)
						}
					}
				})
			}
			var cw sync.WaitGroup
			for range 3 {
				cw.Go(func() {
					for {
						v, err := q.Pop(ctx)
						if err != nil {
							return
						}
						mark(v)
					}
				})
			}
			time.Sleep(time.Millisecond)
			mustClose(t, q)
			wg.Wait()
			cw.Wait()
			if n := q.Drain(mark); n != 0 {
				t.Errorf("%d items left after the consumers stopped", n)
			}
			n := 0
			accepted.Range(func(k, _ any) bool {
				n++
				if seen[k.(int)] != 1 {
					t.Errorf("item %v seen %d times", k, seen[k.(int)])
				}
				return true
			})
			if len(seen) != n || uint64(n) != q.Stats().Pushed {
				t.Fatalf("accepted %d, seen %d, stats %+v", n, len(seen), q.Stats())
			}
		})
	}
}

func TestQueueAllocs(t *testing.T) {
	ctx := context.Background()
	for _, full := range []lifecycle.FullPolicy{lifecycle.Reject, lifecycle.DropOldest, lifecycle.Block} {
		cfg := lifecycle.QueueConfig[int]{Capacity: 1, Full: full}
		if full == lifecycle.DropOldest {
			cfg.OnDrop = func(int) {}
		}
		q := newQueue(t, cfg)
		allocs := testing.AllocsPerRun(100, func() {
			if err := q.Push(ctx, 1); err != nil {
				t.Fatal(err)
			}
			if full != lifecycle.Block {
				// Full: rejected, or the oldest item is dropped.
				if err := q.Push(ctx, 2); (err != nil) != (full == lifecycle.Reject) {
					t.Fatal(err)
				}
			}
			if _, err := q.Pop(ctx); err != nil {
				t.Fatal(err)
			}
		})
		if allocs != 0 {
			t.Errorf("%v: %v allocations per push and pop", full, allocs)
		}
	}
}

func TestZeroQueue(t *testing.T) {
	var q lifecycle.Queue[int]
	if err := q.Push(context.Background(), 1); err == nil {
		t.Fatal("zero Queue accepted an item")
	}
}

// TestQueueDrainAfterCloseSeesInFlight: Drain right after Close, while
// pushes may still be in flight, must remove every accepted item, and every
// DropOldest removal must be matched by an accepted replacement.
func TestQueueDrainAfterCloseSeesInFlight(t *testing.T) {
	for _, full := range []lifecycle.FullPolicy{lifecycle.Reject, lifecycle.DropOldest} {
		for round := range 200 {
			var dropped atomic.Int64
			cfg := lifecycle.QueueConfig[int]{Capacity: 4, Full: full}
			if full == lifecycle.DropOldest {
				cfg.OnDrop = func(int) { dropped.Add(1) }
			}
			q := newQueue(t, cfg)
			var accepted atomic.Int64
			var wg sync.WaitGroup
			for range 4 {
				wg.Go(func() {
					for {
						switch err := q.Push(context.Background(), round); err {
						case nil:
							accepted.Add(1)
						case lifecycle.ErrQueueClosed:
							return
						}
						runtime.Gosched() // single-threaded targets have no preemption
					}
				})
			}
			time.Sleep(time.Duration(round%5) * 10 * time.Microsecond)
			mustClose(t, q)
			drained := q.Drain(nil)
			wg.Wait()
			if q.Len() != 0 {
				t.Fatalf("%v: %d items left after Drain", full, q.Len())
			}
			if full == lifecycle.DropOldest && int64(drained) != accepted.Load()-dropped.Load() {
				t.Fatalf("DropOldest: accepted %d, dropped %d, drained %d", accepted.Load(), dropped.Load(), drained)
			}
		}
	}
}
