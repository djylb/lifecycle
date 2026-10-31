package lifecycle_test

import (
	"context"
	"errors"
	"io"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/djylb/lifecycle"
)

func mustSetLatest[T any](t testing.TB, l *lifecycle.Latest[T], v T) {
	t.Helper()
	if err := l.Set(v); err != nil {
		t.Fatalf("set: %v", err)
	}
}

func mustSetKeyed[K comparable, T any](t testing.TB, q *lifecycle.KeyedLatest[K, T], k K, v T) {
	t.Helper()
	if err := q.Set(context.Background(), k, v); err != nil {
		t.Fatalf("set: %v", err)
	}
}

func mustCloseC(t testing.TB, c io.Closer) {
	t.Helper()
	if err := c.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func TestLatestOverwrite(t *testing.T) {
	var dropped []int
	l := lifecycle.NewLatest(lifecycle.LatestConfig[int]{OnDrop: func(v int) { dropped = append(dropped, v) }})
	if _, _, ok := l.TryGet(); ok {
		t.Fatal("empty TryGet ok")
	}
	for i := range 3 {
		if err := l.Set(i); err != nil {
			t.Fatal(err)
		}
	}
	v, missed, err := l.Wait(context.Background())
	if v != 2 || missed != 2 || err != nil || !slices.Equal(dropped, []int{0, 1}) {
		t.Fatalf("got %d %d %v dropped %v", v, missed, err, dropped)
	}
	mustSetLatest(t, l, 3)
	if v, missed, ok := l.TryGet(); v != 3 || missed != 0 || !ok {
		t.Fatalf("TryGet %d %d %v", v, missed, ok)
	}
	if st := l.Stats(); st != (lifecycle.LatestStats{Pushed: 4, Overwritten: 2}) {
		t.Fatalf("stats %+v", st)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, _, err := l.Wait(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("wait on empty: %v", err)
	}
}

func TestLatestWaitWakesAndClose(t *testing.T) {
	l := lifecycle.NewLatest(lifecycle.LatestConfig[int]{})
	got := make(chan int)
	go func() {
		v, _, err := l.Wait(context.Background())
		if err != nil {
			t.Error(err)
		}
		got <- v
	}()
	time.Sleep(5 * time.Millisecond)
	mustSetLatest(t, l, 7)
	if v := <-got; v != 7 {
		t.Fatalf("got %d", v)
	}

	errc := make(chan error)
	go func() { _, _, err := l.Wait(context.Background()); errc <- err }()
	time.Sleep(5 * time.Millisecond)
	mustSetLatest(t, l, 8)
	if err := <-errc; err != nil {
		t.Fatal(err)
	}
	go func() { _, _, err := l.Wait(context.Background()); errc <- err }()
	time.Sleep(5 * time.Millisecond)
	mustCloseC(t, l)
	if err := <-errc; !errors.Is(err, lifecycle.ErrQueueClosed) {
		t.Fatalf("wait after close: %v", err)
	}

	l2 := lifecycle.NewLatest(lifecycle.LatestConfig[int]{})
	mustSetLatest(t, l2, 1)
	mustCloseC(t, l2)
	mustCloseC(t, l2)
	if err := l2.Set(2); !errors.Is(err, lifecycle.ErrQueueClosed) {
		t.Fatalf("set after close: %v", err)
	}
	if v, _, err := l2.Wait(context.Background()); v != 1 || err != nil {
		t.Fatalf("pending after close: %d %v", v, err)
	}
	if n := l2.Drain(nil); n != 0 {
		t.Fatalf("drain %d", n)
	}
}

func TestZeroLatest(t *testing.T) {
	var l lifecycle.Latest[int]
	if l.Set(1) == nil {
		t.Fatal("zero Set ok")
	}
	if _, _, err := l.Wait(context.Background()); err == nil {
		t.Fatal("zero Wait ok")
	}
	if _, _, ok := l.TryGet(); ok || l.Drain(nil) != 0 || l.Stats() != (lifecycle.LatestStats{}) {
		t.Fatal("zero Latest not empty")
	}
	mustCloseC(t, &l)
	var k lifecycle.KeyedLatest[int, int]
	if k.Set(context.Background(), 1, 1) == nil {
		t.Fatal("zero Set ok")
	}
	if _, _, err := k.Pop(context.Background()); err == nil {
		t.Fatal("zero Pop ok")
	}
	_ = k.Close() // only checks that closing a zero value does not panic
}

func newKeyed[K comparable, T any](t testing.TB, cfg lifecycle.KeyedLatestConfig[K, T]) *lifecycle.KeyedLatest[K, T] {
	t.Helper()
	q, err := lifecycle.NewKeyedLatest(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return q
}

func TestKeyedLatestConfig(t *testing.T) {
	drop := func(int, int) {}
	for _, cfg := range []lifecycle.KeyedLatestConfig[int, int]{
		{},
		{Capacity: 1},
		{Full: lifecycle.Reject},
		{Capacity: 1, Full: lifecycle.Block + 1},
		{Capacity: 1, Full: lifecycle.DropOldest},
		{Capacity: 1, Full: lifecycle.Reject, BlockTimeout: time.Second},
		{Capacity: 1, Full: lifecycle.Block, BlockTimeout: -1},
	} {
		if _, err := lifecycle.NewKeyedLatest(cfg); err == nil {
			t.Errorf("accepted %+v", cfg)
		}
	}
	if _, err := lifecycle.NewKeyedLatest(lifecycle.KeyedLatestConfig[int, int]{Capacity: 1, Full: lifecycle.Reject, OnDrop: drop}); err != nil {
		t.Error(err)
	}
}

type kv struct{ k, v int }

func TestKeyedLatestOrderAndOverwrite(t *testing.T) {
	var dropped []kv
	q := newKeyed(t, lifecycle.KeyedLatestConfig[int, int]{Capacity: 3, Full: lifecycle.Reject,
		OnDrop: func(k, v int) { dropped = append(dropped, kv{k, v}) }})
	ctx := context.Background()
	for _, e := range []kv{{1, 10}, {2, 20}, {1, 11}, {3, 30}, {2, 21}} {
		if err := q.Set(ctx, e.k, e.v); err != nil {
			t.Fatal(err)
		}
	}
	if err := q.Set(ctx, 4, 40); !errors.Is(err, lifecycle.ErrFull) {
		t.Fatalf("new key on full: %v", err)
	}
	if err := q.Set(ctx, 3, 31); err != nil {
		t.Fatalf("pending key on full: %v", err)
	}
	var got []kv
	q.Drain(func(k, v int) { got = append(got, kv{k, v}) })
	if !slices.Equal(got, []kv{{1, 11}, {2, 21}, {3, 31}}) || !slices.Equal(dropped, []kv{{1, 10}, {2, 20}, {3, 30}}) {
		t.Fatalf("got %v dropped %v", got, dropped)
	}
	if st := q.Stats(); st != (lifecycle.KeyedLatestStats{Pushed: 6, Overwritten: 3, Rejected: 1, Cap: 3}) {
		t.Fatalf("stats %+v", st)
	}
}

func TestKeyedLatestDropOldest(t *testing.T) {
	var dropped []kv
	q := newKeyed(t, lifecycle.KeyedLatestConfig[int, int]{Capacity: 2, Full: lifecycle.DropOldest,
		OnDrop: func(k, v int) { dropped = append(dropped, kv{k, v}) }})
	ctx := context.Background()
	for i := range 4 {
		mustSetKeyed(t, q, i, i)
	}
	k, v, err := q.Pop(ctx)
	if k != 2 || v != 2 || err != nil || !slices.Equal(dropped, []kv{{0, 0}, {1, 1}}) {
		t.Fatalf("pop %d %d %v dropped %v", k, v, err, dropped)
	}
	if st := q.Stats(); st.Evicted != 2 || st.Len != 1 {
		t.Fatalf("stats %+v", st)
	}
}

func TestKeyedLatestBlock(t *testing.T) {
	q := newKeyed(t, lifecycle.KeyedLatestConfig[int, int]{Capacity: 1, Full: lifecycle.Block, BlockTimeout: 10 * time.Millisecond})
	ctx := context.Background()
	mustSetKeyed(t, q, 1, 1)
	if err := q.Set(ctx, 1, 2); err != nil {
		t.Fatalf("overwrite blocked: %v", err)
	}
	if err := q.Set(ctx, 2, 2); !errors.Is(err, lifecycle.ErrFull) {
		t.Fatalf("timeout: %v", err)
	}
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if err := q.Set(cctx, 2, 2); !errors.Is(err, context.Canceled) {
		t.Fatalf("ctx: %v", err)
	}

	q2 := newKeyed(t, lifecycle.KeyedLatestConfig[int, int]{Capacity: 1, Full: lifecycle.Block})
	mustSetKeyed(t, q2, 0, 0)
	errc := make(chan error, 3)
	for k := 1; k <= 3; k++ {
		go func() { errc <- q2.Set(ctx, k, k) }()
	}
	for q2.Stats().Waited < 3 {
		time.Sleep(time.Millisecond)
	}
	// One Pop frees one slot at a time; every blocked Set must get in.
	for range 3 {
		if _, _, err := q2.Pop(ctx); err != nil {
			t.Fatal(err)
		}
		if err := <-errc; err != nil {
			t.Fatal(err)
		}
	}
	mustCloseC(t, q2)
	if q2.Drain(nil) != 1 {
		t.Fatal("last key not drained")
	}
	if _, _, err := q2.Pop(ctx); !errors.Is(err, lifecycle.ErrQueueClosed) {
		t.Fatalf("pop after close: %v", err)
	}
	if err := q2.Set(ctx, 9, 9); !errors.Is(err, lifecycle.ErrQueueClosed) {
		t.Fatalf("set after close: %v", err)
	}

	// Close wakes a blocked Set.
	q3 := newKeyed(t, lifecycle.KeyedLatestConfig[int, int]{Capacity: 1, Full: lifecycle.Block})
	mustSetKeyed(t, q3, 0, 0)
	go func() { errc <- q3.Set(ctx, 1, 1) }()
	for q3.Stats().Waited < 1 {
		time.Sleep(time.Millisecond)
	}
	mustCloseC(t, q3)
	if err := <-errc; !errors.Is(err, lifecycle.ErrQueueClosed) {
		t.Fatalf("blocked set after close: %v", err)
	}
}

func TestLatestAllocs(t *testing.T) {
	ctx := context.Background()
	l := lifecycle.NewLatest(lifecycle.LatestConfig[int]{OnDrop: func(int) {}})
	if n := testing.AllocsPerRun(100, func() {
		if l.Set(1) != nil || l.Set(2) != nil {
			t.Fatal("set")
		}
		if _, _, err := l.Wait(ctx); err != nil {
			t.Fatal(err)
		}
	}); n != 0 {
		t.Errorf("Latest: %v allocs", n)
	}
	for _, full := range []lifecycle.FullPolicy{lifecycle.Reject, lifecycle.DropOldest, lifecycle.Block} {
		q := newKeyed(t, lifecycle.KeyedLatestConfig[int, int]{Capacity: 8, Full: full, OnDrop: func(int, int) {}})
		i := 0
		if n := testing.AllocsPerRun(1000, func() {
			for j := range 8 {
				if q.Set(ctx, i+j, j) != nil {
					t.Fatal("set")
				}
			}
			if q.Set(ctx, i, 0) != nil {
				t.Fatal("overwrite")
			}
			for range 8 {
				if _, _, err := q.Pop(ctx); err != nil {
					t.Fatal(err)
				}
			}
			i += 8
		}); n != 0 {
			t.Errorf("KeyedLatest %v: %v allocs", full, n)
		}
	}
}

// TestKeyedLatestModel checks random operations against a simple model.
func TestKeyedLatestModel(t *testing.T) {
	for seed := range 200 {
		ops := make([]byte, 64)
		for i := range ops {
			ops[i] = byte(seed*31 + i*i*7 + i*seed)
		}
		keyedModel(t, ops)
	}
}

func FuzzKeyedLatest(f *testing.F) {
	f.Add([]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
	f.Add([]byte{255, 0, 128, 3, 3, 3, 64, 200})
	f.Fuzz(func(t *testing.T, ops []byte) { keyedModel(t, ops) })
}

func keyedModel(t *testing.T, ops []byte) {
	if len(ops) < 2 {
		return
	}
	capacity := int(ops[0]%4) + 1
	full := lifecycle.FullPolicy(ops[1]%2 + 1) // Reject or DropOldest; Block would wait
	var dropped []kv
	q := newKeyed(t, lifecycle.KeyedLatestConfig[int, int]{Capacity: capacity, Full: full,
		OnDrop: func(k, v int) { dropped = append(dropped, kv{k, v}) }})
	var model []kv
	var wantDropped []kv
	ctx := context.Background()
	for i, b := range ops[2:] {
		if b&3 == 0 {
			k, v, ok := q.TryPop()
			if ok != (len(model) > 0) || ok && (kv{k, v} != model[0]) {
				t.Fatalf("op %d: pop %d %d %v, model %v", i, k, v, ok, model)
			}
			if ok {
				model = model[1:]
			}
			continue
		}
		k := int(b>>2) % 6
		err := q.Set(ctx, k, i)
		pos := slices.IndexFunc(model, func(e kv) bool { return e.k == k })
		switch {
		case pos >= 0:
			wantDropped = append(wantDropped, model[pos])
			model[pos].v = i
		case len(model) < capacity:
			model = append(model, kv{k, i})
		case full == lifecycle.Reject:
			if !errors.Is(err, lifecycle.ErrFull) {
				t.Fatalf("op %d: want ErrFull, got %v", i, err)
			}
			continue
		default:
			wantDropped = append(wantDropped, model[0])
			model = append(model[1:], kv{k, i})
		}
		if err != nil {
			t.Fatalf("op %d: %v", i, err)
		}
	}
	if !slices.Equal(dropped, wantDropped) || q.Len() != len(model) {
		t.Fatalf("dropped %v want %v; len %d model %v", dropped, wantDropped, q.Len(), model)
	}
}

// TestLatestStress checks with concurrent setters that every value is
// either taken once or dropped once, and that the consumer sees each
// setter's values in increasing order.
func TestLatestStress(t *testing.T) {
	deadline := time.Now().Add(300 * time.Millisecond)
	const setters = 4
	var droppedN atomic.Int64
	l := lifecycle.NewLatest(lifecycle.LatestConfig[[2]int]{OnDrop: func([2]int) { droppedN.Add(1) }})
	var sent atomic.Int64
	var wg sync.WaitGroup
	for w := range setters {
		wg.Go(func() {
			for i := 0; time.Now().Before(deadline); i++ {
				if l.Set([2]int{w, i}) != nil {
					return
				}
				sent.Add(1)
				runtime.Gosched()
			}
		})
	}
	last := [setters]int{-1, -1, -1, -1}
	var got, missed int64
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			v, m, err := l.Wait(context.Background())
			if err != nil {
				return
			}
			got++
			missed += int64(m)
			if v[1] <= last[v[0]] {
				t.Errorf("setter %d: %d after %d", v[0], v[1], last[v[0]])
			}
			last[v[0]] = v[1]
		}
	}()
	wg.Wait()
	mustCloseC(t, l)
	<-done
	if got+droppedN.Load() != sent.Load() || missed != droppedN.Load() {
		t.Fatalf("sent %d got %d dropped %d missed %d", sent.Load(), got, droppedN.Load(), missed)
	}
}

// TestKeyedLatestStress runs concurrent setters and poppers under each
// policy and checks conservation: every accepted value is popped, dropped
// or drained exactly once.
func TestKeyedLatestStress(t *testing.T) {
	for _, full := range []lifecycle.FullPolicy{lifecycle.Reject, lifecycle.DropOldest, lifecycle.Block} {
		t.Run(full.String(), func(t *testing.T) {
			var droppedN, poppedN, accepted atomic.Int64
			q := newKeyed(t, lifecycle.KeyedLatestConfig[int, int]{Capacity: 4, Full: full,
				OnDrop: func(int, int) { droppedN.Add(1) }})
			deadline := time.Now().Add(200 * time.Millisecond)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			var wg, pw sync.WaitGroup
			for w := range 4 {
				wg.Go(func() {
					for i := 0; time.Now().Before(deadline); i++ {
						switch err := q.Set(ctx, (w*7+i)%9, i); {
						case err == nil:
							accepted.Add(1)
						case errors.Is(err, lifecycle.ErrFull):
						default:
							t.Error(err)
							return
						}
						runtime.Gosched()
					}
				})
			}
			for range 2 {
				pw.Go(func() {
					for {
						if _, _, err := q.Pop(ctx); err != nil {
							if !errors.Is(err, lifecycle.ErrQueueClosed) {
								t.Error(err)
							}
							return
						}
						poppedN.Add(1)
					}
				})
			}
			wg.Wait()
			mustCloseC(t, q)
			pw.Wait()
			drained := q.Drain(nil)
			if total := poppedN.Load() + droppedN.Load() + int64(drained); total != accepted.Load() {
				t.Fatalf("accepted %d, popped %d dropped %d drained %d", accepted.Load(), poppedN.Load(), droppedN.Load(), drained)
			}
		})
	}
}
