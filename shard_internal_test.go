package lifecycle

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func shardedScope(t *testing.T) *Scope {
	t.Helper()
	s := New(context.Background(), CancelOnError())
	s.mu.Lock()
	s.growLocked(0)
	s.growLocked(2)
	s.mu.Unlock()
	if n := len(*s.shards.Load()); n != 4 {
		t.Fatalf("shards %d", n)
	}
	return s
}

func waitScope(t *testing.T, s *Scope) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := s.Wait(ctx)
	if errors.Is(err, ErrNotFinished) {
		t.Fatalf("not finished: %+v", s.Stats())
	}
	return err
}

func TestShardsJoinTasksAndChildren(t *testing.T) {
	s := shardedScope(t)
	release := make(chan struct{})
	var ended sync.WaitGroup
	for range 20 {
		ended.Add(1)
		if err := s.Go("t", func(context.Context) error {
			defer ended.Done()
			<-release
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	kids := make([]*Scope, 0, 10)
	for range 10 {
		k, err := s.Child("k", CancelOnError())
		if err != nil {
			t.Fatal(err)
		}
		if k.slot == nil {
			t.Fatal("child not on a shard")
		}
		kids = append(kids, k)
	}
	if st := s.Stats(); st.Tasks != 20 || st.Scopes != 10 {
		t.Fatalf("stats %+v", st)
	}
	s.Close(nil)
	for _, k := range kids {
		select {
		case <-k.Done():
		default:
			t.Fatal("child not closed by Close")
		}
	}
	if err := s.Go("late", func(context.Context) error { return nil }); !errors.Is(err, ErrClosed) {
		t.Fatalf("Go after Close: %v", err)
	}
	if _, err := s.Child("late", CancelOnError()); !errors.Is(err, ErrClosed) {
		t.Fatalf("Child after Close: %v", err)
	}
	select {
	case <-s.Finished():
		t.Fatal("finished before its tasks")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if err := waitScope(t, s); err != nil {
		t.Fatal(err)
	}
	ended.Wait()
}

func TestShardsCloseWhenIdle(t *testing.T) {
	s := shardedScope(t)
	gate, second := make(chan struct{}), make(chan struct{})
	busy := func() {
		t.Helper()
		select {
		case <-s.Done():
			t.Fatal("closed while a task runs")
		case <-time.After(10 * time.Millisecond):
		}
	}
	if err := s.Go("first", func(context.Context) error {
		<-gate
		// started before first ends: idle must not be seen in between
		return s.Go("second", func(context.Context) error {
			<-second
			return nil
		})
	}); err != nil {
		t.Fatal(err)
	}
	s.CloseWhenIdle()
	busy()
	close(gate)
	busy()
	close(second)
	if err := waitScope(t, s); err != nil {
		t.Fatal(err)
	}
}

func collisions(t *testing.T, target *sync.Mutex) <-chan struct{} {
	t.Helper()
	ch := make(chan struct{}, 1)
	hook := func(mu *sync.Mutex) {
		if mu == target {
			select {
			case ch <- struct{}{}:
			default:
			}
		}
	}
	testLockCollision.Store(&hook)
	t.Cleanup(func() { testLockCollision.Store(nil) })
	return ch
}

func TestLockCountedGrows(t *testing.T) {
	var mu sync.Mutex
	var misses uint32
	collided := collisions(t, &mu)
	for i := 1; i <= growAfter; i++ {
		mu.Lock()
		got := make(chan bool)
		go func() {
			r := lockCounted(&mu, &misses)
			mu.Unlock()
			got <- r
		}()
		<-collided
		mu.Unlock()
		if r := <-got; r != (i == growAfter) {
			t.Fatalf("collision %d: grow %v", i, r)
		}
	}
	if !mu.TryLock() || misses != 0 {
		t.Fatalf("misses %d", misses)
	}
	mu.Unlock()
}

func TestContentionMakesShards(t *testing.T) {
	s := New(context.Background(), CancelOnError())
	collided := collisions(t, &s.mu)
	for range growAfter {
		s.mu.Lock()
		done := make(chan error)
		go func() { done <- s.Go("t", func(context.Context) error { return nil }) }()
		<-collided
		s.mu.Unlock()
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	if s.shards.Load() == nil {
		t.Fatal("no shards after repeated collisions")
	}
	s.Close(nil)
	if err := waitScope(t, s); err != nil {
		t.Fatal(err)
	}
}

func TestLockCountedWindow(t *testing.T) {
	var mu sync.Mutex
	var window uint32
	collided := collisions(t, &mu)
	collide := func() bool {
		mu.Lock()
		got := make(chan bool)
		go func() {
			r := lockCounted(&mu, &window)
			mu.Unlock()
			got <- r
		}()
		<-collided
		mu.Unlock()
		return <-got
	}
	for range growAfter - 1 {
		if collide() {
			t.Fatal("grew early")
		}
	}
	for range growWindow {
		if lockCounted(&mu, &window) {
			t.Fatal("grew without a collision")
		}
		mu.Unlock()
	}
	if collide() {
		t.Fatal("old collisions outlived their window")
	}
}

type ctxKey struct{}

func TestChildContext(t *testing.T) {
	dl := time.Now().Add(time.Hour)
	base, cancelBase := context.WithDeadline(context.WithValue(context.Background(), ctxKey{}, "v"), dl)
	defer cancelBase()
	s := New(base, CancelOnError())
	k, err := s.Child("k", CancelOnError())
	if err != nil {
		t.Fatal(err)
	}
	g, err := k.Child("g", CancelOnError())
	if err != nil {
		t.Fatal(err)
	}
	ctx := g.Context()
	if d, ok := ctx.Deadline(); !ok || !d.Equal(dl) {
		t.Fatalf("deadline %v %v", d, ok)
	}
	if ctx.Value(ctxKey{}) != "v" {
		t.Fatal("value lost")
	}
	cause := errors.New("stop")
	s.Close(cause)
	select {
	case <-ctx.Done():
	default:
		t.Fatal("grandchild context not cancelled by Close")
	}
	if !errors.Is(context.Cause(ctx), cause) {
		t.Fatalf("cause %v", context.Cause(ctx))
	}
	if err := waitScope(t, s); err != nil {
		t.Fatal(err)
	}
}

func TestGrowthCoalescesCollisions(t *testing.T) {
	s := New(t.Context(), CancelOnError())
	s.mu.Lock()
	s.growLocked(0)
	table := *s.shards.Load()
	s.mu.Unlock()
	hits := make(chan struct{}, len(table))
	hook := func(mu *sync.Mutex) {
		for _, sd := range table {
			if mu == &sd.mu {
				hits <- struct{}{}
				return
			}
		}
	}
	testLockCollision.Store(&hook)
	defer testLockCollision.Store(nil)
	var workers sync.WaitGroup
	for _, sd := range table {
		sd.mu.Lock()
		sd.misses = growAfter - 1
		workers.Go(func() { s.lockShard(sd); sd.mu.Unlock() })
	}
	for range table {
		<-hits
	}
	for _, sd := range table {
		sd.mu.Unlock()
	}
	workers.Wait()
	if got := len(*s.shards.Load()); got != 2*len(table) {
		t.Fatalf("grew from %d to %d", len(table), got)
	}
	s.Close(nil)
}
