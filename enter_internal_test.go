package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"testing/synctest"
)

func testTaskPaths(t *testing.T, test func(*testing.T, func() *Scope)) {
	t.Helper()
	t.Run("base", func(t *testing.T) {
		test(t, func() *Scope { return New(context.Background(), CancelOnError()) })
	})
	t.Run("sharded", func(t *testing.T) {
		test(t, func() *Scope { return shardedScope(t) })
	})
}

type drainCloser chan struct{}

func (c drainCloser) Close() error { close(c); return nil }

func TestEnterDrain(t *testing.T) {
	testTaskPaths(t, func(t *testing.T, newScope func() *Scope) {
		t.Helper()
		s := newScope()
		var zero Task
		zero.End()
		t.Cleanup(func() { s.Close(nil) })
		first, err := s.Enter()
		if err != nil {
			t.Fatal(err)
		}
		defer first.End()
		if (first.shard == nil) != (s.shards.Load() == nil) {
			t.Fatal("Enter registered on the wrong path")
		}

		child, err := s.Child("child", CancelOnError())
		if err != nil {
			t.Fatal(err)
		}
		s.CloseWhenIdle()
		held, err := s.Enter()
		if err != nil {
			t.Fatalf("Enter during CloseWhenIdle: %v", err)
		}
		defer held.End()
		grandchild, err := child.Child("grandchild", CancelOnError())
		if err != nil {
			t.Fatalf("CloseWhenIdle changed the child's lifetime: %v", err)
		}
		childTask, err := grandchild.Enter()
		if err != nil {
			t.Fatal(err)
		}
		defer childTask.End()
		tree := []*Scope{s, child, grandchild}
		var signals [3]<-chan struct{}
		for i, scope := range tree {
			signals[i] = scope.Draining()
			select {
			case <-signals[i]:
				t.Fatalf("%q stopped accepting work before Drain", scope.Name())
			default:
			}
		}
		run := make(chan struct{})
		if err = s.Go("task", func(ctx context.Context) error {
			select {
			case <-run:
				return ErrClosed // expected while draining
			case <-ctx.Done():
				return context.Cause(ctx)
			}
		}); err != nil {
			t.Fatal(err)
		}
		s.Drain()
		s.Drain()
		s.CloseWhenIdle()
		if st := s.Stats(); st.Tasks != 4 || st.Scopes != 2 || !st.Draining || st.Closed || st.Finished {
			t.Fatalf("stats while draining: %+v", st)
		}
		for i, scope := range tree {
			assertLiveDrain(t, scope, signals[i])
		}
		resource := make(drainCloser)
		if _, err = s.Add(resource); err != nil {
			t.Fatalf("Add during Drain: %v", err)
		}

		var endings sync.WaitGroup
		for range 4 {
			endings.Go(first.End)
		}
		endings.Wait()
		if st := s.Stats(); st.Tasks != 3 || st.Closed || st.Finished {
			t.Fatalf("stats after concurrent End: %+v", st)
		}
		held.End()
		handoff := make(chan *Task)
		endings.Go(func() { (<-handoff).End() })
		handoff <- &childTask
		endings.Wait()
		if err = waitScope(t, child); err != nil {
			t.Fatal(err)
		}
		if s.Context().Err() != nil {
			t.Fatal("scope closed while its Go task was still running")
		}
		close(run)
		if err := waitScope(t, s); err != nil {
			t.Fatal(err)
		}
		if cause := context.Cause(s.Context()); !errors.Is(cause, ErrClosed) {
			t.Fatalf("graceful close cause: %v", cause)
		}
		select {
		case <-resource:
		default:
			t.Fatal("resource still open after Finished")
		}
	})
}

func assertLiveDrain(t *testing.T, s *Scope, signal <-chan struct{}) {
	t.Helper()
	assertDrainingSignal(t, s, signal)
	if s.Context().Err() != nil || s.Cause() != nil || !s.Stats().Draining {
		t.Fatalf("%q did not enter a live drain", s.Name())
	}
	if _, err := s.Enter(); !errors.Is(err, ErrClosed) {
		t.Fatalf("Enter during Drain: %v", err)
	}
	if err := s.Go("late", func(context.Context) error { return nil }); !errors.Is(err, ErrClosed) {
		t.Fatalf("Go during Drain: %v", err)
	}
	if _, err := s.Child("late", CancelOnError()); !errors.Is(err, ErrClosed) {
		t.Fatalf("Child during Drain: %v", err)
	}
}

func assertDrainingSignal(t *testing.T, s *Scope, signal <-chan struct{}) {
	t.Helper()
	if s.Draining() != signal {
		t.Fatal("Draining changed its signal channel")
	}
	select {
	case <-signal:
	default:
		t.Fatalf("%q still has an open Draining signal", s.Name())
	}
}

func TestDrainExpectedErrors(t *testing.T) {
	errs := []error{fmt.Errorf("new task: %w", ErrClosed), context.Canceled, net.ErrClosed, ErrQueueClosed}
	reported := make(chan error, len(errs))
	s := New(context.Background(), Isolate(func(err error) { reported <- err }).Expected(func(error) bool { return true }))
	t.Cleanup(func() { s.Close(nil) })
	signal := s.Draining()
	for _, result := range errs {
		if err := s.Go("task", func(context.Context) error {
			<-signal
			return result
		}); err != nil {
			t.Fatal(err)
		}
	}
	s.Drain()
	if err := waitScope(t, s); err != nil {
		t.Fatal(err)
	}
	close(reported)
	seen := make(map[error]bool)
	for err := range reported {
		var taskErr *TaskError
		if !errors.As(err, &taskErr) {
			t.Fatalf("expected task error, got %v", err)
		}
		seen[taskErr.Err] = true
	}
	if len(seen) != 3 || !seen[context.Canceled] || !seen[net.ErrClosed] || !seen[ErrQueueClosed] {
		t.Fatalf("errors reported during Drain: %v", seen)
	}
}

func TestDrainDuringCloseCascade(t *testing.T) {
	root := New(context.Background(), CancelOnError())
	child, err := root.Child("child", CancelOnError())
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := child.Child("leaf", CancelOnError())
	if err != nil {
		t.Fatal(err)
	}
	task, err := leaf.Enter()
	if err != nil {
		t.Fatal(err)
	}
	defer task.End()
	signal := leaf.Draining()
	paused := make(chan struct{})
	resume := make(chan struct{})
	child.stop = func() bool {
		close(paused)
		<-resume
		return true
	}
	cause := errors.New("forced close")
	var workers sync.WaitGroup
	workers.Go(func() { root.Close(cause) })
	<-paused // Close has reached the child, with the leaf still open.
	root.Drain()
	select {
	case <-signal:
	default:
		t.Error("Drain returned before sealing the leaf")
	}
	if !errors.Is(leaf.Cause(), cause) {
		t.Errorf("leaf cause during concurrent Close: %v", leaf.Cause())
	}
	if root.Close(cause) {
		t.Error("concurrent Close claimed an already-closing subtree")
	}
	select {
	case <-child.Done():
		t.Error("concurrent Close cancelled the paused child's context")
	default:
	}
	close(resume)
	workers.Wait()
	assertDrainingSignal(t, leaf, signal)
	task.End()
	if err = waitScope(t, root); err != nil {
		t.Fatal(err)
	}
}

func TestEnterDuringShardGrowthAndDrain(t *testing.T) {
	testTaskPaths(t, func(t *testing.T, newScope func() *Scope) {
		t.Helper()
		s := newScope()
		hold, err := s.Enter()
		if err != nil {
			t.Fatal(err)
		}
		defer hold.End()
		signal := s.Draining()
		picked := make(chan struct{})
		var workers sync.WaitGroup
		workers.Go(func() {
			// Pause Enter between choosing a slot and taking its count lock.
			sd := s.pickShard()
			close(picked)
			<-signal
			if registered, ok := s.registerTask(sd); ok {
				task := Task{scope: s, shard: registered}
				task.End()
				t.Error("registration through a pre-growth slot passed the cutoff")
			}
		})
		<-picked
		s.mu.Lock()
		workers.Go(s.Drain)
		var count int
		if sh := s.shards.Load(); sh != nil {
			count = len(*sh)
		}
		s.growLocked(count)
		s.mu.Unlock()
		workers.Wait()
		if sh := s.shards.Load(); sh == nil || len(*sh) <= count {
			t.Fatal("shards did not grow before Drain")
		}
		if st := s.Stats(); st.Tasks != 1 || !st.Draining {
			t.Fatalf("registration before growth was not retained: %+v", st)
		}
		hold.End()
		if err = waitScope(t, s); err != nil {
			t.Fatal(err)
		}
	})
}

func TestIdleSealRechecksShards(t *testing.T) {
	s := shardedScope(t)
	if !s.shardsMayBeIdle() {
		t.Fatal("empty shards reported busy")
	}
	task, err := s.Enter()
	if err != nil {
		t.Fatal(err)
	}
	defer task.End()
	s.CloseWhenIdle()
	s.mu.Lock()
	// An unlocked scan from before Enter must not let the seal skip rechecking.
	sealed := s.sealIdleShardsLocked()
	s.mu.Unlock()
	if sealed {
		t.Fatal("sealed scope from a stale idle hint")
	}
	task.End()
	if err = waitScope(t, s); err != nil {
		t.Fatal(err)
	}
}

func TestDrainingConcurrentLastShardEnds(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := shardedScope(t)
		shards := *s.shards.Load()
		var tasks [2]Task
		for i := range tasks {
			sd, ok := s.registerTask(shards[i])
			if !ok || sd != shards[i] {
				t.Fatal("could not register one task on each shard")
			}
			tasks[i] = Task{scope: s, shard: sd}
		}
		s.Drain()
		published := make(chan *shard)
		resumeFirst, resumeSecond := make(chan struct{}), make(chan struct{})
		hook := func(scope *Scope, sd *shard) {
			if scope != s {
				return
			}
			published <- sd
			if sd == shards[0] {
				<-resumeFirst
			} else {
				<-resumeSecond
			}
		}
		previous := testBeforeShardIdleCheck.Swap(&hook)
		defer testBeforeShardIdleCheck.Store(previous)

		var workers sync.WaitGroup
		workers.Go(tasks[0].End)
		first := <-published
		secondEnded := make(chan struct{})
		workers.Go(func() {
			tasks[1].End()
			close(secondEnded)
		})
		second := <-published
		if first != shards[0] || second != shards[1] || first.busy.Load() || second.busy.Load() {
			t.Error("both shards must publish empty before probing")
		}

		// Keep the first End parked. The second probe must see both empty
		// shards and finish the scope without any further call to settle.
		close(resumeSecond)
		<-secondEnded
		select {
		case <-s.Finished():
		default:
			t.Error("scope did not finish after the second shard became empty")
		}
		close(resumeFirst)
		workers.Wait()
	})
}

func TestFinishedFollowsCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := New(context.Background(), CancelOnError())
		task, err := s.Enter()
		if err != nil {
			t.Fatal(err)
		}
		signal := s.Draining()
		beforeCancel := make(chan struct{})
		resumeClose := make(chan struct{})
		s.stop = func() bool {
			close(beforeCancel)
			<-resumeClose
			return true
		}
		var workers sync.WaitGroup
		workers.Go(func() { s.Close(nil) })
		<-beforeCancel
		workers.Go(task.End)
		synctest.Wait() // End is either waiting for Done or has already published Finished.
		select {
		case <-s.Finished():
			t.Error("Finished closed before cancellation")
		default:
		}
		if st := s.Stats(); st.Tasks != 0 || !st.Closed || st.Draining || st.Finished {
			t.Errorf("stats before cancellation: %+v", st)
		}
		close(resumeClose)
		workers.Wait()
		assertDrainingSignal(t, s, signal)
		if err = waitScope(t, s); err != nil {
			t.Fatal(err)
		}
		select {
		case <-s.Done():
		default:
			t.Fatal("context still live after Finished")
		}
	})
}

func TestEnterShutdownRace(t *testing.T) {
	for _, stop := range []struct {
		name   string
		drain  bool
		cancel bool
	}{
		{name: "Drain", drain: true},
		{name: "Close", cancel: true},
		{name: "Both", drain: true, cancel: true},
	} {
		t.Run(stop.name, func(t *testing.T) {
			testTaskPaths(t, func(t *testing.T, newScope func() *Scope) {
				t.Helper()
				for range 32 {
					s := newScope()
					hold, err := s.Enter()
					if err != nil {
						t.Fatal(err)
					}
					cause := errors.New("grace period elapsed")
					var tasks [8]Task
					var workers sync.WaitGroup
					start := make(chan struct{})
					for i := range tasks {
						workers.Go(func() {
							<-start
							var err error
							tasks[i], err = s.Enter()
							if err != nil && !errors.Is(err, ErrClosed) {
								t.Errorf("Enter racing with shutdown: %v", err)
							}
							if i%2 == 0 {
								tasks[i].End()
							}
						})
					}
					if stop.drain {
						workers.Go(func() { <-start; s.Drain() })
					}
					if stop.cancel {
						workers.Go(func() { <-start; s.Close(cause) })
					}
					close(start)
					workers.Wait()
					if _, err := s.Enter(); !errors.Is(err, ErrClosed) {
						t.Errorf("Enter after admission stopped: %v", err)
					}
					if !stop.cancel {
						if s.Context().Err() != nil || !s.Close(cause) {
							t.Error("Close did not interrupt Drain")
						}
					}
					if !errors.Is(context.Cause(s.Context()), cause) {
						t.Errorf("cause: %v", context.Cause(s.Context()))
					}
					select {
					case <-s.Finished():
						t.Error("Finished before the remaining tasks ended")
					default:
					}
					for i := range tasks {
						tasks[i].End()
					}
					hold.End()
					if err := waitScope(t, s); err != nil {
						t.Fatal(err)
					}
					if st := s.Stats(); st.Tasks != 0 || !st.Finished {
						t.Errorf("final stats: %+v", st)
					}
				}
			})
		})
	}
}

func TestEnterAllocs(t *testing.T) {
	testTaskPaths(t, func(t *testing.T, newScope func() *Scope) {
		t.Helper()
		s := newScope()
		if n := testing.AllocsPerRun(1000, func() {
			task, err := s.Enter()
			if err != nil {
				t.Fatal(err)
			}
			task.End()
		}); n != 0 {
			t.Errorf("Enter and End allocated %v times", n)
		}
		s.Drain()
		select {
		case <-s.Finished():
		default:
			t.Fatal("Drain did not close an idle scope")
		}
	})
}
