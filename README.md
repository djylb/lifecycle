# lifecycle

Structured concurrency and resource lifetimes for Go: a tree of scopes that
own goroutines, `io.Closer`s and child scopes, a bounded queue whose
behaviour when full is chosen up front, and newest-value-wins mailboxes.

- Standard library only, tests included.
- Every goroutine started with `Go` is joined; panics become errors with
  the task name and stack.
- `Enter` joins work in a caller-managed goroutine, such as a callback.
- `Close` never blocks and runs no user code; `Wait` is bounded by a
  context.
- A root watches its cancellable parent until either context ends. Cleanup
  runs separately and continues if a resource exits its goroutine.

```
go get github.com/djylb/lifecycle
```

Requires Go 1.26 or later.

## Scopes

```go
ln, err := net.Listen("tcp", addr)
if err != nil {
	return err
}
s := lifecycle.New(ctx, lifecycle.CancelOnError())
if _, err := s.Add(ln); err != nil {
	s.Close(nil)
	return err // Add closed the refused listener
}

if err := s.Go("accept", func(ctx context.Context) error {
	for {
		c, err := ln.Accept()
		if err != nil {
			return err // reported unless the scope is already closing
		}
		conn, err := s.Child("conn", lifecycle.Isolate(logError))
		if err != nil {
			c.Close()
			return err
		}
		if _, err := conn.Add(c); err != nil {
			conn.Close(nil)
			return err
		}
		if err := conn.Go("serve", func(ctx context.Context) error { return serve(ctx, c) }); err != nil {
			conn.Close(nil)
			return err
		}
		conn.CloseWhenIdle()
	}
}); err != nil {
	s.Close(nil)
	return err
}

// Later, on shutdown:
s.Close(nil)
waitCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
defer cancel()
err = s.Wait(waitCtx)
```

Every scope is created with an error policy, and there is no default:

| Policy             | A task error...                                         |
|--------------------|---------------------------------------------------------|
| `CancelOnError()`  | closes the scope; `Wait` returns it                     |
| `Isolate(handler)` | goes to `handler`; the scope keeps running              |

Policies are refined with two options:

- `.Propagate()` on a child's policy hands the child's result, when it
  finishes with an error, to the parent's policy like a task error.
  Without it a child's errors stay in the child.
- `.Expected(fn)` decides which errors are a normal exit after the scope's
  context is cancelled. The default accepts `context.Canceled`, `ErrClosed`,
  `ErrQueueClosed` and `net.ErrClosed`. An error matching the cancellation
  cause is also expected. The default also accepts the scope context's own
  deadline exit. An explicit predicate replaces the default, including this
  deadline check. During a live drain, only errors matching
  `ErrClosed` are expected. A joined error is filtered only when every
  branch is expected. Panics from `Go` are always reported.

If an error handler or expected-exit predicate panics or calls
`runtime.Goexit`, the scope keeps the original report and the callback
failure together and starts closing. A registered Closer that calls
`runtime.Goexit` contributes `ErrGoexit` to the result. Cleanup continues
with the remaining resources.

For a batch of work, `Run` waits for every task, then closes the scope and
returns the first error:

```go
err := lifecycle.Run(ctx, func(s *lifecycle.Scope) error {
	for _, u := range urls {
		s.Go(u, func(ctx context.Context) error { return fetch(ctx, u) })
	}
	return nil
})
```

`CloseWhenIdle` is the same behaviour for any scope: it closes once no task
and no child is left. Until then, `Go`, `Child` and `Enter` still admit work.
Each child controls its own lifetime, so a batch can wait for children
that are still accepting work.

Use `Enter` for a callback running in a goroutine owned by its caller:

```go
func onEvent(s *lifecycle.Scope) {
	task, err := s.Enter()
	if err != nil {
		return // shutdown has stopped admissions
	}
	defer task.End()
	processEvent(s.Context())
}
```

`Enter` and `End` use the same task counts as `Go`. Registration and completion
themselves allocate no per-task memory. A `*Task` may escape to the heap when
handed off, and shard growth and shutdown can allocate. Pass a pointer to the
recipient and have it call `End`. The `go vet` copylocks check flags copies
of a `Task`. Repeating `End` on the same handle is safe, including from
multiple goroutines. The final `End` can cancel the scope's context and start
resource cleanup. The callback handles its own errors and panics. If a panic
leaves a task without a deferred `End` or an owner to end it, the scope
cannot finish.

`Drain` walks the subtree and stops `Go`, `Child` and `Enter` with `ErrClosed`
one scope at a time. Descendants can admit work until reached, and the entire
subtree is sealed when `Drain` returns. Each scope closes once its admitted
tasks and children finish. Admitted work keeps its context live and can still
use `Add`. `Draining()` signals each scope's admission cutoff during `Drain`
or `Close`. `Done()` signals context cancellation. `Stats().Draining` is
true during the graceful phase. The following shutdown waits up to a grace
period, then cancels outstanding work and waits a bounded time for cleanup.

```go
s.Drain()
graceCtx, cancelGrace := context.WithTimeout(context.Background(), gracePeriod)
defer cancelGrace()
finished, err := s.WaitResult(graceCtx)
if !finished {
	s.Close(context.DeadlineExceeded)
	cleanupCtx, cancelCleanup := context.WithTimeout(context.Background(), cleanupTimeout)
	defer cancelCleanup()
	err = s.Wait(cleanupCtx)
}
return err
```

Closing a scope:

- seals the subtree so `Go`, `Enter`, `Add` and `Child` return `ErrClosed`.
  `Close` cancels the contexts it starts closing before it returns. With
  concurrent `Close` calls, use `<-s.Done()` to observe cancellation of a
  particular scope already being closed by another call;
- closes resources in reverse order of `Add`, children's resources before
  the parent's; errors and panics of `Close` methods are joined into the
  result;
- does not wait for tasks: `Wait` does, and returns once every task of the
  subtree has ended and every resource is closed.

`Add` on a closed scope closes the resource at once and returns
`ErrClosed`, so a resource handed to a scope is never leaked. `Add` also
returns a function that takes the resource back without closing it.

## Queue

```go
q, err := lifecycle.NewQueue(lifecycle.QueueConfig[*Job]{
	Capacity: 256,
	Full:     lifecycle.DropOldest,
	OnDrop:   (*Job).Release,
})
```

| `Full`       | Push on a full queue                                         |
|--------------|--------------------------------------------------------------|
| `Reject`     | returns `ErrFull`                                            |
| `DropOldest` | commits the replacement and counters, then calls `OnDrop` with the oldest item |
| `Block`      | waits up to `BlockTimeout` and while the context lasts       |

`Close` stops new pushes and wakes every waiter; queued items remain
available to `Pop` until the queue is empty, then `Pop` returns
`ErrQueueClosed`. `Drain` removes the rest. `Stats` reports pushed,
dropped, rejected and waited counts. Push and Pop do not allocate, apart
from the timer of a `Block` push that has to wait with a `BlockTimeout`.

## Latest and KeyedLatest

For state where only the newest value matters, such as configuration or
health updates:

```go
cfg := lifecycle.NewLatest(lifecycle.LatestConfig[*Config]{})
cfg.Set(c) // never waits for space; replaces a value not taken yet

c, missed, err := cfg.Wait(ctx) // missed: values overwritten since the last Wait
```

`KeyedLatest` keeps the newest value per key for a bounded number of keys:

```go
h, err := lifecycle.NewKeyedLatest(lifecycle.KeyedLatestConfig[string, Health]{
	Capacity: 1024,
	Full:     lifecycle.Reject, // or DropOldest (evicts the oldest key) or Block
})
h.Set(ctx, "peer-1", Up)
peer, state, err := h.Pop(ctx)
```

Keys must compare equal to themselves. A non-reflexive key or a key
containing a dynamically incomparable value makes `Set` return
`ErrInvalidKey` before it changes the mailbox.

Keys come out in the order they became pending; a `Set` on a pending key
replaces its value in place without moving it and never hits the full
policy, which applies only to new keys. `OnDrop` receives every value that
is overwritten or evicted before it was taken (required with
`DropOldest`), outside any lock. `TakePending(fn)` consumes pending values,
passing them to `fn` when provided and returning how many it took. `Latest`
takes at most one value, and `KeyedLatest` takes keys in `TryPop` order until
the mailbox is empty. Both can take pending values after `Close`.
For all three containers the replacement and its counters are committed
before the synchronous `OnDrop` callback runs. If the callback panics or
exits its goroutine, the old value belongs to the callback and the new
value has been accepted.
`Close`, `Stats`, `ErrFull` and `ErrQueueClosed` behave as for `Queue`.
`Set`, `Wait` and `Pop` do not allocate, apart from the timer of a `Block`
set that has to wait with a `BlockTimeout`.

## lifecycletest

```go
func TestMain(m *testing.M) { lifecycletest.VerifyMain(m) } // goroutine leak check

func TestServer(t *testing.T) {
	err := lifecycletest.RunCycle(t, 5*time.Second, func(s *lifecycle.Scope) error {
		return startServer(s) // start → close → wait → empty → no leaks
	})
	if err != nil {
		t.Fatal(err)
	}
}
```

`CheckGoroutines(t)` checks a single test, `AssertEmpty(t, s)` a scope
tree, and `Leaks` exposes the stack-dump based check itself.

## License

Apache License 2.0
