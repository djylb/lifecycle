# lifecycle

Structured concurrency and resource lifetimes for Go: a tree of scopes that
own goroutines, `io.Closer`s and child scopes, and a bounded queue whose
behaviour when full is chosen up front.

- Standard library only, tests included.
- Every goroutine started with `Go` is joined; panics become errors with
  the task name and stack.
- `Close` never blocks and runs no user code; `Wait` is bounded by a
  context.
- No background goroutines: the package starts only the task goroutines,
  at most one goroutine per scope while it closes its resources, and the
  `context.AfterFunc` callback of a root scope whose parent context ends.

```
go get github.com/djylb/lifecycle
```

Requires Go 1.26 or later.

## Scopes

```go
s := lifecycle.New(ctx, lifecycle.CancelOnError())

ln, err := net.Listen("tcp", addr)
if err != nil {
	return err
}
s.Add(ln) // closed when the scope closes

s.Go("accept", func(ctx context.Context) error {
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
		conn.Add(c)
		conn.Go("serve", func(ctx context.Context) error { return serve(ctx, c) })
	}
})

// Later, on shutdown:
s.Close(nil)
waitCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
defer cancel()
err = s.Wait(waitCtx) // errors.Is(err, lifecycle.ErrNotFinished) on timeout
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
- `.Expected(fn)` decides which errors are a normal exit once the scope is
  closing. The default, `IsExpectedExit`, accepts `context.Canceled`,
  `ErrClosed`, `ErrQueueClosed` and `net.ErrClosed`; an error matching the
  close cause is always expected. Expected errors are not reported; panics
  always are.

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
and no child is left.

Closing a scope:

- marks it and its whole subtree closed and cancels their contexts before
  `Close` returns; from then on `Go`, `Add` and `Child` return `ErrClosed`;
- closes resources in reverse order of `Add`, children's resources before
  the parent's; errors and panics of `Close` methods are joined into the
  result;
- does not wait for tasks: `Wait` does, and returns once every task of the
  subtree has returned and every resource is closed.

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
| `DropOldest` | removes the oldest item, passes it to `OnDrop`, adds the new |
| `Block`      | waits up to `BlockTimeout` and while the context lasts       |

`Close` stops new pushes and wakes every waiter; queued items remain
available to `Pop` until the queue is empty, then `Pop` returns
`ErrQueueClosed`. `Drain` removes the rest. `Stats` reports pushed,
dropped, rejected and waited counts. Push and Pop do not allocate, apart
from the timer of a `Block` push that has to wait with a `BlockTimeout`.

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
