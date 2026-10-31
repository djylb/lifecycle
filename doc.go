// Package lifecycle provides structured concurrency and resource lifetimes:
// a tree of scopes that own goroutines, io.Closers and child scopes, a
// bounded queue with an explicit policy for when it is full, and
// newest-value-wins mailboxes ([Latest], [KeyedLatest]).
//
// # Scopes
//
// A [Scope] is created with [New] or [Scope.Child] and an error [Policy]:
// [CancelOnError] closes the scope on the first task error, [Isolate] hands
// every task error to a handler and leaves the scope running. There is no
// default policy. A child's errors stay in the child unless its policy has
// [Policy.Propagate]; [Policy.Expected] decides which errors count as a
// normal exit while a scope closes.
//
// [Run] is the wait-for-all form: it waits until the tasks it started have
// returned, closes the scope and returns the first error.
//
// [Scope.Go] starts a named task. Every task is joined: a scope finishes
// only after all of its tasks have returned. A panic in a task is recovered
// and reported as a [*TaskError] wrapping a [*PanicError] with the stack.
//
// [Scope.Add] registers an io.Closer. Resources are closed when the scope
// closes, in reverse order of registration, after every child scope has
// closed its own resources. Errors and panics of Closers are joined into the
// scope's result.
//
// [Scope.Close] is idempotent and never blocks: it marks the scope and its
// whole subtree closed, cancels their contexts and, only if there is
// something to close, starts one goroutine per scope that closes the
// resources. [Scope.Wait] waits, bounded by a context, until the scope has
// finished: closed, every task returned, every child finished and every
// resource closed. Wait does not close the scope.
//
// # Guarantees
//
//   - Go, Add and Child are linearised with Close by the scope's mutex: a
//     task, resource or child is either registered before Close and then
//     joined or closed by it, or refused with [ErrClosed].
//   - When Close returns, the scope and all its descendants are closed and
//     their contexts are cancelled. A scope's context is cancelled only by
//     Close, so Done is closed exactly when Go starts to fail.
//   - Go on a closed scope does not run the function. Add on a closed scope
//     closes the resource synchronously in the caller and returns
//     [ErrClosed] joined with the Close error, so ownership handed to Add is
//     never leaked. Child on a closed scope returns ErrClosed.
//   - No lock is held while user code runs: task functions, Closers, error
//     handlers and queue drop callbacks run with no lock of this package
//     held. Close itself runs no user code.
//   - Error handlers run before the task or child that produced the error is
//     counted as done, so no handler call happens after Wait has returned.
//   - After a scope has started closing, a task error that matches the
//     close cause or that its policy expects ([IsExpectedExit] by default)
//     is a normal exit and is not reported. Panics are always reported.
//
// # Goroutines started by this package
//
//   - One per successful Go call; it ends when the task function returns or
//     panics and its error has been delivered.
//   - At most one per scope while it closes, started by Close only when the
//     scope holds resources or child scopes; it ends when the resources are
//     closed.
//   - For a root scope whose parent context is cancelled before the scope is
//     closed, the one goroutine of context.AfterFunc, which calls Close and
//     ends. Closing the scope first unregisters it.
//
// [Queue], [Latest] and [KeyedLatest] start no goroutines. They must be
// created with their constructors. On a zero value, the methods that return
// an error (Push, Pop, Set, Wait) return an error saying so; the others act
// as on an empty container (TryPop and TryGet find nothing, Drain removes
// nothing, Stats, Len and Cap report zero, Close does nothing).
package lifecycle
