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
// [Scope.Go] starts a named task. A panic in it is recovered and reported
// as a [*TaskError] wrapping a [*PanicError] with the stack. [Scope.Enter]
// registers work in a goroutine managed by the caller, which handles its
// own errors and panics. Both forms are joined: a scope waits for every Go
// function to return and every Enter handle to be ended with [Task.End].
// Completion can be handed off to another goroutine by passing it a *Task.
//
// [Scope.Add] registers an io.Closer. Resources are closed when the scope
// closes, in reverse order of registration, after every child scope has
// closed its own resources. Errors and panics of Closers are joined into the
// scope's result.
//
// [Scope.Close] is idempotent. It seals the subtree, starts its context
// cancellation and starts resource cleanup where needed. [Scope.Wait]
// waits, bounded by a context, until cancellation, tasks, children and
// resource cleanup are complete. Wait does not close the scope.
//
// [Scope.Drain] seals task and child admissions one scope at a time. The
// cutoff is local to each scope, and the whole subtree is sealed on return.
// Each scope closes once its admitted work and children finish. Existing
// work keeps its context live and can still register resources. [Scope.Close]
// can interrupt the drain at any time. [Scope.CloseWhenIdle] continues to
// admit work until its own scope is idle so a batch can wait for children
// that manage their own lifetimes.
//
// # Guarantees
//
//   - Go, Enter, Add and Child are linearised with Close. A task, resource
//     or child is either accepted and joined or closed, or refused with
//     [ErrClosed]. Go, Enter and Child share the same admission cutoff
//     during Drain. A busy scope spreads its tasks and children over lock
//     shards that it grows on demand.
//   - Draining signals the local admission cutoff. Close cancels the contexts
//     it starts closing before it returns. With concurrent Close calls, a
//     scope already being closed can still have a live context. Its Done
//     channel signals cancellation. Finished signals that cancellation,
//     joined work and cleanup are complete.
//   - Go on a closed scope does not run the function. Add on a closed scope
//     closes the resource synchronously in the caller and returns
//     [ErrClosed] joined with the Close error, so ownership handed to Add is
//     never leaked. Child on a closed scope returns ErrClosed.
//   - No lock is held while user code runs: task functions, Closers, error
//     handlers and queue drop callbacks run with no lock of this package
//     held. Close itself runs no user code.
//   - Error handlers run before the task or child that produced the error is
//     counted as done, so no handler call happens after Wait has returned.
//   - After admissions stop, only ErrClosed is an expected exit until the
//     scope's context is cancelled. Afterwards, the cancellation cause and
//     errors matched by Policy.Expected are expected exits. Panics from Go
//     are always reported.
//
// # Goroutines started by this package
//
//   - One per successful Go call; it ends when the task function returns or
//     panics and its error has been delivered.
//   - At most one per scope when it closes and holds resources or child
//     scopes; it ends when the resources are closed.
//   - A root with a live, cancellable parent starts a watcher. It exits
//     when either context ends, propagating parent cancellation to the scope.
//   - A child with a non-nil result and Policy.Propagate starts one goroutine
//     to deliver the error. It unlinks the child when the parent's policy returns
//     or exits. The parent's Wait includes this work.
//   - A context derived directly from Scope.Context can use context.AfterFunc
//     to run cancellation in a short-lived callback goroutine. Wrapping the
//     scope context with context.WithValue hides AfterFunc. Deriving a
//     cancellable context from that wrapper can start a standard-library
//     propagation goroutine that waits until either context is cancelled.
//   - If a Closer exits its goroutine with runtime.Goexit, a replacement
//     cleanup goroutine closes the remaining resources and publishes completion.
//
// [Queue], [Latest] and [KeyedLatest] start no goroutines. They must be
// created with their constructors. On a zero value, the methods that return
// an error (Push, Pop, Set, Wait) return an error saying so; the others act
// as on an empty container (TryPop and TryGet find nothing, Queue.Drain and
// the mailboxes' TakePending remove nothing, Stats, Len and Cap report zero,
// Close does nothing).
package lifecycle
