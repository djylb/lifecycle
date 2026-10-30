package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"runtime/debug"
	"sync"
	"time"
)

// Policy decides what a scope does with the errors of its tasks and of the
// child scopes that propagate to it. The zero Policy is invalid; start from
// [CancelOnError] or [Isolate] and refine it with [Policy.Expected] and
// [Policy.Propagate].
type Policy struct {
	isolate   bool
	propagate bool
	handler   func(error)
	expected  func(error) bool
}

// CancelOnError returns the policy that closes the scope with the first task
// error as cause. The scope's result holds that error and any other
// unexpected error reported while the scope closes.
func CancelOnError() Policy { return Policy{handler: discard} }

func discard(error) {}

// Isolate returns the policy that passes every task error to handler and
// keeps the scope running. Errors are not kept in the scope's result, so a
// long-lived scope does not accumulate them. handler is called from the
// task's goroutine, concurrently with other calls, and must not wait for
// the scope to finish. It must not be nil.
func Isolate(handler func(err error)) Policy {
	if handler == nil {
		return Policy{}
	}
	return Policy{isolate: true, handler: handler}
}

// Expected returns p with fn deciding which task errors are an expected
// exit once the scope has started closing; those are not reported. An error
// matching the close cause is always expected. The default is
// [IsExpectedExit]; a nil fn restores it.
func (p Policy) Expected(fn func(err error) bool) Policy {
	p.expected = fn
	return p
}

// Propagate returns p for a child scope whose non-nil result, when it
// finishes, is reported to its parent as a [*TaskError] named after the
// child, exactly like a task error of the parent. Without it a child's
// errors stay in the child. A root scope ignores it.
func (p Policy) Propagate() Policy {
	p.propagate = true
	return p
}

func (p Policy) valid() bool { return p.handler != nil }

// IsExpectedExit reports whether err is the usual way a task ends when its
// scope closes: it matches context.Canceled, [ErrClosed], [ErrQueueClosed]
// or net.ErrClosed. It is the default of [Policy.Expected].
func IsExpectedExit(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, ErrClosed) ||
		errors.Is(err, ErrQueueClosed) || errors.Is(err, net.ErrClosed)
}

// Stats describes a scope and its descendants at one moment. The counts are
// gathered one scope at a time, so they are not an atomic snapshot of a tree
// that is changing.
type Stats struct {
	Tasks     int  // running tasks
	Resources int  // registered resources not yet taken for closing
	Scopes    int  // descendant scopes that have not finished
	Closed    bool // this scope has started closing
	Finished  bool // this scope has finished
}

// Scope owns tasks, resources and child scopes. All methods are safe for
// concurrent use. A Scope must be closed, by Close, by a task error under
// CancelOnError, by its parent, by the cancellation of the context given to
// New, or once idle after CloseWhenIdle; it is never closed just because its
// tasks returned.
type Scope struct {
	name     string
	parent   *Scope
	policy   Policy
	ctx      context.Context
	cancel   context.CancelCauseFunc
	finished chan struct{}

	mu       sync.Mutex
	stop     func() bool // unregisters the parent context's AfterFunc
	closed   bool
	closing  bool // the closer goroutine is running
	idle     bool // close once no task and no child is left
	done     bool
	cause    error
	result   error
	first    error // first error of the result
	errs     []error
	tasks    int
	nres     int
	resHead  *resource
	resTail  *resource
	nkids    int
	kidsHead *Scope
	sibPrev  *Scope // guarded by parent.mu
	sibNext  *Scope // guarded by parent.mu
	closersC chan struct{}
}

type resource struct {
	c          io.Closer
	prev, next *resource
	removed    bool
}

// detachedCtx keeps the values and the deadline of the parent context but
// not its cancellation, which reaches the scope through Close.
type detachedCtx struct {
	context.Context
	parent context.Context
}

func (c detachedCtx) Deadline() (time.Time, bool) { return c.parent.Deadline() }

// New returns a root scope. Its context carries the values and the deadline
// of ctx, and the cancellation of ctx closes the scope with
// context.Cause(ctx) as cause. New panics if ctx is nil or policy is the
// zero Policy.
func New(ctx context.Context, policy Policy) *Scope {
	if ctx == nil {
		panic("lifecycle: nil Context")
	}
	s := newScope("", nil, policy, detachedCtx{context.WithoutCancel(ctx), ctx})
	if ctx.Err() != nil {
		s.Close(context.Cause(ctx))
		return s
	}
	if ctx.Done() != nil {
		stop := context.AfterFunc(ctx, func() { s.Close(context.Cause(ctx)) })
		s.mu.Lock()
		s.stop = stop
		closed := s.closed
		s.mu.Unlock()
		if closed {
			stop()
		}
	}
	return s
}

func newScope(name string, parent *Scope, policy Policy, base context.Context) *Scope {
	if !policy.valid() {
		panic("lifecycle: invalid Policy")
	}
	s := &Scope{name: name, parent: parent, policy: policy, finished: make(chan struct{})}
	s.ctx, s.cancel = context.WithCancelCause(base)
	return s
}

// Child returns a new scope under s with its own policy. The child's
// context derives from s's, closing s closes the child, and s finishes only
// after the child. The child's errors stay in the child unless its policy
// has [Policy.Propagate]. Child panics if policy is the zero Policy and
// returns ErrClosed if s is closed.
func (s *Scope) Child(name string, policy Policy) (*Scope, error) {
	k := newScope(name, s, policy, s.ctx)
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		k.cancel(ErrClosed)
		return nil, ErrClosed
	}
	k.sibNext = s.kidsHead
	if s.kidsHead != nil {
		s.kidsHead.sibPrev = k
	}
	s.kidsHead = k
	s.nkids++
	s.mu.Unlock()
	return k, nil
}

// Name returns the name given to Child, or "" for a root scope.
func (s *Scope) Name() string { return s.name }

// Context returns the scope's context, cancelled when the scope closes with
// the close cause.
func (s *Scope) Context() context.Context { return s.ctx }

// Done returns a channel closed when the scope starts closing.
func (s *Scope) Done() <-chan struct{} { return s.ctx.Done() }

// Cause returns the cause the scope was closed with, or nil while it is
// open.
func (s *Scope) Cause() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cause
}

// Err returns the scope's result once it has finished, and nil before. The
// result joins the errors kept by the policy, the errors and panics of
// Closers, and nothing else: a cause given to Close is not an error of the
// scope.
func (s *Scope) Err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.result
}

// Wait waits until the scope has finished and returns its result. If ctx
// ends first, Wait returns an error wrapping [ErrNotFinished] and
// context.Cause(ctx). Wait does not close the scope. A task, Closer or error
// handler of the scope or of an ancestor must not wait for it without a
// deadline: the scope cannot finish before they return.
func (s *Scope) Wait(ctx context.Context) error {
	select {
	case <-s.finished:
		return s.Err()
	default:
	}
	select {
	case <-s.finished:
		return s.Err()
	case <-ctx.Done():
		return fmt.Errorf("%w: %w", ErrNotFinished, context.Cause(ctx))
	}
}

// Finished returns a channel closed when the scope has finished.
func (s *Scope) Finished() <-chan struct{} { return s.finished }

// Go starts fn in a new goroutine with the scope's context. name identifies
// the task in errors. fn's error and any panic are handled by the scope's
// policy. Go returns ErrClosed, without running fn, if the scope is closed.
func (s *Scope) Go(name string, fn func(ctx context.Context) error) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrClosed
	}
	s.tasks++
	s.mu.Unlock()
	go s.run(name, fn)
	return nil
}

func (s *Scope) run(name string, fn func(context.Context) error) {
	returned := false
	defer func() {
		if !returned { // runtime.Goexit
			s.report(&TaskError{Name: name, Err: ErrGoexit}, false)
		}
		s.mu.Lock()
		s.tasks--
		s.settle()
	}()
	err, panicked := call(s.ctx, fn)
	returned = true
	if err != nil {
		s.report(&TaskError{Name: name, Err: err}, !panicked)
	}
}

func call(ctx context.Context, fn func(context.Context) error) (err error, panicked bool) {
	defer func() {
		if v := recover(); v != nil {
			err, panicked = &PanicError{Value: v, Stack: debug.Stack()}, true
		}
	}()
	return fn(ctx), false
}

// report applies the policy to a task or child error. A filterable error
// that looks like a normal exit after the scope started closing is dropped.
func (s *Scope) report(te *TaskError, filterable bool) {
	// errors.Is may call user Is methods and Expected is user code: both
	// run with no lock held.
	s.mu.Lock()
	closed, cause := s.closed, s.cause
	s.mu.Unlock()
	if filterable && closed && (errors.Is(te.Err, cause) || s.isExpected(te.Err)) {
		return
	}
	if s.policy.isolate {
		s.policy.handler(te)
		return
	}
	s.mu.Lock()
	s.errs = append(s.errs, te)
	s.mu.Unlock()
	s.Close(te) // no-op if already closed
}

func (s *Scope) isExpected(err error) bool {
	if s.policy.expected != nil {
		return s.policy.expected(err)
	}
	return IsExpectedExit(err)
}

// Add registers c to be closed when the scope closes and returns a function
// that unregisters c without closing it. remove reports whether it
// unregistered c; it returns false once c was removed or taken for closing.
//
// If the scope is closed, Add closes c in the calling goroutine and returns
// ErrClosed joined with the error, or recovered panic, of c.Close.
func (s *Scope) Add(c io.Closer) (remove func() bool, err error) {
	if c == nil {
		return func() bool { return false }, errNilCloser
	}
	r := &resource{c: c}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return func() bool { return false }, errors.Join(ErrClosed, closeOne(c))
	}
	r.prev = s.resTail
	if s.resTail != nil {
		s.resTail.next = r
	} else {
		s.resHead = r
	}
	s.resTail = r
	s.nres++
	s.mu.Unlock()
	return func() bool { return s.remove(r) }, nil
}

func (s *Scope) remove(r *resource) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || r.removed {
		return false
	}
	r.removed = true
	if r.prev != nil {
		r.prev.next = r.next
	} else {
		s.resHead = r.next
	}
	if r.next != nil {
		r.next.prev = r.prev
	} else {
		s.resTail = r.prev
	}
	r.prev, r.next, r.c = nil, nil, nil
	s.nres--
	return true
}

func closeOne(c io.Closer) (err error) {
	defer func() {
		if v := recover(); v != nil {
			err = &PanicError{Value: v, Stack: debug.Stack()}
		}
	}()
	return c.Close()
}

// Close starts closing the scope and its descendants with cause, or with
// ErrClosed if cause is nil, and reports whether this call started it. It
// does not block and runs no user code: when it returns, the whole subtree
// is closed and its contexts are cancelled, and the resources are being
// closed by at most one goroutine per scope. Use Wait to wait for the end.
func (s *Scope) Close(cause error) bool {
	s.mu.Lock()
	return s.closeLocked(cause)
}

// closeLocked is Close with s.mu held; it unlocks.
func (s *Scope) closeLocked(cause error) bool {
	if cause == nil {
		cause = ErrClosed
	}
	if s.closed {
		s.mu.Unlock()
		return false
	}
	s.closed, s.cause = true, cause
	var kids []*Scope
	if s.nkids > 0 {
		kids = make([]*Scope, 0, s.nkids)
		for k := s.kidsHead; k != nil; k = k.sibNext {
			kids = append(kids, k)
		}
	}
	last := s.resTail
	s.resHead, s.resTail, s.nres = nil, nil, 0
	needCloser := kids != nil || last != nil
	s.closing = needCloser
	stop := s.stop
	fin := s.finishableLocked()
	s.mu.Unlock()

	if stop != nil {
		stop()
	}
	for _, k := range kids {
		k.Close(cause)
	}
	s.cancel(cause)
	if needCloser {
		go s.closeResources(kids, last)
	}
	if fin {
		s.finish()
	}
	return true
}

// closeResources waits until the children have closed their resources,
// then closes the scope's own in reverse order of registration.
func (s *Scope) closeResources(kids []*Scope, last *resource) {
	for _, k := range kids {
		<-k.closersDone()
	}
	var errs []error
	for r := last; r != nil; r = r.prev {
		if err := closeOne(r.c); err != nil {
			errs = append(errs, err)
		}
	}
	s.mu.Lock()
	s.errs = append(s.errs, errs...)
	s.closing = false
	ch := s.closersC
	fin := s.finishableLocked()
	s.mu.Unlock()
	if ch != nil {
		close(ch)
	}
	if fin {
		s.finish()
	}
}

var closedChan = func() chan struct{} {
	c := make(chan struct{})
	close(c)
	return c
}()

// closersDone returns a channel closed once the closed scope has closed its
// resources.
func (s *Scope) closersDone() <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closing {
		return closedChan
	}
	if s.closersC == nil {
		s.closersC = make(chan struct{})
	}
	return s.closersC
}

// CloseWhenIdle closes the scope, with ErrClosed as cause, as soon as no
// task is running and no child scope is left, which may be at once. Until
// then Go and Child keep working; a task may start more tasks.
func (s *Scope) CloseWhenIdle() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.idle = true
	s.settle()
}

// settle runs with s.mu held after a task or child left, or after
// CloseWhenIdle, and unlocks: it closes an idle scope or finishes a
// drained one.
func (s *Scope) settle() {
	if s.idle && !s.closed && s.tasks == 0 && s.nkids == 0 {
		s.closeLocked(nil)
		return
	}
	fin := s.finishableLocked()
	s.mu.Unlock()
	if fin {
		s.finish()
	}
}

// finishableLocked reports, once, that the scope has just finished, and
// records its result.
func (s *Scope) finishableLocked() bool {
	if !s.closed || s.done || s.closing || s.tasks > 0 || s.nkids > 0 {
		return false
	}
	s.done = true
	s.result = errors.Join(s.errs...)
	if len(s.errs) > 0 {
		s.first = s.errs[0]
	}
	s.errs = nil
	return true
}

func (s *Scope) finish() {
	close(s.finished)
	if p := s.parent; p != nil {
		p.childFinished(s)
	}
}

func (s *Scope) childFinished(k *Scope) {
	if res := k.result; res != nil && k.policy.propagate { // immutable once done
		s.report(&TaskError{Name: k.name, Err: res}, false)
	}
	s.mu.Lock()
	if k.sibPrev != nil {
		k.sibPrev.sibNext = k.sibNext
	} else {
		s.kidsHead = k.sibNext
	}
	if k.sibNext != nil {
		k.sibNext.sibPrev = k.sibPrev
	}
	k.sibPrev, k.sibNext = nil, nil
	s.nkids--
	s.settle()
}

// Stats returns the counts of the scope and its descendants.
func (s *Scope) Stats() Stats {
	s.mu.Lock()
	st := Stats{Tasks: s.tasks, Resources: s.nres, Scopes: s.nkids, Closed: s.closed, Finished: s.done}
	var kids []*Scope
	for k := s.kidsHead; k != nil; k = k.sibNext {
		kids = append(kids, k)
	}
	s.mu.Unlock()
	for _, k := range kids {
		ks := k.Stats()
		st.Tasks += ks.Tasks
		st.Resources += ks.Resources
		st.Scopes += ks.Scopes
	}
	return st
}

// Run runs fn with a new CancelOnError scope, waits until every task and
// child scope fn started has finished, then closes the scope and waits for
// its resources. It returns the first error: of fn, of a task, of a
// propagating child or of a Closer, in the order they were reported. If
// there is none but the scope was closed early, by the end of ctx or by
// Close with a cause, it returns that cause. fn runs in the caller's
// goroutine; an error from it closes the scope like a task error named
// "run", and a panic closes the scope before it continues. Like any scope,
// the tasks must honour their context for Run to return after a failure.
func Run(ctx context.Context, fn func(s *Scope) error) error {
	s := New(ctx, CancelOnError())
	returned := false
	defer func() {
		if !returned {
			s.Close(nil)
		}
	}()
	err := fn(s)
	returned = true
	if err != nil {
		s.report(&TaskError{Name: "run", Err: err}, false)
	}
	s.CloseWhenIdle()
	<-s.finished
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.first == nil && !errors.Is(s.cause, ErrClosed) {
		return s.cause
	}
	return s.first
}
