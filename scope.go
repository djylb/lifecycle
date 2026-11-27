package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"reflect"
	"runtime/debug"
	"sync"
	"sync/atomic"
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
// long-lived scope does not accumulate them. Task reports call handler in
// the task's goroutine. Propagated child reports run in a delivery goroutine.
// Calls can overlap. handler must be non-nil and must not wait for the scope
// to finish.
func Isolate(handler func(err error)) Policy {
	if handler == nil {
		return Policy{}
	}
	return Policy{isolate: true, handler: handler}
}

// Expected returns p with fn deciding which task errors are expected exits
// after the scope's context is cancelled. An error matching the cancellation
// cause is also expected. From the admission cutoff until cancellation, only
// errors matching [ErrClosed] are expected. Joined errors are checked branch
// by branch. A nil fn accepts [IsExpectedExit] and the scope's own context
// error. A non-nil fn replaces both checks. In particular, explicitly passing
// IsExpectedExit reports context.DeadlineExceeded when the inherited deadline
// cause is a different error that it does not match.
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
// scope's context is cancelled: it matches context.Canceled, [ErrClosed],
// [ErrQueueClosed] or net.ErrClosed. See [Policy.Expected] for the additional
// context-dependent checks made by a scope.
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
	Draining  bool // this scope is draining before Close starts
	Closed    bool // this scope has started closing
	Finished  bool // this scope has finished
}

// Scope owns tasks, resources and child scopes. All methods are safe for
// concurrent use. Methods panic on an unconstructed Scope. Construct a Scope
// with New or Child and never copy it. A Scope must be closed, by Close, by a
// task error under CancelOnError, by its parent, by cancellation of the context given to
// New, or once idle after CloseWhenIdle or Drain; it is never closed just
// because its tasks ended.
type Scope struct {
	ctx      context.Context //nolint:containedctx // the scope is the context's owner; tasks and resources run under it
	parent   *Scope
	cancel   context.CancelCauseFunc
	finished chan struct{}
	// shards is nil until task or child registrations contend on mu; from
	// then on they register on a shard. See shard.
	shards       atomic.Pointer[[]*shard]
	slot         *shard // the parent's shard this scope is registered on, or nil
	name         string
	sourceDone   <-chan struct{}
	deadlineExit *atomic.Bool
	normalClose  bool
	cause        error // guarded by mu, like the fields below
	result       error
	first        error // first error of the result
	resHead      *resource
	resTail      *resource
	kidsHead     *Scope
	sibPrev      *Scope // guarded by parent.mu, or by slot.mu when slot != nil
	sibNext      *Scope // guarded by parent.mu, or by slot.mu when slot != nil
	closersC     chan struct{}
	drainingC    chan struct{} // allocated on demand; guarded by mu
	policy       Policy
	errs         []error // guarded by mu

	mu               sync.Mutex
	tasks            int
	nres             int
	nkids            int
	pending          int           // tasks and children on shards when the scope closed, not yet ended
	state            atomic.Uint32 // written under mu, also read under shard locks
	misses           uint32        // collision window of mu, see lockCounted
	closingResources bool          // the closer goroutine is running or about to start
}

// Admission closes at scopeSealing, and resource registration closes at
// scopeIdleSealed or scopeClosing. scopeFinishing claims final publication
// exactly once.
const (
	scopeOpen          = iota
	scopeCloseWhenIdle // still accepting tasks and children
	scopeSealing       // sealed locally, propagating Drain to children
	scopeDraining      // subtree sealed, waiting until idle
	scopeIdleSealed    // idle cutoff committed; a cancelled root awaits its watcher
	scopeClosing       // sealed locally, propagating Close to children
	scopeClosed        // Close propagated and context cancelled
	scopeFinishing     // result fixed; Finished is published after cancellation
)

// A shard holds part of a busy scope's tasks and children under its own
// mutex. A scope starts with none, so a small scope pays one nil pointer;
// the first shards are made when registrations on the scope's mutex keep
// colliding, and their number doubles when a shard's own mutex keeps
// colliding. Shards are never moved or removed, so a count stays on the
// shard it was made on. Lock order: Scope.mu before shard.mu.
//
// Until Close, the shard's counts are its own. Close transfers them into
// Scope.pending under both mutexes. Tasks and children that end after the
// transfer also decrement pending under Scope.mu.
type shard struct {
	kidsHead    *Scope
	tasks       int
	nkids       int
	mu          sync.Mutex
	busy        atomic.Bool // hint for idle checks; derived from tasks and nkids under mu
	misses      uint32
	transferred bool
	_           [64]byte // keep busy shards on separate cache lines
}

// A scope, or one of its shards, asks for more shards once growAfter of
// the last growWindow registrations on its mutex collided, so the number
// of shards follows the contention actually seen and stops growing when
// collisions become rare.
const (
	growAfter  = 16
	growWindow = 256
)

// lockCounted locks mu and reports whether collisions, counted in the low
// 16 bits of *window and registrations in the high 16 bits, reached
// growAfter within growWindow registrations; it then starts a new window.
// *window is guarded by mu.
func lockCounted(mu *sync.Mutex, window *uint32) bool {
	missed := !mu.TryLock()
	if missed {
		if hook := testLockCollision.Load(); hook != nil {
			(*hook)(mu)
		}
		mu.Lock()
	}
	w := *window + 1<<16
	if missed {
		w++
	}
	switch {
	case w&0xffff >= growAfter:
		*window = 0
		return true
	case w>>16 >= growWindow:
		*window = 0
	default:
		*window = w
	}
	return false
}

// grow doubles the shards of s, unless another call already grew past
// have shards or s has stopped accepting tasks and children. It runs with
// s.mu held.
func (s *Scope) growLocked(have int) {
	if s.state.Load() >= scopeSealing {
		return
	}
	cur := s.shards.Load()
	if cur != nil && len(*cur) != have {
		return
	}
	next := make([]*shard, max(2, 2*have))
	if cur != nil {
		copy(next, *cur)
	}
	for i := have; i < len(next); i++ {
		next[i] = &shard{}
	}
	s.shards.Store(&next)
}

// pickShard returns a shard of s, or nil while s has none. The runtime's
// generator keeps no state shared between callers.
func (s *Scope) pickShard() *shard {
	if sh := s.shards.Load(); sh != nil {
		return pickOf(*sh)
	}
	return nil
}

// grownShard returns a shard of s, which must already have shards, as it
// does once lockBase has reported true.
func (s *Scope) grownShard() *shard { return pickOf(*s.shards.Load()) }

func pickOf(shards []*shard) *shard {
	return shards[rand.IntN(len(shards))] //nolint:gosec // load spreading, not security
}

// lockShard locks sd, growing the shards of s if sd keeps colliding.
func (s *Scope) lockShard(sd *shard) {
	have := len(*s.shards.Load())
	if lockCounted(&sd.mu, &sd.misses) {
		sd.mu.Unlock()
		s.mu.Lock()
		s.growLocked(have)
		s.mu.Unlock()
		sd.mu.Lock()
	}
}

// lockBase locks s.mu for a registration, making the first shards if it
// keeps colliding. It reports whether shards exist now, in which case the
// caller unlocks and registers on a shard instead.
func (s *Scope) lockBase() bool {
	if lockCounted(&s.mu, &s.misses) {
		s.growLocked(0)
	}
	return s.shards.Load() != nil
}

var testLockCollision atomic.Pointer[func(*sync.Mutex)] //nolint:gochecknoglobals // deterministic collision test barrier

var testBeforeClosePropagation atomic.Pointer[func(*Scope)] //nolint:gochecknoglobals // deterministic shutdown test barrier

var testBeforeShardIdleCheck atomic.Pointer[func(*Scope, *shard)] //nolint:gochecknoglobals // optional test barrier; avoids adding state to every scope or shard

// shardEnded finishes the end of a task or child on sd: sd.mu is held and
// the count already decremented; it unlocks.
func (s *Scope) shardEnded(sd *shard) {
	transferred := sd.transferred
	empty := sd.tasks == 0 && sd.nkids == 0
	if empty {
		// Publish before probing other shards. Once admissions are sealed,
		// the last publication is followed by a probe that can see all empty.
		sd.busy.Store(false)
	}
	sd.mu.Unlock()
	switch {
	case transferred:
		s.mu.Lock()
		s.pending--
		s.settle()
	case empty && settlesWhenIdle(s.state.Load()):
		if hook := testBeforeShardIdleCheck.Load(); hook != nil {
			(*hook)(s, sd)
		}
		if s.shardsMayBeIdle() {
			s.mu.Lock()
			s.settle()
		}
	}
}

// markBusy runs under sd.mu before incrementing the first task or child.
func (sd *shard) markBusy() {
	if sd.tasks == 0 && sd.nkids == 0 {
		sd.busy.Store(true)
	}
}

// shardsMayBeIdle avoids taking shard locks while work is visible. A true
// result must be rechecked under the locks before committing to close.
func (s *Scope) shardsMayBeIdle() bool {
	if sh := s.shards.Load(); sh != nil {
		for _, sd := range *sh {
			if sd.busy.Load() {
				return false
			}
		}
	}
	return true
}

// sealIdleShardsLocked stops admissions if all shards are empty, with s.mu
// held and the base counts already zero. It holds all shard locks until
// it seals the scope, so no new admission can slip in after the idle check.
func (s *Scope) sealIdleShardsLocked() bool {
	sh := s.shards.Load()
	if sh == nil {
		s.sealLocked(scopeIdleSealed)
		return true
	}
	empty := true
	for _, sd := range *sh {
		sd.mu.Lock()
		empty = empty && sd.tasks == 0 && sd.nkids == 0
	}
	if empty {
		s.sealLocked(scopeIdleSealed)
	}
	for _, sd := range *sh {
		sd.mu.Unlock()
	}
	return empty
}

type resource struct {
	c          io.Closer
	prev, next *resource
	removed    bool
}

// detachedCtx keeps the values and the deadline of the parent context but
// not its cancellation, which reaches the scope through Close.
type detachedCtx struct {
	context.Context                 //nolint:containedctx // wrapper type: it is a context that adjusts the deadline
	parent          context.Context //nolint:containedctx // only the deadline is read from it
}

func (c detachedCtx) Deadline() (time.Time, bool) { return c.parent.Deadline() }

// uncancelled is a child scope's base context: the values and the deadline
// of the parent scope's context without its cancellation, which reaches the
// child through Close. Unlike context.WithoutCancel it keeps the deadline.
type uncancelled struct {
	context.Context //nolint:containedctx // wrapper type: it is a context that hides the cancellation
}

func (uncancelled) Done() <-chan struct{} { return nil }
func (uncancelled) Err() error            { return nil }

// New returns a root scope. Its context carries the values and the deadline
// of ctx, and the cancellation of ctx closes the scope with
// context.Cause(ctx) as cause. New panics if ctx is nil or policy is the
// zero Policy.
func New(ctx context.Context, policy Policy) *Scope {
	if ctx == nil {
		panic("lifecycle: nil Context")
	}
	s := newScope(detachedCtx{context.WithoutCancel(ctx), ctx}, "", nil, policy) //nolint:contextcheck // the root scope owns its context; the cancellation of ctx reaches it through the root watcher
	s.sourceDone = ctx.Done()
	if ctx.Err() != nil {
		s.closeFromParent(ctx)
	} else if s.sourceDone != nil {
		go func() {
			select {
			case <-s.sourceDone:
				s.closeFromParent(ctx)
			case <-s.Done():
			}
		}()
	}

	return s
}

func (s *Scope) closeFromParent(ctx context.Context) {
	cause, deadline := context.Cause(ctx), errors.Is(ctx.Err(), context.DeadlineExceeded)
	s.mu.Lock()
	if s.state.Load() >= scopeClosing {
		s.mu.Unlock()
		return
	}
	s.closeLocked(cause, deadline)
}

type scopeContext struct {
	context.Context //nolint:containedctx // implements context by delegation, adjusting Err for inherited deadlines
	deadline        *atomic.Bool
	signal          context.Context //nolint:containedctx // carries notifications independently of the cause context
}

// Done exposes the distinct notification context's channel so derived
// contexts subscribe through AfterFunc and read this context's Err and
// Cause together.
func (c scopeContext) Done() <-chan struct{} { return c.signal.Done() }

func (c scopeContext) AfterFunc(fn func()) func() bool { return context.AfterFunc(c.signal, fn) }

func (c scopeContext) Err() error {
	err := c.signal.Err()
	if err != nil && c.deadline.Load() {
		return context.DeadlineExceeded
	}
	return err
}

func newScope(base context.Context, name string, parent *Scope, policy Policy) *Scope {
	if !policy.valid() {
		panic("lifecycle: invalid Policy")
	}
	s := &Scope{name: name, parent: parent, policy: policy, finished: make(chan struct{})}
	ctx, cancel := context.WithCancelCause(base)
	signal, notify := context.WithCancel(context.Background())
	s.deadlineExit = new(atomic.Bool)
	s.ctx = scopeContext{ctx, s.deadlineExit, signal}
	s.cancel = func(cause error) { cancel(cause); notify() }
	return s
}

// Child returns a new scope under s with its own policy. The child's
// context derives from s's, closing s closes the child, and s finishes only
// after the child. The child's errors stay in the child unless its policy
// has [Policy.Propagate]. Child panics if policy is the zero Policy and
// returns ErrClosed if s is draining or closed.
func (s *Scope) Child(name string, policy Policy) (*Scope, error) {
	s.check()
	// The child's context does not register with s's: Close closes the
	// children before cancelling s, so registration would only serialise
	// every Child on s's context.
	k := newScope(uncancelled{s.ctx}, name, s, policy)
	if sd := s.pickShard(); sd != nil {
		return s.childAdd(sd, k)
	}
	if s.lockBase() {
		s.mu.Unlock()
		return s.childAdd(s.grownShard(), k)
	}
	if s.state.Load() >= scopeSealing {
		s.mu.Unlock()
		k.cancel(ErrClosed)
		return nil, ErrClosed
	}
	k.link(&s.kidsHead)
	s.nkids++
	s.mu.Unlock()
	return k, nil
}

func (s *Scope) link(head **Scope) {
	s.sibNext = *head
	if *head != nil {
		(*head).sibPrev = s
	}
	*head = s
}

func (s *Scope) childAdd(sd *shard, k *Scope) (*Scope, error) {
	if err := s.childOn(sd, k); err != nil {
		return nil, err
	}
	return k, nil
}

func (s *Scope) childOn(sd *shard, k *Scope) error {
	s.lockShard(sd)
	if s.state.Load() >= scopeSealing {
		sd.mu.Unlock()
		k.cancel(ErrClosed)
		return ErrClosed
	}
	k.slot = sd
	k.link(&sd.kidsHead)
	sd.markBusy()
	sd.nkids++
	sd.mu.Unlock()
	return nil
}

func (s *Scope) check() {
	if s.finished == nil {
		panic("lifecycle: unconstructed Scope")
	}
}

// Name returns the name given to Child, or "" for a root scope.
func (s *Scope) Name() string { s.check(); return s.name }

// Context returns the scope's context, cancelled when the scope closes with
// the close cause.
func (s *Scope) Context() context.Context { s.check(); return s.ctx }

// Done returns a channel closed when the scope's context is cancelled.
func (s *Scope) Done() <-chan struct{} { s.check(); return s.ctx.Done() }

// Draining returns a channel closed when this scope stops accepting tasks
// and child scopes, by Drain or Close. A parent's shutdown reaches each
// descendant's signal when it seals that descendant's admissions.
func (s *Scope) Draining() <-chan struct{} {
	s.check()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.drainingC != nil {
		return s.drainingC
	}
	if s.state.Load() >= scopeSealing {
		return closedChan
	}
	s.drainingC = make(chan struct{})
	return s.drainingC
}

// Cause returns the chosen close cause, or nil until it is known. When an
// idle root observes its parent has already ended, the watcher supplies the
// parent's cause. Wait for Done to observe the published cancellation.
func (s *Scope) Cause() error {
	s.check()
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cause
}

// Err returns the scope's result once it has finished, and nil before. The
// result joins the errors kept by the policy, the errors and panics of
// Closers, and nothing else: a cause given to Close is not an error of the
// scope.
func (s *Scope) Err() error {
	s.check()
	select {
	case <-s.finished:
		return s.result
	default:
		return nil
	}
}

// Wait waits until the scope has finished and returns its result. If ctx
// ends first, Wait returns an error wrapping [ErrNotFinished] and
// context.Cause(ctx). Wait does not close the scope. A task, Closer or error
// handler must not wait indefinitely for a scope whose completion requires
// that callback to return. For example, a child task cannot wait for its root.
func (s *Scope) Wait(ctx context.Context) error { _, err := s.WaitResult(ctx); return err }

// WaitResult returns true and the scope result when it observes completion.
// Otherwise, it returns false and an error wrapping ErrNotFinished and the wait context cause.
func (s *Scope) WaitResult(ctx context.Context) (bool, error) {
	s.check()
	select {
	case <-s.finished:
		return true, s.Err()
	default:
	}
	select {
	case <-s.finished:
		return true, s.Err()
	case <-ctx.Done():
		return false, fmt.Errorf("%w: %w", ErrNotFinished, context.Cause(ctx))
	}
}

// Finished returns a channel closed when the scope has finished.
func (s *Scope) Finished() <-chan struct{} { s.check(); return s.finished }

// Go starts fn in a new goroutine with the scope's context. name identifies
// the task in errors. fn's error and any panic are handled by the scope's
// policy. Go returns ErrClosed, without running fn, if the scope is draining
// or closed.
func (s *Scope) Go(name string, fn func(ctx context.Context) error) error {
	s.check()
	sd, ok := s.registerTask(s.pickShard())
	if !ok {
		return ErrClosed
	}
	go s.run(sd, name, fn)
	return nil
}

// Task is the completion handle for work registered with [Scope.Enter].
// The zero value is safe to end. A Task returned by Enter must not be copied.
// The go vet copylocks check flags copies. Hand off completion by passing
// a pointer to the Task, which can cause it to escape to the heap.
type Task struct {
	_     [0]sync.Mutex // make go vet flag copies of a Task
	scope *Scope
	shard *shard
	ended bool // guarded by the mutex that protects the task's count
}

// Enter registers work in the caller's goroutine as a task. It starts no
// goroutine and returns ErrClosed if the scope is draining or closed. Call
// [Task.End] when the work is done, typically with defer, or hand off the
// handle by pointer to whoever will finish the work. The caller handles its
// own errors and panics, outside the scope's policy. If a panic leaves a task
// without a deferred End or an owner to end it, the scope cannot finish.
// Task registration and completion themselves allocate no per-task storage.
// Shard growth and scope shutdown can allocate.
func (s *Scope) Enter() (Task, error) {
	s.check()
	sd, ok := s.registerTask(s.pickShard())
	if !ok {
		return Task{}, ErrClosed
	}
	return Task{scope: s, shard: sd}, nil
}

// registerTask tries to count one task on the chosen shard or the base.
// It returns the count's shard (nil for the base) and whether it was accepted.
func (s *Scope) registerTask(sd *shard) (*shard, bool) {
	if sd == nil {
		if !s.lockBase() {
			if s.state.Load() >= scopeSealing {
				s.mu.Unlock()
				return nil, false
			}
			s.tasks++
			s.mu.Unlock()
			return nil, true
		}
		s.mu.Unlock()
		sd = s.grownShard()
	}
	s.lockShard(sd)
	if s.state.Load() >= scopeSealing {
		sd.mu.Unlock()
		return nil, false
	}
	sd.markBusy()
	sd.tasks++
	sd.mu.Unlock()
	return sd, true
}

// End marks the task complete. It is safe to call on the same handle more
// than once and from any goroutine, including concurrently. The final task
// may complete scope shutdown, including context cancellation and starting
// resource cleanup.
func (t *Task) End() {
	s, sd := t.scope, t.shard
	if s == nil {
		return
	}
	mu := &s.mu
	if sd != nil {
		mu = &sd.mu
	}
	mu.Lock()
	if t.ended {
		mu.Unlock()
		return
	}
	t.ended = true
	if sd != nil {
		sd.tasks--
		s.shardEnded(sd)
		return
	}
	s.tasks--
	s.settle()
}

func (s *Scope) run(sd *shard, name string, fn func(context.Context) error) {
	task := Task{scope: s, shard: sd}
	returned := false
	defer task.End()
	defer func() {
		if !returned { // runtime.Goexit
			s.report(&TaskError{Name: name, Err: ErrGoexit}, false)
		}
	}()
	panicked, err := call(func() error { return fn(s.ctx) })
	returned = true
	if err != nil {
		s.report(&TaskError{Name: name, Err: err}, !panicked)
	}
}

func call(fn func() error) (panicked bool, err error) {
	defer func() {
		if v := recover(); v != nil {
			panicked, err = true, panicError(v)
		}
	}()
	return false, fn()
}

// report applies the policy to a task or child error.
func (s *Scope) report(te *TaskError, filterable bool) {
	returned := false
	defer func() {
		if returned {
			return
		}
		err := ErrGoexit
		if v := recover(); v != nil {
			err = panicError(v)
		}
		s.recordFailure(errors.Join(te, err))
	}()
	if filterable && s.expectedExit(te.Err) {
		returned = true
		return
	}
	if s.policy.isolate {
		s.policy.handler(te)
		returned = true
		return
	}
	returned = true
	s.recordFailure(te)
}

func (s *Scope) recordFailure(err error) {
	s.mu.Lock()
	if s.state.Load() == scopeFinishing {
		s.mu.Unlock()
		return
	}
	s.errs = append(s.errs, err)
	if s.state.Load() < scopeIdleSealed {
		s.closeLocked(err, false)
		return
	}
	s.mu.Unlock()
}

// expectedExit uses the admission state and the actual cancellation signal.
// Matching errors and calling Expected may run user code, with no lock held.
func (s *Scope) expectedExit(err error) bool {
	if s.state.Load() < scopeSealing {
		return false
	}
	var cause error
	select {
	case <-s.Done():
		cause = context.Cause(s.ctx)
	default:
	}
	comparableCause := cause != nil && comparableValue(reflect.ValueOf(cause))
	return allExpected(err, cause, comparableCause, func(e error) bool {
		if cause == nil {
			return errors.Is(e, ErrClosed)
		}
		if s.policy.expected != nil {
			return s.policy.expected(e)
		}
		return errors.Is(e, s.ctx.Err()) || IsExpectedExit(e)
	})
}

func allExpected(err, cause error, comparableCause bool, match func(error) bool) bool {
	matched := false
	for e := err; e != nil; {
		// A comparable target value makes interface equality safe even when
		// another value of its type holds a slice in an interface field.
		if comparableCause && equalValues(e, cause) {
			return true
		}
		if is, ok := e.(interface{ Is(target error) bool }); ok && cause != nil && !matched {
			matched = is.Is(cause)
		}
		if w, ok := e.(interface{ Unwrap() []error }); ok {
			branches := w.Unwrap()
			if len(branches) == 0 {
				break
			}
			for _, branch := range branches {
				if !allExpected(branch, cause, comparableCause, match) {
					return false
				}
			}
			return true
		}
		w, ok := e.(interface{ Unwrap() error })
		if !ok {
			break
		}
		e = w.Unwrap()
	}
	return matched || match(err)
}

// Add registers c to be closed when the scope closes and returns a function
// that unregisters c without closing it. remove reports whether it
// unregistered c; it returns false once c was removed or taken for closing.
//
// If the scope is closed, Add closes c in the calling goroutine and returns
// ErrClosed joined with the error, or recovered panic, of c.Close.
func (s *Scope) Add(c io.Closer) (remove func() bool, err error) {
	s.check()
	if c == nil {
		return func() bool { return false }, errNilCloser
	}
	r := &resource{c: c}
	s.mu.Lock()
	if s.state.Load() >= scopeIdleSealed {
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
	if s.state.Load() >= scopeIdleSealed || r.removed {
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

func panicError(v any) *PanicError { return &PanicError{Value: v, Stack: debug.Stack()} }

func closeOne(c io.Closer) error {
	_, err := call(c.Close)
	return err
}

// Close starts closing the scope and its descendants with cause, or with
// ErrClosed if cause is nil, and reports whether this call started it. It
// does not wait for tasks and runs no user code. When it returns, the
// subtree is sealed and the contexts of the scopes it started closing have
// been cancelled. With concurrent Close calls, a scope already being
// closed by another call can still have a live context. Wait on that
// scope's [Scope.Done] channel to observe its cancellation. Use [Scope.Wait]
// to wait for all tasks, children and resource cleanup.
func (s *Scope) Close(cause error) bool { s.check(); return s.closeWith(cause, false) }
func (s *Scope) closeWith(cause error, deadline bool) bool {
	s.mu.Lock()
	switch s.state.Load() {
	case scopeIdleSealed, scopeClosed, scopeFinishing:
		s.mu.Unlock()
		return false
	case scopeClosing:
		s.continueCloseLocked()
		return false
	case scopeOpen, scopeCloseWhenIdle, scopeSealing, scopeDraining:
		// Close below.
	}
	s.closeLocked(cause, deadline)
	return true
}

// sealLocked advances the state with s.mu held and publishes the cutoff at
// its first transition. Shard admissions also observe it through state.
func (s *Scope) sealLocked(state uint32) {
	first := s.state.Load() < scopeSealing
	s.state.Store(state)
	if first && s.drainingC != nil {
		close(s.drainingC)
	}
}

// closeLocked commits the cause and cutoff with s.mu held. It unlocks
// before propagating shutdown.
func (s *Scope) closeLocked(cause error, deadline bool) {
	s.deadlineExit.Store(deadline)
	s.sealLocked(scopeClosing)
	if cause == nil {
		s.normalClose = true
		cause = ErrClosed
	}
	s.cause = cause
	kids := s.childrenLocked(true)
	last := s.resTail
	s.resHead, s.resTail, s.nres = nil, nil, 0
	needCloser := kids != nil || last != nil
	s.closingResources = needCloser
	s.mu.Unlock()

	if hook := testBeforeClosePropagation.Load(); hook != nil {
		(*hook)(s)
	}
	if len(kids) > 0 {
		s.closeChildren(kids, cause)
	}
	s.cancel(cause)
	s.mu.Lock()
	s.state.Store(scopeClosed)
	fin := s.finishableLocked()
	s.mu.Unlock()
	if needCloser {
		go s.closeResources(kids, last)
	}
	if fin {
		s.finish()
	}
}

// continueCloseLocked assists a concurrent Close with its chosen cause.
// It unlocks before visiting children.
func (s *Scope) continueCloseLocked() {
	kids, cause := s.childrenLocked(false), s.cause
	s.mu.Unlock()
	s.closeChildren(kids, cause)
}

// closeChildren propagates the chosen cause. Concurrent callers may help.
func (s *Scope) closeChildren(kids []*Scope, cause error) {
	for _, k := range kids {
		k.closeWith(cause, s.deadlineExit.Load())
	}
}

func appendChildren(kids []*Scope, head *Scope) []*Scope {
	for k := head; k != nil; k = k.sibNext {
		kids = append(kids, k)
	}
	return kids
}

// childrenLocked snapshots the children after the admission cutoff. A
// registration that passed the cutoff check holds its count lock until
// it is visible here. Close also transfers ownership of the shard counts.
func (s *Scope) childrenLocked(takeCounts bool) []*Scope {
	var kids []*Scope
	if s.nkids > 0 {
		kids = make([]*Scope, 0, s.nkids)
		kids = appendChildren(kids, s.kidsHead)
	}
	if sh := s.shards.Load(); sh != nil {
		for _, sd := range *sh {
			sd.mu.Lock()
			if takeCounts {
				sd.transferred = true
				s.pending += sd.tasks + sd.nkids
			}
			kids = appendChildren(kids, sd.kidsHead)
			sd.mu.Unlock()
		}
	}
	return kids
}

// closeResources waits until the children have closed their resources,
// then closes the scope's own in reverse order of registration.
func (s *Scope) closeResources(kids []*Scope, last *resource) {
	for _, k := range kids {
		<-k.closersDone()
	}
	s.closeRemaining(last, nil)
}

func (s *Scope) closeRemaining(last *resource, errs []error) {
	completed := false
	defer func() {
		if !completed {
			go s.closeRemaining(last, append(errs, ErrGoexit))
		}
	}()
	for last != nil {
		r := last
		last = r.prev
		c := r.c
		r.prev, r.next, r.c = nil, nil, nil
		if err := closeOne(c); err != nil {
			errs = append(errs, err)
		}
	}
	completed = true
	s.mu.Lock()
	s.errs = append(s.errs, errs...)
	s.closingResources = false
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

var closedChan = func() chan struct{} { //nolint:gochecknoglobals // read-only closed channel shared by every scope that needs one
	c := make(chan struct{})
	close(c)
	return c
}()

// closersDone returns a channel closed once the closed scope has closed its
// resources.
func (s *Scope) closersDone() <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closingResources {
		return closedChan
	}
	if s.closersC == nil {
		s.closersC = make(chan struct{})
	}
	return s.closersC
}

// CloseWhenIdle closes the scope, with ErrClosed as cause, as soon as no
// task is running and no child scope is left, which may be at once. Until
// then Go, Child and Enter keep working. Child scopes keep their own
// lifetimes so a batch can wait for children that are still accepting work.
// A root whose parent has already ended closes with the parent's cause.
// Use [Scope.Drain] to stop admissions throughout the subtree.
func (s *Scope) CloseWhenIdle() {
	s.check()
	s.mu.Lock()
	if s.state.Load() != scopeOpen {
		s.mu.Unlock()
		return
	}
	s.state.Store(scopeCloseWhenIdle)
	s.settle()
}

// Drain stops admissions on s and walks the subtree, sealing each scope
// in turn. A descendant may still accept work until Drain reaches it.
// The whole subtree has stopped admitting tasks and children when Drain
// returns. Each scope closes with ErrClosed as cause once its admitted
// tasks and children finish. A root whose parent has already ended uses
// the parent's cause.
// Drain returns without waiting for work to finish. In-flight work keeps
// its context live and can still register resources with Add while draining.
// Call Close at any time to cancel outstanding work and close descendants.
func (s *Scope) Drain() {
	s.check()
	s.mu.Lock()
	state := s.state.Load()
	switch state {
	case scopeDraining, scopeIdleSealed, scopeClosed, scopeFinishing:
		s.mu.Unlock()
		return
	case scopeClosing:
		s.continueCloseLocked()
		return
	case scopeOpen, scopeCloseWhenIdle, scopeSealing:
		// Drain below.
	}
	if state < scopeSealing {
		s.sealLocked(scopeSealing)
	}
	// Another Drain may be walking these children. Visit them as well so
	// every caller returns after the cutoff.
	kids := s.childrenLocked(false)
	if len(kids) > 0 {
		s.mu.Unlock()
		for _, k := range kids {
			k.Drain()
		}
		s.mu.Lock()
	}
	if s.state.Load() == scopeSealing {
		s.state.Store(scopeDraining)
		s.settle()
		return
	}
	s.mu.Unlock()
}

func settlesWhenIdle(state uint32) bool {
	return state == scopeCloseWhenIdle || state == scopeDraining
}

// settle runs with s.mu held after a task or child left, or after
// CloseWhenIdle or Drain, and unlocks. It starts a graceful close once
// idle, or finishes shutdown after a close.
func (s *Scope) settle() {
	if settlesWhenIdle(s.state.Load()) && s.tasks == 0 && s.nkids == 0 && s.shardsMayBeIdle() && s.sealIdleShardsLocked() {
		select {
		case <-s.sourceDone:
			// Leave the cause to the watcher. The last child's Close may
			// be on this stack, and the parent's Err or Value can block.
			s.mu.Unlock()
		default:
			s.closeLocked(nil, false)
		}
		return
	}
	fin := s.finishableLocked()
	s.mu.Unlock()
	if fin {
		s.finish()
	}
}

// finishableLocked claims completion once and records the result. The
// owner publishes it after cancellation in finish.
func (s *Scope) finishableLocked() bool {
	if s.state.Load() != scopeClosed || s.closingResources || s.tasks > 0 || s.nkids > 0 || s.pending > 0 {
		return false
	}
	s.state.Store(scopeFinishing)
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
		if s.result != nil && s.policy.propagate {
			// Completion can run on a descendant's Close stack. Deliver
			// errors separately because the parent's policy may block.
			go p.reportChild(s)
		} else {
			p.removeChild(s)
		}
	}
}

func (s *Scope) reportChild(k *Scope) {
	defer s.removeChild(k)
	s.report(&TaskError{Name: k.name, Err: k.result}, false)
}

func (s *Scope) removeChild(k *Scope) {
	if sd := k.slot; sd != nil {
		sd.mu.Lock()
		k.unlink(&sd.kidsHead)
		sd.nkids--
		s.shardEnded(sd)
		return
	}
	s.mu.Lock()
	k.unlink(&s.kidsHead)
	s.nkids--
	s.settle()
}

func (s *Scope) unlink(head **Scope) {
	if s.sibPrev != nil {
		s.sibPrev.sibNext = s.sibNext
	} else {
		*head = s.sibNext
	}
	if s.sibNext != nil {
		s.sibNext.sibPrev = s.sibPrev
	}
	s.sibPrev, s.sibNext = nil, nil
}

// Stats returns the counts of the scope and its descendants. It visits the
// whole subtree, locking one scope at a time, so its cost grows with the
// number of descendants: use it for diagnostics, not on a hot path. The
// totals are not kept incrementally because that would make every Go,
// Enter, Child and Add update every ancestor, contending on the root.
func (s *Scope) Stats() Stats {
	s.check()
	s.mu.Lock()
	state := s.state.Load()
	st := Stats{
		Tasks:     s.tasks,
		Resources: s.nres,
		Scopes:    s.nkids,
		Draining:  state >= scopeSealing && state < scopeIdleSealed,
		Closed:    state >= scopeIdleSealed,
	}
	select {
	case <-s.finished:
		st.Finished = true
	default:
	}
	var kids []*Scope
	kids = appendChildren(kids, s.kidsHead)
	if sh := s.shards.Load(); sh != nil {
		for _, sd := range *sh {
			sd.mu.Lock()
			st.Tasks += sd.tasks
			st.Scopes += sd.nkids
			kids = appendChildren(kids, sd.kidsHead)
			sd.mu.Unlock()
		}
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
	if s.first == nil && err != nil {
		return &TaskError{Name: "run", Err: err}
	}
	if s.first == nil && !s.normalClose {
		return s.cause
	}
	return s.first
}
