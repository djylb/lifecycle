package lifecycle

import (
	"context"
	"errors"
	"sync"
	"time"
)

var (
	errZeroLatest      = errors.New("lifecycle: Latest not created by NewLatest")
	errZeroKeyedLatest = errors.New("lifecycle: KeyedLatest not created by NewKeyedLatest")
)

// signal wakes one waiter. It holds at most one token, so a send never
// blocks; a waiter that takes a token re-checks the state under the lock
// and, if something is left for others, passes the token on.
type signal chan struct{}

func (s signal) notify() {
	select {
	case s <- struct{}{}:
	default:
	}
}

// LatestConfig configures a [Latest]. Every field is optional.
type LatestConfig[T any] struct {
	// OnDrop receives each value that a Set overwrites before it was taken,
	// so that it can be released. It runs in the setting goroutine with no
	// lock held.
	OnDrop func(T)
}

// LatestStats are the counters of a [Latest].
type LatestStats struct {
	Pushed      uint64 // values accepted by Set, including those overwritten later
	Overwritten uint64 // values replaced by a later Set before they were taken
	Len         int    // 1 if a value is pending, else 0
}

// Latest is a single-slot mailbox where the newest value wins: Set never
// blocks and replaces a value that has not been taken yet. It is safe for
// concurrent use, starts no goroutines, and waiting calls park on channels
// with no lock held. It is meant for one consumer; with several, each
// value goes to one of them.
//
// Close stops new values and wakes every waiter. A value still pending stays
// available to Wait and TryGet, after which Wait returns ErrQueueClosed.
// Close implements io.Closer, so a Latest can be added to a [Scope].
type Latest[T any] struct {
	mu     sync.Mutex
	v      T
	has    bool
	closed bool
	missed uint64 // values overwritten since the last one was taken
	ready  signal
	done   chan struct{}
	onDrop func(T)

	pushed, overwritten uint64
}

// NewLatest returns an empty Latest configured by cfg.
func NewLatest[T any](cfg LatestConfig[T]) *Latest[T] {
	return &Latest[T]{
		ready:  make(signal, 1),
		done:   make(chan struct{}),
		onDrop: cfg.OnDrop,
	}
}

// Set stores v as the pending value, replacing and passing to OnDrop any
// value not taken yet. It never blocks. On a closed Latest it returns
// ErrQueueClosed and v stays with the caller.
func (l *Latest[T]) Set(v T) error {
	if l.ready == nil {
		return errZeroLatest
	}
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return ErrQueueClosed
	}
	old, had := l.v, l.has
	l.v, l.has = v, true
	l.pushed++
	if had {
		l.missed++
		l.overwritten++
	}
	l.mu.Unlock()
	l.ready.notify()
	if had && l.onDrop != nil {
		l.onDrop(old)
	}
	return nil
}

// take removes the pending value; l.mu must be held.
func (l *Latest[T]) take() (T, uint64) {
	var zero T
	v, missed := l.v, l.missed
	l.v, l.has, l.missed = zero, false, 0
	return v, missed
}

// TryGet takes the pending value if there is one. missed is the number of
// values overwritten since the previous value was taken.
func (l *Latest[T]) TryGet() (v T, missed uint64, ok bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.has {
		return v, 0, false
	}
	v, missed = l.take()
	return v, missed, true
}

// Wait takes the pending value, waiting while there is none and the Latest
// is open. missed is the number of values overwritten since the previous
// value was taken. It returns ErrQueueClosed once the Latest is closed and
// empty, or ctx.Err() if ctx ends first.
func (l *Latest[T]) Wait(ctx context.Context) (v T, missed uint64, err error) {
	if l.ready == nil {
		return v, 0, errZeroLatest
	}
	for {
		l.mu.Lock()
		if l.has {
			v, missed = l.take()
			l.mu.Unlock()
			return v, missed, nil
		}
		closed := l.closed
		l.mu.Unlock()
		if closed {
			return v, 0, ErrQueueClosed
		}
		select {
		case <-l.ready:
		case <-l.done:
		case <-ctx.Done():
			return v, 0, ctx.Err()
		}
	}
}

// Drain takes the pending value, if any, passes it to fn and returns how
// many values it took (0 or 1).
func (l *Latest[T]) Drain(fn func(T)) int {
	v, _, ok := l.TryGet()
	if !ok {
		return 0
	}
	if fn != nil {
		fn(v)
	}
	return 1
}

// Close closes the Latest; see [Latest]. It is idempotent, runs no user
// code and always returns nil.
func (l *Latest[T]) Close() error {
	if l.done == nil {
		return nil
	}
	l.mu.Lock()
	if !l.closed {
		l.closed = true
		close(l.done)
	}
	l.mu.Unlock()
	return nil
}

// Stats returns the counters.
func (l *Latest[T]) Stats() LatestStats {
	l.mu.Lock()
	defer l.mu.Unlock()
	st := LatestStats{Pushed: l.pushed, Overwritten: l.overwritten}
	if l.has {
		st.Len = 1
	}
	return st
}

// KeyedLatestConfig configures a [KeyedLatest]. Capacity and Full are
// required.
type KeyedLatestConfig[K comparable, T any] struct {
	// Capacity is the maximum number of distinct pending keys; it must be
	// positive.
	Capacity int
	// Full is the policy when a Set brings a new key and Capacity keys are
	// pending. A Set on a pending key always overwrites in place.
	Full FullPolicy
	// OnDrop receives each value removed before it was taken: overwritten
	// by a Set on the same key, or evicted with the oldest key under
	// DropOldest. It is required with DropOldest and optional otherwise. It
	// runs in the setting goroutine with no lock held.
	OnDrop func(K, T)
	// BlockTimeout bounds each waiting Set under Block. Zero means the wait
	// is bounded by Set's context alone. Not allowed with other policies.
	BlockTimeout time.Duration
}

// KeyedLatestStats are the counters of a [KeyedLatest].
type KeyedLatestStats struct {
	Pushed      uint64 // values accepted by Set, including those dropped later
	Overwritten uint64 // values replaced by a Set on the same pending key
	Evicted     uint64 // values removed with the oldest key by DropOldest
	Rejected    uint64 // Sets of a new key refused because it was full, including Block Sets that gave up
	Waited      uint64 // Block Sets that found it full and waited
	Len         int    // pending keys now
	Cap         int    // capacity
}

type keyedEntry[K comparable, T any] struct {
	key K
	v   T
}

// KeyedLatest keeps the newest value of each key, for at most Capacity
// distinct pending keys. Pop returns pending keys in the order in which they
// became pending; a Set on a pending key replaces its value without moving
// it. It is safe for concurrent use, starts no goroutines, and waiting calls
// park on channels with no lock held.
//
// Close stops new values and wakes every waiter. Pending keys stay
// available to Pop and TryPop, after which Pop returns ErrQueueClosed.
// Drain removes them instead. Close implements io.Closer, so a KeyedLatest
// can be added to a [Scope].
type KeyedLatest[K comparable, T any] struct {
	mu      sync.Mutex
	ring    []keyedEntry[K, T] // pending keys in FIFO order, from head
	head, n int
	index   map[K]int // key -> position in ring
	ready   signal    // a key is pending
	space   signal    // a slot is free, for Block
	done    chan struct{}
	onDrop  func(K, T)
	timeout time.Duration

	pushed, overwritten, evicted, rejected, waited uint64

	full   FullPolicy
	closed bool
}

// NewKeyedLatest returns an empty KeyedLatest configured by cfg.
func NewKeyedLatest[K comparable, T any](cfg KeyedLatestConfig[K, T]) (*KeyedLatest[K, T], error) {
	switch {
	case cfg.Capacity <= 0:
		return nil, errors.New("lifecycle: KeyedLatest capacity must be positive")
	case cfg.Full < Reject || cfg.Full > Block:
		return nil, errors.New("lifecycle: KeyedLatest full policy not set")
	case cfg.Full == DropOldest && cfg.OnDrop == nil:
		return nil, errors.New("lifecycle: DropOldest KeyedLatest needs OnDrop")
	case cfg.BlockTimeout < 0, cfg.Full != Block && cfg.BlockTimeout != 0:
		return nil, errors.New("lifecycle: BlockTimeout must be zero or positive and is only used by Block")
	}
	return &KeyedLatest[K, T]{
		ring:    make([]keyedEntry[K, T], cfg.Capacity),
		index:   make(map[K]int, cfg.Capacity),
		ready:   make(signal, 1),
		space:   make(signal, 1),
		done:    make(chan struct{}),
		full:    cfg.Full,
		onDrop:  cfg.OnDrop,
		timeout: cfg.BlockTimeout,
	}, nil
}

// Set stores v as the newest value of k. If k is pending, its value is
// replaced in place and the old one passed to OnDrop. Otherwise k is added
// at the end, applying the full policy if Capacity keys are pending: Reject
// returns ErrFull, DropOldest evicts the oldest pending key and passes its
// value to OnDrop, Block waits for a free slot, at most BlockTimeout and
// while ctx lasts. ctx is otherwise unused. On a closed KeyedLatest it
// returns ErrQueueClosed. On any error v stays with the caller.
func (q *KeyedLatest[K, T]) Set(ctx context.Context, k K, v T) error {
	if q.ring == nil {
		return errZeroKeyedLatest
	}
	if done, err := q.trySet(k, v, false); done {
		return err
	}
	return q.blockSet(ctx, k, v)
}

// blockSet waits for a free slot under Block and retries trySet.
func (q *KeyedLatest[K, T]) blockSet(ctx context.Context, k K, v T) error {
	q.mu.Lock()
	q.waited++
	q.mu.Unlock()
	var timeout <-chan time.Time
	if q.timeout > 0 {
		t := time.NewTimer(q.timeout)
		defer t.Stop()
		timeout = t.C
	}
	for {
		select {
		case <-q.space:
		case <-q.done:
		case <-ctx.Done():
			q.reject()
			return ctx.Err()
		case <-timeout:
			q.reject()
			return ErrFull
		}
		if done, err := q.trySet(k, v, true); done {
			return err
		}
	}
}

// trySet does Set without waiting. It reports done false when the policy
// is Block and no slot is free. waited tells that the caller took a space
// wake-up, which it passes on if a slot is still free.
func (q *KeyedLatest[K, T]) trySet(k K, v T, waited bool) (done bool, err error) {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return true, ErrQueueClosed
	}
	if i, ok := q.index[k]; ok {
		old := q.ring[i].v
		q.ring[i].v = v
		q.pushed++
		q.overwritten++
		more := q.n < len(q.ring)
		q.mu.Unlock()
		if waited && more {
			q.space.notify() // the wake-up this Set took is not needed
		}
		if q.onDrop != nil {
			q.onDrop(k, old)
		}
		return true, nil
	}
	if q.n < len(q.ring) {
		q.add(k, v)
		more := q.n < len(q.ring)
		q.mu.Unlock()
		q.ready.notify()
		if waited && more {
			q.space.notify() // pass the wake-up on to another blocked Set
		}
		return true, nil
	}
	switch q.full {
	case Reject:
		q.rejected++
		q.mu.Unlock()
		return true, ErrFull
	case DropOldest: // full, so no Set has waited
		old := q.removeHead()
		q.evicted++
		q.add(k, v)
		q.mu.Unlock()
		q.ready.notify()
		q.onDrop(old.key, old.v)
		return true, nil
	case Block:
		q.mu.Unlock()
		return false, nil
	default: // validated by NewKeyedLatest
		q.mu.Unlock()
		return true, errZeroKeyedLatest
	}
}

func (q *KeyedLatest[K, T]) reject() {
	q.mu.Lock()
	q.rejected++
	q.mu.Unlock()
}

// add appends k; q.mu must be held and a slot free.
func (q *KeyedLatest[K, T]) add(k K, v T) {
	i := (q.head + q.n) % len(q.ring)
	q.ring[i] = keyedEntry[K, T]{k, v}
	q.index[k] = i
	q.n++
	q.pushed++
}

// removeHead removes the oldest key; q.mu must be held and a key pending.
func (q *KeyedLatest[K, T]) removeHead() keyedEntry[K, T] {
	e := q.ring[q.head]
	q.ring[q.head] = keyedEntry[K, T]{}
	delete(q.index, e.key)
	q.head = (q.head + 1) % len(q.ring)
	q.n--
	return e
}

// pop removes the oldest key if there is one and passes the wake-ups on.
func (q *KeyedLatest[K, T]) pop() (keyedEntry[K, T], bool, bool) {
	q.mu.Lock()
	if q.n == 0 {
		closed := q.closed
		q.mu.Unlock()
		return keyedEntry[K, T]{}, false, closed
	}
	e := q.removeHead()
	more := q.n > 0
	q.mu.Unlock()
	q.space.notify()
	if more {
		q.ready.notify()
	}
	return e, true, false
}

// Pop removes the oldest pending key and returns it with its newest value,
// waiting while none is pending and the KeyedLatest is open. It returns
// ErrQueueClosed once it is closed and empty, or ctx.Err() if ctx ends
// first.
func (q *KeyedLatest[K, T]) Pop(ctx context.Context) (k K, v T, err error) {
	if q.ring == nil {
		return k, v, errZeroKeyedLatest
	}
	for {
		e, ok, closed := q.pop()
		if ok {
			return e.key, e.v, nil
		}
		if closed {
			return k, v, ErrQueueClosed
		}
		select {
		case <-q.ready:
		case <-q.done:
		case <-ctx.Done():
			return k, v, ctx.Err()
		}
	}
}

// TryPop removes the oldest pending key and returns it with its newest
// value if there is one.
func (q *KeyedLatest[K, T]) TryPop() (k K, v T, ok bool) {
	if q.ring == nil {
		return k, v, false
	}
	e, ok, _ := q.pop()
	return e.key, e.v, ok
}

// Drain removes every pending key, passes each with its value to fn, and
// returns how many it removed. Keys set during Drain may be removed too.
func (q *KeyedLatest[K, T]) Drain(fn func(K, T)) int {
	n := 0
	for {
		k, v, ok := q.TryPop()
		if !ok {
			return n
		}
		n++
		if fn != nil {
			fn(k, v)
		}
	}
}

// Close closes the KeyedLatest; see [KeyedLatest]. It is idempotent, runs
// no user code and always returns nil.
func (q *KeyedLatest[K, T]) Close() error {
	if q.done == nil {
		return nil
	}
	q.mu.Lock()
	if !q.closed {
		q.closed = true
		close(q.done)
	}
	q.mu.Unlock()
	return nil
}

// Len returns the number of pending keys.
func (q *KeyedLatest[K, T]) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.n
}

// Cap returns the capacity.
func (q *KeyedLatest[K, T]) Cap() int { return len(q.ring) }

// Stats returns the counters.
func (q *KeyedLatest[K, T]) Stats() KeyedLatestStats {
	q.mu.Lock()
	defer q.mu.Unlock()
	return KeyedLatestStats{
		Pushed:      q.pushed,
		Overwritten: q.overwritten,
		Evicted:     q.evicted,
		Rejected:    q.rejected,
		Waited:      q.waited,
		Len:         q.n,
		Cap:         len(q.ring),
	}
}
