package lifecycle

import (
	"context"
	"errors"
	"hash/maphash"
	"reflect"
	"sync"
	"time"
)

var (
	// ErrInvalidKey indicates a dynamically incomparable or non-reflexive key.
	ErrInvalidKey      = errors.New("lifecycle: invalid mailbox key")
	errZeroLatest      = errors.New("lifecycle: Latest not created by NewLatest")
	errZeroKeyedLatest = errors.New("lifecycle: KeyedLatest not created by NewKeyedLatest")
)

func closeMailbox(closed *bool, done chan struct{}) {
	if !*closed {
		*closed = true
		close(done)
	}
}

// signal wakes one waiter. It holds at most one token, so sending never
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

// Latest must be constructed with NewLatest and must not be copied.
// Latest is a single-slot mailbox where the newest value wins: Set never
// waits for space and replaces a value that has not been taken yet. It is safe for
// concurrent use, starts no goroutines, and waiting calls park on channels
// with no lock held. It is meant for one consumer; with several, each
// value goes to one of them.
//
// Close stops new values and wakes every waiter. A value still pending stays
// available to Wait, TryGet and TakePending. Once empty, Wait returns ErrQueueClosed.
// Close implements io.Closer so callers can add the mailbox to a [Scope].
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
// value not taken yet. It never waits for space. On a closed Latest it returns
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

// TakePending takes the pending value, if any, and returns how many values
// it took (0 or 1). It passes the value to fn when fn is non-nil.
func (l *Latest[T]) TakePending(fn func(T)) int {
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
	closeMailbox(&l.closed, l.done)
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

// Waiting setters join the collecting round. A change releases that entire
// round by passing one token between its members. A change during delivery
// schedules the collecting round next, so new arrivals cannot steal tokens.
type writerRounds struct {
	rounds     [2]writerRound
	collecting int
	pending    bool
}
type writerRound struct {
	signal signal
	count  int
}

func (w *writerRounds) join() *writerRound {
	r := &w.rounds[w.collecting]
	r.count++
	return r
}

func (w *writerRounds) notify() {
	if w.rounds[w.collecting].count == 0 {
		return
	}
	if w.rounds[1-w.collecting].count != 0 {
		w.pending = true
		return
	}
	w.pending = false
	w.collecting = 1 - w.collecting
	w.rounds[1-w.collecting].signal.notify()
}

func (w *writerRounds) leave(r *writerRound) {
	r.count--
	if r == &w.rounds[w.collecting] {
		return
	}
	if r.count != 0 {
		r.signal.notify()
		return
	}
	// A cancelled waiter may have left the delivery token in the channel.
	select {
	case <-r.signal:
	default:
	}
	if w.pending {
		w.pending = false
		w.notify()
	}
}

type keyedEntry[K comparable, T any] struct {
	key K
	v   T
}

// KeyedLatest must be constructed with NewKeyedLatest and must not be copied.
// KeyedLatest keeps the newest value of each key, for at most Capacity
// distinct pending keys. Pop returns pending keys in the order in which they
// became pending; a Set on a pending key replaces its value without moving
// it. It is safe for concurrent use, starts no goroutines, and waiting calls
// park on channels with no lock held.
//
// Close stops new values and wakes every waiter. Pending keys stay
// available to Pop and TryPop, after which Pop returns ErrQueueClosed.
// TakePending removes them as well. Close implements io.Closer, so a KeyedLatest
// can be added to a [Scope].
type KeyedLatest[K comparable, T any] struct {
	mu            sync.Mutex
	ring          []keyedEntry[K, T] // pending keys in FIFO order, from head
	head, n       int
	buckets, next []int // collision chains, ring positions plus one
	seed          maphash.Seed
	writers       writerRounds
	ready         signal // a key is pending
	space         signal // a slot is free, for Block
	done          chan struct{}
	onDrop        func(K, T)
	timeout       time.Duration

	pushed, overwritten, evicted, rejected, waited uint64

	full   FullPolicy
	closed bool
}

// NewKeyedLatest returns an empty KeyedLatest configured by cfg.
func NewKeyedLatest[K comparable, T any](cfg KeyedLatestConfig[K, T]) (*KeyedLatest[K, T], error) {
	if err := validateBuffer(cfg.Capacity, cfg.Full, cfg.BlockTimeout); err != nil {
		return nil, err
	}
	if cfg.Full == DropOldest && cfg.OnDrop == nil {
		return nil, errLatestOnDrop
	}
	q := &KeyedLatest[K, T]{
		buckets: make([]int, cfg.Capacity),
		next:    make([]int, cfg.Capacity),
		seed:    maphash.MakeSeed(),
		onDrop:  cfg.OnDrop,
	}
	q.initRing(cfg.Capacity, cfg.Full, cfg.BlockTimeout)
	if cfg.Full == Block {
		for i := range q.writers.rounds {
			q.writers.rounds[i].signal = make(signal, 1)
		}
	}
	return q, nil
}

func (q *KeyedLatest[K, T]) initRing(capacity int, full FullPolicy, timeout time.Duration) {
	q.ring = make([]keyedEntry[K, T], capacity)
	q.ready = make(signal, 1)
	q.done = make(chan struct{})
	q.full = full
	q.timeout = timeout
}

// Set returns ErrInvalidKey if k cannot compare equal to itself, even after Close.
// Set stores v as the newest value of k. If k is pending, its value is
// replaced in place and the old one passed to OnDrop. Otherwise, k is added
// at the end, applying the full policy if Capacity keys are pending: Reject
// returns ErrFull, DropOldest evicts the oldest pending key and passes its
// value to OnDrop, Block waits for a free slot, at most BlockTimeout and
// while ctx lasts. ctx is otherwise unused. On a closed KeyedLatest it
// returns ErrQueueClosed. On any error v stays with the caller.
func (q *KeyedLatest[K, T]) Set(ctx context.Context, k K, v T) error {
	if q.ring == nil {
		return errZeroKeyedLatest
	}
	if !validKey(k) {
		return ErrInvalidKey
	}
	return q.set(ctx, k, v)
}

func equalValues[T comparable](a, b T) bool { return a == b }

func validKey[K comparable](k K) bool {
	return comparableValue(reflect.ValueOf(&k).Elem()) && equalValues(k, k)
}

// Interface fields in composite keys may contain slices, maps or functions.
// Walk those values without boxing the key or provoking a comparison panic.
func comparableValue(v reflect.Value) bool {
	if !v.Type().Comparable() {
		return false
	}
	switch v.Kind() {
	case reflect.Interface:
		return v.IsNil() || comparableValue(v.Elem())
	case reflect.Struct:
		// The iterator escapes and allocates when walking composite keys.
		//goland:noinspection GoFixStdIterators
		for i := range v.NumField() { //nolint:modernize // indexing avoids boxing keys in an escaping iterator
			if !comparableValue(v.Field(i)) {
				return false
			}
		}
	case reflect.Array:
		kind := v.Type().Elem().Kind()
		if kind == reflect.Interface || kind == reflect.Struct || kind == reflect.Array {
			for i := range v.Len() {
				if !comparableValue(v.Index(i)) {
					return false
				}
			}
		}
	default:
		return true // comparable scalar, pointer or channel
	}
	return true
}

func (q *KeyedLatest[K, T]) set(ctx context.Context, k K, v T) error {
	done, round, err := q.trySet(k, v, nil)
	if done {
		return err
	}
	return q.blockSet(ctx, k, v, round)
}

// blockSet waits for a free slot under Block and retries trySet.
func (q *KeyedLatest[K, T]) blockSet(ctx context.Context, k K, v T, round *writerRound) error {
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
		wake := q.space
		if round != nil {
			wake = round.signal
		}
		select {
		case <-wake:
		case <-q.done:
		case <-ctx.Done():
			q.reject(round)
			return ctx.Err()
		case <-timeout:
			q.reject(round)
			return ErrFull
		}
		done, next, err := q.trySet(k, v, round)
		round = next
		if done {
			return err
		}
	}
}

// trySet does Set without waiting. It reports done false when the policy
// is Block and the caller must wait. Queue writers wait for space, keyed
// writers join a notification round so a new pending key also wakes them.
func (q *KeyedLatest[K, T]) trySet(k K, v T, round *writerRound) (done bool, next *writerRound, err error) {
	q.mu.Lock()
	if round != nil {
		q.writers.leave(round)
	}
	if q.closed {
		q.mu.Unlock()
		return true, nil, ErrQueueClosed
	}
	if i := q.lookup(k); i >= 0 {
		old := q.ring[i].v
		q.ring[i].v = v
		q.pushed++
		q.overwritten++
		more := q.n < len(q.ring)
		q.mu.Unlock()
		if more {
			q.space.notify()
		}
		if q.onDrop != nil {
			q.onDrop(k, old)
		}
		return true, nil, nil
	}
	if q.n < len(q.ring) {
		q.add(k, v)
		more := q.n < len(q.ring)
		q.mu.Unlock()
		q.ready.notify()
		if more {
			q.space.notify() // pass the wake-up on to another blocked Set
		}
		return true, nil, nil
	}
	switch q.full {
	case Reject:
		q.rejected++
		q.mu.Unlock()
		return true, nil, ErrFull
	case DropOldest: // full, so no Set has waited
		old := q.removeHead()
		q.evicted++
		q.add(k, v)
		q.mu.Unlock()
		q.ready.notify()
		q.onDrop(old.key, old.v)
		return true, nil, nil
	case Block:
		if q.buckets != nil {
			next = q.writers.join()
		}
		q.mu.Unlock()
		return false, next, nil
	default: // validated by NewKeyedLatest
		q.mu.Unlock()
		return true, nil, errZeroKeyedLatest
	}
}

func (q *KeyedLatest[K, T]) reject(round *writerRound) {
	q.mu.Lock()
	if round != nil {
		q.writers.leave(round)
	}
	q.rejected++
	q.mu.Unlock()
}

func (q *KeyedLatest[K, T]) bucket(k K) uint64 {
	return maphash.Comparable(q.seed, k) % uint64(len(q.buckets))
}
func (q *KeyedLatest[K, T]) lookup(k K) int {
	if q.buckets != nil {
		for pos := q.buckets[q.bucket(k)]; pos != 0; pos = q.next[pos-1] {
			if q.ring[pos-1].key == k {
				return pos - 1
			}
		}
	}
	return -1
}

// add appends k; q.mu must be held and a slot free.
func (q *KeyedLatest[K, T]) add(k K, v T) {
	i := (q.head + q.n) % len(q.ring)
	q.ring[i] = keyedEntry[K, T]{k, v}
	if q.buckets != nil {
		b := q.bucket(k)
		q.next[i] = q.buckets[b]
		q.buckets[b] = i + 1
		q.writers.notify()
	}
	q.n++
	q.pushed++
}

// removeHead removes the oldest key; q.mu must be held and a key pending.
func (q *KeyedLatest[K, T]) removeHead() keyedEntry[K, T] {
	e := q.ring[q.head]
	q.ring[q.head] = keyedEntry[K, T]{}
	if q.buckets != nil {
		link := &q.buckets[q.bucket(e.key)]
		for *link != q.head+1 {
			link = &q.next[*link-1]
		}
		*link = q.next[q.head]
		q.writers.notify()
	}
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

// TakePending removes keys in TryPop order until none are pending and
// returns how many it took. It passes each key and value to fn when fn is
// non-nil. Keys set during the call may be taken too.
func (q *KeyedLatest[K, T]) TakePending(fn func(K, T)) int {
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
	closeMailbox(&q.closed, q.done)
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
