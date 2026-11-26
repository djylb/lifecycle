package lifecycle

import (
	"context"
	"errors"
	"time"
)

var (
	// ErrFull is returned by Queue.Push and KeyedLatest.Set when there is no
	// room and the policy refuses the value: always under Reject, after
	// BlockTimeout under Block.
	ErrFull = errors.New("lifecycle: queue full")

	// ErrQueueClosed is returned by Push and Set on a closed Queue, Latest
	// or KeyedLatest, and by Pop and Wait once a closed one is empty.
	ErrQueueClosed = errors.New("lifecycle: queue closed")
)

var errZeroQueue = errors.New("lifecycle: Queue not created by NewQueue")

// FullPolicy is what Push does when the queue is full.
type FullPolicy uint8

const (
	// Reject refuses the new item with ErrFull.
	Reject FullPolicy = iota + 1
	// DropOldest commits the replacement and counters, then calls OnDrop with the evicted item.
	DropOldest
	// Block waits for space, at most BlockTimeout and while Push's context
	// lasts.
	Block
)

func (p FullPolicy) String() string {
	switch p {
	case Reject:
		return "Reject"
	case DropOldest:
		return "DropOldest"
	case Block:
		return "Block"
	}
	return "FullPolicy(invalid)"
}

// QueueConfig configures a [Queue]. Capacity and Full are required.
type QueueConfig[T any] struct {
	// Capacity is the maximum number of queued items; it must be positive.
	Capacity int
	// Full is the policy when the queue is full.
	Full FullPolicy
	// OnDrop receives each item removed by DropOldest, so that it can be
	// released. It is required with DropOldest and not allowed otherwise. It
	// runs in the pushing goroutine with no lock held, after the replacement
	// and its counters are committed.
	OnDrop func(T)
	// BlockTimeout bounds each waiting Push under Block. Zero means the wait
	// is bounded by Push's context alone. Not allowed with other policies.
	BlockTimeout time.Duration
}

// QueueStats are the counters of a [Queue].
type QueueStats struct {
	Pushed   uint64 // items accepted, including those dropped later
	Dropped  uint64 // items removed by DropOldest
	Rejected uint64 // pushes refused because the queue was full, including Block pushes that gave up
	Waited   uint64 // Block pushes that found the queue full and waited
	Len      int    // items queued now
	Cap      int    // capacity
}

// Queue must be constructed with NewQueue and must not be copied.
// Queue is a bounded FIFO queue safe for concurrent use. It has no
// unbounded mode and starts no goroutines; waiting Push and Pop calls park
// on channels and hold no lock.
//
// Close stops new pushes and wakes every waiting Push and Pop. Items already
// queued stay available: Pop and TryPop return them until the queue is
// empty, after which Pop returns ErrQueueClosed. Drain removes them
// instead. Close implements io.Closer, so a queue can be added to a
// [Scope].
type Queue[T any] struct {
	// A FIFO uses the same ring and policy transitions with indexing disabled.
	fifo KeyedLatest[struct{}, T]
}

func validateBuffer(capacity int, full FullPolicy, timeout time.Duration) error {
	switch {
	case capacity <= 0:
		return errQueueCapacity
	case full < Reject || full > Block:
		return errQueueFull
	case timeout < 0 || full != Block && timeout != 0:
		return errBlockTimeout
	}
	return nil
}

// NewQueue returns an empty queue configured by cfg.
func NewQueue[T any](cfg QueueConfig[T]) (*Queue[T], error) {
	if err := validateBuffer(cfg.Capacity, cfg.Full, cfg.BlockTimeout); err != nil {
		return nil, err
	}
	switch {
	case cfg.Full == DropOldest && cfg.OnDrop == nil:
		return nil, errQueueOnDrop
	case cfg.Full != DropOldest && cfg.OnDrop != nil:
		return nil, errQueueOnDropUnused
	}
	q := new(Queue[T])
	q.fifo.initRing(cfg.Capacity, cfg.Full, cfg.BlockTimeout)
	if cfg.Full == Block {
		q.fifo.space = make(signal, 1)
	}
	if cfg.OnDrop != nil {
		q.fifo.onDrop = func(_ struct{}, v T) { cfg.OnDrop(v) }
	}
	return q, nil
}

// Push adds v according to the full policy. It returns ErrQueueClosed after Close,
// ErrFull on rejection or BlockTimeout, or ctx.Err() if cancelled while waiting.
func (q *Queue[T]) Push(ctx context.Context, v T) error {
	if q.fifo.ring == nil {
		return errZeroQueue
	}
	return q.fifo.set(ctx, struct{}{}, v)
}

// Pop returns the oldest item, ErrQueueClosed when closed and empty,
// or ctx.Err() if cancelled while waiting.
func (q *Queue[T]) Pop(ctx context.Context) (T, error) {
	if q.fifo.ring == nil {
		var zero T
		return zero, errZeroQueue
	}
	_, v, err := q.fifo.Pop(ctx)
	return v, err
}

// TryPop removes the oldest available item without waiting for a producer.
func (q *Queue[T]) TryPop() (T, bool) { _, v, ok := q.fifo.TryPop(); return v, ok }

// Drain removes queued items and passes them to fn when non-nil.
func (q *Queue[T]) Drain(fn func(T)) int {
	return q.fifo.TakePending(func(_ struct{}, v T) {
		if fn != nil {
			fn(v)
		}
	})
}

// Close stops admissions and wakes waiting callers.
func (q *Queue[T]) Close() error { return q.fifo.Close() }

// Len returns the number of queued items.
func (q *Queue[T]) Len() int { return q.fifo.Len() }

// Cap returns the capacity.
func (q *Queue[T]) Cap() int { return q.fifo.Cap() }

// Stats returns the counters.
func (q *Queue[T]) Stats() QueueStats {
	s := q.fifo.Stats()
	return QueueStats{Pushed: s.Pushed, Dropped: s.Evicted, Rejected: s.Rejected, Waited: s.Waited, Len: s.Len, Cap: s.Cap}
}
