package lifecycle

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
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
	// DropOldest removes the oldest item, passes it to OnDrop, and adds the
	// new one. Removal and addition happen together, so Close never leaves
	// an item dropped without its replacement added. If racing pushes keep
	// taking the freed space, more than one item may be dropped before the
	// new one is added or Push returns ErrQueueClosed.
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
	// runs in the pushing goroutine with no lock held.
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

const queueClosedBit = 1 << 62

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
	items    chan T
	done     chan struct{} // closed by Close
	sealed   chan struct{} // closed once Close ran and no Push is in flight
	state    atomic.Int64  // queueClosedBit | number of Push calls in flight
	once     sync.Once
	sealOnce sync.Once
	full     FullPolicy
	onDrop   func(T)
	timeout  time.Duration

	pushed, dropped, rejected, waited atomic.Uint64
}

// NewQueue returns an empty queue configured by cfg.
func NewQueue[T any](cfg QueueConfig[T]) (*Queue[T], error) {
	switch {
	case cfg.Capacity <= 0:
		return nil, errors.New("lifecycle: queue capacity must be positive")
	case cfg.Full < Reject || cfg.Full > Block:
		return nil, errors.New("lifecycle: queue full policy not set")
	case cfg.Full == DropOldest && cfg.OnDrop == nil:
		return nil, errors.New("lifecycle: DropOldest queue needs OnDrop")
	case cfg.Full != DropOldest && cfg.OnDrop != nil:
		return nil, errors.New("lifecycle: OnDrop is only used by DropOldest")
	case cfg.BlockTimeout < 0, cfg.Full != Block && cfg.BlockTimeout != 0:
		return nil, errors.New("lifecycle: BlockTimeout must be zero or positive and is only used by Block")
	}
	return &Queue[T]{
		items:   make(chan T, cfg.Capacity),
		done:    make(chan struct{}),
		sealed:  make(chan struct{}),
		full:    cfg.Full,
		onDrop:  cfg.OnDrop,
		timeout: cfg.BlockTimeout,
	}, nil
}

// enter registers a Push in flight and reports false if the queue is
// closed.
func (q *Queue[T]) enter() bool {
	if q.state.Add(1)&queueClosedBit != 0 {
		q.exit()
		return false
	}
	return true
}

func (q *Queue[T]) exit() {
	if q.state.Add(-1) == queueClosedBit {
		q.seal()
	}
}

// seal may run more than once: a Push that enters after Close and leaves
// again brings the count back to zero too.
func (q *Queue[T]) seal() { q.sealOnce.Do(func() { close(q.sealed) }) }

// Push adds v according to the full policy. ctx bounds the wait under
// Block and is otherwise unused. It returns ErrFull, ErrQueueClosed, or
// ctx.Err() when a Block wait ends with ctx.
func (q *Queue[T]) Push(ctx context.Context, v T) error {
	for {
		if !q.enter() {
			return ErrQueueClosed
		}
		select {
		case q.items <- v:
			q.exit()
			q.pushed.Add(1)
			return nil
		default:
		}
		switch q.full {
		case Reject:
			q.exit()
			q.rejected.Add(1)
			return ErrFull
		case Block:
			err := q.wait(ctx, v)
			q.exit()
			return err
		case DropOldest:
			if q.replaceOldest(v) {
				q.pushed.Add(1)
				return nil
			}
		default: // a Queue not made by NewQueue
			q.exit()
			return errZeroQueue
		}
	}
}

// replaceOldest removes one item and adds v within one in-flight section,
// so Close cannot come between them, then leaves the section and releases
// the removed item. It reports false if a racing Push took the space; the
// caller then tries again.
func (q *Queue[T]) replaceOldest(v T) bool {
	var old T
	got, added := false, false
	select {
	case old = <-q.items:
		got = true
	default:
	}
	select {
	case q.items <- v:
		added = true
	default:
	}
	q.exit()
	if got {
		q.dropped.Add(1)
		q.onDrop(old)
	}
	return added
}

func (q *Queue[T]) wait(ctx context.Context, v T) error {
	q.waited.Add(1)
	var timeout <-chan time.Time
	if q.timeout > 0 {
		t := time.NewTimer(q.timeout)
		defer t.Stop()
		timeout = t.C
	}
	select {
	case q.items <- v:
		q.pushed.Add(1)
		return nil
	case <-q.done:
		return ErrQueueClosed
	case <-ctx.Done():
		q.rejected.Add(1)
		return ctx.Err()
	case <-timeout:
		q.rejected.Add(1)
		return ErrFull
	}
}

// Pop removes and returns the oldest item, waiting while the queue is empty
// and open. It returns ErrQueueClosed once the queue is closed and empty,
// or ctx.Err() if ctx ends first.
func (q *Queue[T]) Pop(ctx context.Context) (T, error) {
	if q.done == nil {
		var zero T
		return zero, errZeroQueue
	}
	select {
	case v := <-q.items:
		return v, nil
	default:
	}
	select {
	case v := <-q.items:
		return v, nil
	case <-q.done:
		<-q.sealed // no Push is still adding an item
		if v, ok := q.TryPop(); ok {
			return v, nil
		}
		var zero T
		return zero, ErrQueueClosed
	case <-ctx.Done():
		var zero T
		return zero, ctx.Err()
	}
}

// TryPop removes and returns the oldest item if there is one. On a closed
// queue it first waits for Push calls still in flight, so it misses no item.
func (q *Queue[T]) TryPop() (T, bool) {
	select {
	case v := <-q.items:
		return v, true
	default:
	}
	select {
	case <-q.done:
		<-q.sealed
		select {
		case v := <-q.items:
			return v, true
		default:
		}
	default:
	}
	var zero T
	return zero, false
}

// Drain removes every queued item, passes each to fn, and returns how many
// it removed. After Close it removes everything, including items of Push
// calls that were in flight. On an open queue, items pushed during Drain
// may be removed too.
func (q *Queue[T]) Drain(fn func(T)) int {
	n := 0
	for {
		v, ok := q.TryPop()
		if !ok {
			return n
		}
		n++
		if fn != nil {
			fn(v)
		}
	}
}

// Close closes the queue; see [Queue]. It is idempotent and always returns
// nil.
func (q *Queue[T]) Close() error {
	if q.done == nil {
		return nil
	}
	q.once.Do(func() {
		close(q.done)
		if q.state.Add(queueClosedBit) == queueClosedBit {
			q.seal()
		}
	})
	return nil
}

// Len returns the number of queued items.
func (q *Queue[T]) Len() int { return len(q.items) }

// Cap returns the capacity.
func (q *Queue[T]) Cap() int { return cap(q.items) }

// Stats returns the counters.
func (q *Queue[T]) Stats() QueueStats {
	return QueueStats{
		Pushed:   q.pushed.Load(),
		Dropped:  q.dropped.Load(),
		Rejected: q.rejected.Load(),
		Waited:   q.waited.Load(),
		Len:      len(q.items),
		Cap:      cap(q.items),
	}
}
