package locker

import (
	"context"
	"sync"
	"time"
)

const observerQueueCapacity = 64

// EventType identifies a lock lifecycle event.
type EventType string

// Lifecycle event types emitted by Locker.
const (
	EventAcquireAttempt EventType = "acquire_attempt"
	EventContention     EventType = "contention"
	EventAcquired       EventType = "acquired"
	EventAcquireFailed  EventType = "acquire_failed"
	EventRefreshed      EventType = "refreshed"
	EventRefreshFailed  EventType = "refresh_failed"
	EventReleased       EventType = "released"
	EventReleaseFailed  EventType = "release_failed"
	EventLost           EventType = "lost"
)

// Event describes one lock lifecycle observation.
//
// Key is the raw caller-provided key. It may contain sensitive or
// high-cardinality data and must not be used as an uncontrolled metric label.
// Duration is elapsed time for the current acquire flow or storage operation.
type Event struct {
	Type     EventType
	Key      string
	Blocking bool
	Attempts int
	Duration time.Duration
	Err      error
}

// Observer receives lifecycle events asynchronously. Calls from one Locker are
// serialized; the same Observer used by different Locker instances may be
// called concurrently. Delivered events within one lock flow remain ordered.
// Delivery uses one bounded best-effort queue per Locker and may drop routine
// attempt, contention, or refresh-success events under sustained backpressure.
// Observe should return quickly. Locker isolates observer panics from lock
// lifecycle.
type Observer interface {
	Observe(context.Context, Event)
}

// ObserverFunc adapts a function to Observer.
type ObserverFunc func(context.Context, Event)

// Observe implements Observer.
func (f ObserverFunc) Observe(ctx context.Context, event Event) {
	f(ctx, event)
}

type observedEvent struct {
	ctx   context.Context
	event Event
}

// eventDispatcher isolates observer latency from every lock flow owned by one
// Locker while preserving the order of events delivered for each flow.
// Delivery is bounded: the lowest-priority queued events are dropped first
// when an observer applies sustained backpressure.
type eventDispatcher struct {
	observer Observer

	mu      sync.Mutex
	queue   []observedEvent
	running bool
}

func newEventDispatcher(observer Observer) *eventDispatcher {
	if nilInterface(observer) {
		return nil
	}

	return &eventDispatcher{
		observer: observer,
		queue:    make([]observedEvent, 0, observerQueueCapacity),
	}
}

func (d *eventDispatcher) enqueue(ctx context.Context, event Event) {
	if d == nil {
		return
	}

	d.mu.Lock()
	if len(d.queue) == observerQueueCapacity {
		incomingPriority := observerEventPriority(event.Type)
		if incomingPriority == 0 {
			d.mu.Unlock()

			return
		}

		drop := -1
		dropPriority := incomingPriority
		for index := range d.queue {
			priority := observerEventPriority(d.queue[index].event.Type)
			if priority < dropPriority {
				drop = index
				dropPriority = priority
			}
		}
		if drop < 0 {
			d.mu.Unlock()

			return
		}
		copy(d.queue[drop:], d.queue[drop+1:])
		d.queue[len(d.queue)-1] = observedEvent{}
		d.queue = d.queue[:len(d.queue)-1]
	}

	d.queue = append(d.queue, observedEvent{ctx: nonNilContext(ctx), event: event})
	if d.running {
		d.mu.Unlock()

		return
	}
	d.running = true
	d.mu.Unlock()

	go d.run()
}

func (d *eventDispatcher) run() {
	defer d.finishRun()

	for {
		item, ok := d.next()
		if !ok {
			return
		}
		observeSafely(d.observer, item)
	}
}

func (d *eventDispatcher) next() (observedEvent, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if len(d.queue) == 0 {
		return observedEvent{}, false
	}
	item := d.queue[0]
	copy(d.queue, d.queue[1:])
	d.queue[len(d.queue)-1] = observedEvent{}
	d.queue = d.queue[:len(d.queue)-1]

	return item, true
}

func (d *eventDispatcher) finishRun() {
	d.mu.Lock()
	d.running = false
	restart := len(d.queue) > 0
	if restart {
		d.running = true
	}
	d.mu.Unlock()

	if restart {
		go d.run()
	}
}

func observeSafely(observer Observer, item observedEvent) {
	defer func() { _ = recover() }()
	observer.Observe(item.ctx, item.event)
}

func observerEventPriority(eventType EventType) int {
	switch eventType {
	case EventAcquireAttempt, EventContention, EventRefreshed:
		return 0
	case EventRefreshFailed, EventReleaseFailed:
		return 1
	default:
		return 2
	}
}
