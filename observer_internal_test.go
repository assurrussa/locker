package locker

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestEventDispatcherBoundsBackpressureAndKeepsTerminalEvent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		backlog  EventType
		terminal EventType
	}{
		{name: "routine backlog", backlog: EventRefreshed, terminal: EventLost},
		{name: "failure backlog", backlog: EventReleaseFailed, terminal: EventReleased},
		{name: "failed acquire", backlog: EventReleaseFailed, terminal: EventAcquireFailed},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			observerEntered := make(chan struct{})
			observerContinue := make(chan struct{})
			terminalSeen := make(chan struct{})
			var (
				mu           sync.Mutex
				types        []EventType
				enterOnce    sync.Once
				terminalOnce sync.Once
			)
			dispatcher := newEventDispatcher(ObserverFunc(func(_ context.Context, event Event) {
				if event.Type == EventAcquired {
					enterOnce.Do(func() { close(observerEntered) })
					<-observerContinue
				}
				mu.Lock()
				types = append(types, event.Type)
				mu.Unlock()
				if event.Type == test.terminal {
					terminalOnce.Do(func() { close(terminalSeen) })
				}
			}))
			dispatcher.enqueue(context.Background(), Event{Type: EventAcquired})
			<-observerEntered
			for range observerQueueCapacity + 10 {
				dispatcher.enqueue(context.Background(), Event{Type: test.backlog})
			}
			dispatcher.enqueue(context.Background(), Event{Type: test.terminal})
			close(observerContinue)

			select {
			case <-terminalSeen:
			case <-time.After(time.Second):
				t.Fatal("terminal observer event was dropped")
			}
			mu.Lock()
			defer mu.Unlock()
			if len(types) != observerQueueCapacity+1 {
				t.Fatalf("delivered %d events, want %d", len(types), observerQueueCapacity+1)
			}
			if types[0] != EventAcquired || types[len(types)-1] != test.terminal {
				t.Fatalf("unexpected terminal order: first=%q last=%q", types[0], types[len(types)-1])
			}
		})
	}
}

func TestEventDispatcherDropsLowestPriorityQueuedEvent(t *testing.T) {
	t.Parallel()

	observerEntered := make(chan struct{})
	observerContinue := make(chan struct{})
	releasedSeen := make(chan struct{})
	var (
		mu        sync.Mutex
		types     []EventType
		enterOnce sync.Once
	)
	dispatcher := newEventDispatcher(ObserverFunc(func(_ context.Context, event Event) {
		if event.Type == EventAcquired {
			enterOnce.Do(func() { close(observerEntered) })
			<-observerContinue
		}
		mu.Lock()
		types = append(types, event.Type)
		mu.Unlock()
		if event.Type == EventReleased {
			close(releasedSeen)
		}
	}))
	dispatcher.enqueue(context.Background(), Event{Type: EventAcquired})
	<-observerEntered
	dispatcher.enqueue(context.Background(), Event{Type: EventReleaseFailed})
	for range observerQueueCapacity - 1 {
		dispatcher.enqueue(context.Background(), Event{Type: EventRefreshed})
	}
	dispatcher.enqueue(context.Background(), Event{Type: EventReleased})
	close(observerContinue)

	select {
	case <-releasedSeen:
	case <-time.After(time.Second):
		t.Fatal("released event was not delivered")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(types) != observerQueueCapacity+1 {
		t.Fatalf("delivered %d events, want %d", len(types), observerQueueCapacity+1)
	}
	if types[1] != EventReleaseFailed {
		t.Fatalf("higher-priority queued event was dropped: %q", types[1])
	}
}
