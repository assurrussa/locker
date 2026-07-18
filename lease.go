package locker

import (
	"context"
	"errors"
	"sync"
)

type releaseState uint8

const (
	releaseStateActive releaseState = iota
	releaseStateReleasing
	releaseStateReleased
)

// Lease represents one acquired distributed lease. A Lease is bound to the
// Locker that created it and must not be copied.
type Lease struct {
	owner    *Locker
	key      string
	token    string
	blocking bool

	lostCh chan struct{}

	keepaliveCancel context.CancelFunc
	keepaliveDone   chan struct{}
	events          *eventDispatcher

	mu        sync.Mutex
	state     releaseState
	lostErr   error
	lostClose sync.Once
}

func newLease(owner *Locker, key, token string, blocking bool, events *eventDispatcher) *Lease {
	return &Lease{
		owner:         owner,
		key:           key,
		token:         token,
		blocking:      blocking,
		lostCh:        make(chan struct{}),
		keepaliveDone: make(chan struct{}),
		events:        events,
		state:         releaseStateActive,
	}
}

// Key returns the raw caller-provided lock key.
func (lease *Lease) Key() string {
	if lease == nil {
		return ""
	}

	return lease.key
}

// Lost is closed once ownership is unexpectedly and permanently lost. A
// successful Release does not close it.
func (lease *Lease) Lost() <-chan struct{} {
	if lease == nil {
		return nil
	}

	return lease.lostCh
}

// Err returns an error satisfying ErrLost and preserving the first loss cause.
// It returns nil before loss and after a successful Release.
func (lease *Lease) Err() error {
	if !lease.initialized() {
		return ErrInvalidLease
	}

	lease.mu.Lock()
	defer lease.mu.Unlock()

	return lease.lostErr
}

// Release stops keepalive and releases the lease using its ownership token.
// The Store operation respects ctx and is bounded by the Locker operation
// timeout.
//
// A Store error marks the lease lost but permits another Release as a cleanup
// attempt. Loss remains permanent even when that cleanup later succeeds.
func (lease *Lease) Release(ctx context.Context) error {
	if lease == nil || lease.owner == nil {
		return ErrInvalidLease
	}

	return lease.owner.release(ctx, lease)
}

func (lease *Lease) validFor(owner *Locker) bool {
	return lease.initialized() && lease.owner == owner
}

func (lease *Lease) initialized() bool {
	return lease != nil && lease.owner != nil && lease.key != "" && lease.token != "" &&
		lease.lostCh != nil && lease.keepaliveDone != nil
}

func (lease *Lease) beginRelease() bool {
	lease.mu.Lock()
	defer lease.mu.Unlock()

	if lease.state != releaseStateActive {
		return false
	}
	lease.state = releaseStateReleasing

	return true
}

func (lease *Lease) rollbackRelease() {
	lease.mu.Lock()
	defer lease.mu.Unlock()

	if lease.state == releaseStateReleasing {
		lease.state = releaseStateActive
	}
}

func (lease *Lease) markReleased() {
	lease.mu.Lock()
	lease.state = releaseStateReleased
	lease.mu.Unlock()
}

func (lease *Lease) markReleasedWithEvent(ctx context.Context, event Event) error {
	lease.mu.Lock()
	defer lease.mu.Unlock()

	lease.state = releaseStateReleased
	lease.emitLocked(ctx, event)

	return lease.lostErr
}

func (lease *Lease) emitRefreshSuccess(ctx context.Context, event Event) bool {
	lease.mu.Lock()
	defer lease.mu.Unlock()

	if lease.state == releaseStateReleased {
		return false
	}
	lease.emitLocked(ctx, event)

	return lease.state == releaseStateActive
}

func (lease *Lease) recordLoss(
	ctx context.Context,
	cause error,
	onlyWhenActive bool,
	failure Event,
) bool {
	lease.mu.Lock()
	defer lease.mu.Unlock()

	if onlyWhenActive && lease.state != releaseStateActive {
		return false
	}
	lease.emitLocked(ctx, failure)
	if lease.lostErr != nil {
		return false
	}
	if cause == nil {
		lease.lostErr = ErrLost
	} else if errors.Is(cause, ErrLost) {
		lease.lostErr = cause
	} else {
		lease.lostErr = errors.Join(ErrLost, cause)
	}

	lost := failure
	lost.Type = EventLost
	lost.Err = lease.lostErr
	lease.emitLocked(ctx, lost)
	lease.lostClose.Do(func() { close(lease.lostCh) })

	return true
}

func (lease *Lease) emit(ctx context.Context, event Event) {
	if lease == nil || lease.events == nil {
		return
	}
	lease.events.enqueue(ctx, event)
}

func (lease *Lease) emitLocked(ctx context.Context, event Event) {
	if lease.events != nil {
		lease.events.enqueue(ctx, event)
	}
}

func (lease *Lease) setKeepaliveCancel(cancel context.CancelFunc) {
	lease.mu.Lock()
	lease.keepaliveCancel = cancel
	lease.mu.Unlock()
}

func (lease *Lease) stopKeepalive() {
	lease.mu.Lock()
	cancel := lease.keepaliveCancel
	done := lease.keepaliveDone
	lease.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if done != nil {
		<-done
	}
}
