package locker

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"math/big"
	"reflect"
	"time"
)

// Locker coordinates lease-based distributed locks through a Store.
type Locker struct {
	store  Store
	config config
	events *eventDispatcher
}

// New creates a Locker backed by store.
func New(store Store, options ...Option) (*Locker, error) {
	if nilInterface(store) {
		return nil, errors.New("locker: store must not be nil")
	}

	config := defaultConfig()
	for _, option := range options {
		if option != nil {
			option(&config)
		}
	}
	if err := config.validate(); err != nil {
		return nil, fmt.Errorf("locker: invalid options: %w", err)
	}

	return &Locker{
		store:  store,
		config: config,
		events: newEventDispatcher(config.observer),
	}, nil
}

// Acquire waits until key is acquired or ctx ends. After acquisition, ctx no
// longer controls the lease lifetime; callers must use Lease.Release and react
// to Lease.Lost.
func (l *Locker) Acquire(ctx context.Context, key string) (*Lease, error) {
	lease, acquired, err := l.acquire(ctx, key, true)
	if err != nil {
		return nil, err
	}
	if !acquired {
		return nil, ErrNotAcquired
	}

	return lease, nil
}

// TryAcquire attempts to acquire key once. Contention returns
// (nil, false, nil). A successful lease remains active until Release or loss.
func (l *Locker) TryAcquire(ctx context.Context, key string) (*Lease, bool, error) {
	return l.acquire(ctx, key, false)
}

func (l *Locker) release(ctx context.Context, lease *Lease) error {
	if l == nil || !lease.validFor(l) || !lease.beginRelease() {
		return ErrInvalidLease
	}

	lease.stopKeepalive()
	ctx = nonNilContext(ctx)
	started := time.Now()
	releaseCtx, cancel := context.WithTimeout(ctx, l.config.operationTimeout)
	released, releaseErr := l.store.Release(releaseCtx, lease.key, lease.token)
	cancel()
	duration := time.Since(started)

	if releaseErr != nil {
		cause := fmt.Errorf("locker: release: %w", releaseErr)
		firstLoss := lease.recordLoss(ctx, cause, false, Event{
			Type: EventReleaseFailed, Key: lease.key, Blocking: lease.blocking,
			Attempts: 1, Duration: duration, Err: cause,
		})
		lease.rollbackRelease()
		if firstLoss {
			return lease.Err()
		}

		return errors.Join(lease.Err(), cause)
	}

	if !released {
		cause := ErrNotOwner
		lease.recordLoss(ctx, cause, false, Event{
			Type: EventReleaseFailed, Key: lease.key, Blocking: lease.blocking,
			Attempts: 1, Duration: duration, Err: cause,
		})
		lease.markReleased()

		lostErr := lease.Err()
		if errors.Is(lostErr, ErrNotOwner) {
			return lostErr
		}

		return errors.Join(lostErr, ErrNotOwner)
	}

	lostErr := lease.markReleasedWithEvent(ctx, Event{
		Type: EventReleased, Key: lease.key, Blocking: lease.blocking,
		Attempts: 1, Duration: duration,
	})
	if lostErr != nil {
		return lostErr
	}

	return nil
}

// WithLock acquires key, runs callback, and releases the lock. The callback
// context is canceled if the lease is lost and the callback must honor it.
func (l *Locker) WithLock(ctx context.Context, key string, callback func(context.Context) error) error {
	if callback == nil {
		return errors.New("locker: callback must not be nil")
	}

	lease, err := l.Acquire(ctx, key)
	if err != nil {
		return err
	}

	return l.runCallback(ctx, lease, callback)
}

// WithTryLock attempts key once. It runs callback only when acquisition
// succeeds and reports whether the callback ran.
func (l *Locker) WithTryLock(
	ctx context.Context,
	key string,
	callback func(context.Context) error,
) (bool, error) {
	if callback == nil {
		return false, errors.New("locker: callback must not be nil")
	}

	lease, acquired, err := l.TryAcquire(ctx, key)
	if err != nil || !acquired {
		return acquired, err
	}

	return true, l.runCallback(ctx, lease, callback)
}

func (l *Locker) acquire(ctx context.Context, key string, blocking bool) (*Lease, bool, error) {
	started := time.Now()
	ctx = nonNilContext(ctx)
	var events *eventDispatcher
	if l != nil {
		events = l.events
	}
	fail := func(attempts int, cause error) (*Lease, bool, error) {
		err := cause
		if !errors.Is(cause, ErrNotAcquired) {
			err = errors.Join(ErrNotAcquired, cause)
		}
		events.enqueue(ctx, Event{Type: EventAcquireFailed, Key: key, Blocking: blocking, Attempts: attempts, Duration: time.Since(started), Err: err})

		return nil, false, err
	}

	if l == nil || nilInterface(l.store) {
		return fail(0, errors.New("locker: locker is nil or uninitialized"))
	}
	if key == "" {
		return fail(0, errors.New("locker: key must not be empty"))
	}
	if err := ctx.Err(); err != nil {
		return fail(0, err)
	}

	token, err := generateToken()
	if err != nil {
		return fail(0, fmt.Errorf("locker: generate ownership token: %w", err))
	}

	attempts := 0
	backoff := l.config.retryBackoff
	for {
		attempts++
		events.enqueue(ctx, Event{Type: EventAcquireAttempt, Key: key, Blocking: blocking, Attempts: attempts, Duration: time.Since(started)})

		operationCtx, cancel := context.WithTimeout(ctx, l.config.operationTimeout)
		acquired, acquireErr := l.store.Acquire(operationCtx, key, token, l.config.leaseTTL)
		cancel()
		if acquireErr != nil {
			return fail(attempts, fmt.Errorf("locker: acquire: %w", acquireErr))
		}
		if acquired {
			lease := newLease(l, key, token, blocking, events)
			keepaliveCtx, keepaliveCancel := context.WithCancel(context.WithoutCancel(ctx))
			lease.setKeepaliveCancel(keepaliveCancel)
			lease.emit(ctx, Event{Type: EventAcquired, Key: key, Blocking: blocking, Attempts: attempts, Duration: time.Since(started)})
			go l.keepalive(keepaliveCtx, lease)

			return lease, true, nil
		}

		events.enqueue(ctx, Event{Type: EventContention, Key: key, Blocking: blocking, Attempts: attempts, Duration: time.Since(started)})
		if !blocking {
			events.enqueue(ctx, Event{Type: EventAcquireFailed, Key: key, Blocking: false, Attempts: attempts, Duration: time.Since(started), Err: ErrNotAcquired})

			return nil, false, nil
		}

		if sleepErr := sleepContext(ctx, jitter(backoff, l.config.jitterRatio, l.config.retryBackoffMax)); sleepErr != nil {
			return fail(attempts, sleepErr)
		}
		if backoff >= l.config.retryBackoffMax-backoff {
			backoff = l.config.retryBackoffMax
		} else {
			backoff += backoff
		}
	}
}

func (l *Locker) keepalive(ctx context.Context, lease *Lease) {
	ticker := time.NewTicker(l.config.refreshInterval)
	defer ticker.Stop()
	defer close(lease.keepaliveDone)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		started := time.Now()
		refreshCtx, cancel := context.WithTimeout(ctx, l.config.operationTimeout)
		refreshed, refreshErr := l.store.Refresh(refreshCtx, lease.key, lease.token, l.config.leaseTTL)
		cancel()
		duration := time.Since(started)

		if refreshErr != nil || !refreshed {
			cause := ErrNotOwner
			if refreshErr != nil {
				cause = fmt.Errorf("locker: refresh: %w", refreshErr)
			}
			if !lease.recordLoss(ctx, cause, true, Event{
				Type: EventRefreshFailed, Key: lease.key, Blocking: lease.blocking,
				Attempts: 1, Duration: duration, Err: cause,
			}) {
				return
			}

			return
		}

		if !lease.emitRefreshSuccess(ctx, Event{
			Type: EventRefreshed, Key: lease.key, Blocking: lease.blocking,
			Attempts: 1, Duration: duration,
		}) {
			return
		}
	}
}

func (l *Locker) runCallback(ctx context.Context, lease *Lease, callback func(context.Context) error) (result error) {
	ctx = nonNilContext(ctx)
	callbackCtx, cancel := context.WithCancel(ctx)
	defer func() {
		callbackPanic := recover()
		releasePanic, releaseErr := releaseSafely(context.WithoutCancel(ctx), lease)
		result = errors.Join(result, releaseErr)
		if callbackPanic != nil {
			panic(callbackPanic)
		}
		if releasePanic != nil {
			panic(releasePanic)
		}
	}()
	defer cancel()

	go func() {
		select {
		case <-callbackCtx.Done():
		case <-lease.Lost():
			cancel()
		}
	}()

	return callback(callbackCtx)
}

func releaseSafely(ctx context.Context, lease *Lease) (panicValue any, err error) {
	panicked := true
	func() {
		defer func() {
			if panicked {
				panicValue = recover()
			}
		}()
		err = lease.Release(ctx)
		panicked = false
	}()

	return panicValue, err
}

func generateToken() (string, error) {
	bytes := make([]byte, tokenBytes)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}

	return hex.EncodeToString(bytes), nil
}

func sleepContext(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func jitter(base time.Duration, ratio float64, maximum time.Duration) time.Duration {
	lower, upper := jitterBounds(base, ratio, maximum)
	if lower >= upper {
		return lower
	}

	span := int64(upper-lower) + 1
	random, err := rand.Int(rand.Reader, big.NewInt(span))
	if err != nil {
		return minDuration(base, maximum)
	}

	return lower + time.Duration(random.Int64())
}

func jitterBounds(base time.Duration, ratio float64, maximum time.Duration) (time.Duration, time.Duration) {
	if maximum <= 0 {
		maximum = base
	}
	if base > maximum {
		base = maximum
	}
	if base <= 0 || ratio <= 0 {
		return base, base
	}

	amplitude := time.Duration(math.Round(float64(base) * ratio))
	if amplitude < 0 || amplitude > base {
		amplitude = base
	}

	lower := base - amplitude
	if lower < time.Nanosecond {
		lower = time.Nanosecond
	}

	upper := maximum
	if amplitude <= maximum-base {
		upper = base + amplitude
	}

	return lower, upper
}

func minDuration(left, right time.Duration) time.Duration {
	if left < right {
		return left
	}

	return right
}

func nonNilContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}

	return ctx
}

func nilInterface(value any) bool {
	if value == nil {
		return true
	}

	rv := reflect.ValueOf(value)
	switch rv.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return rv.IsNil()
	default:
		return false
	}
}
