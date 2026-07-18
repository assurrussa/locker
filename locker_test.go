package locker_test

import (
	"context"
	"encoding/hex"
	"errors"
	"math"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/assurrussa/locker"
	"github.com/stretchr/testify/require"
)

type storeCall struct {
	key   string
	token string
	ttl   time.Duration
}

type fakeStore struct {
	mu sync.Mutex

	acquireFn func(context.Context, storeCall) (bool, error)
	refreshFn func(context.Context, storeCall) (bool, error)
	releaseFn func(context.Context, storeCall) (bool, error)

	acquires  []storeCall
	refreshes []storeCall
	releases  []storeCall
}

func (f *fakeStore) Acquire(ctx context.Context, key, token string, ttl time.Duration) (bool, error) {
	call := storeCall{key: key, token: token, ttl: ttl}
	f.mu.Lock()
	f.acquires = append(f.acquires, call)
	fn := f.acquireFn
	f.mu.Unlock()
	if fn == nil {
		return false, nil
	}

	return fn(ctx, call)
}

func (f *fakeStore) Refresh(ctx context.Context, key, token string, ttl time.Duration) (bool, error) {
	call := storeCall{key: key, token: token, ttl: ttl}
	f.mu.Lock()
	f.refreshes = append(f.refreshes, call)
	fn := f.refreshFn
	f.mu.Unlock()
	if fn == nil {
		return true, nil
	}

	return fn(ctx, call)
}

func (f *fakeStore) Release(ctx context.Context, key, token string) (bool, error) {
	call := storeCall{key: key, token: token}
	f.mu.Lock()
	f.releases = append(f.releases, call)
	fn := f.releaseFn
	f.mu.Unlock()
	if fn == nil {
		return true, nil
	}

	return fn(ctx, call)
}

func (f *fakeStore) calls() (acquires, refreshes, releases []storeCall) {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]storeCall(nil), f.acquires...), append([]storeCall(nil), f.refreshes...), append([]storeCall(nil), f.releases...)
}

func fastOptions() []locker.Option {
	return []locker.Option{
		locker.WithLeaseTTL(200 * time.Millisecond),
		locker.WithRefreshInterval(20 * time.Millisecond),
		locker.WithOperationTimeout(10 * time.Millisecond),
		locker.WithRetryBackoff(time.Millisecond, 5*time.Millisecond),
		locker.WithJitterRatio(0),
	}
}

func newLocker(t *testing.T, store locker.Store, options ...locker.Option) *locker.Locker {
	t.Helper()
	l, err := locker.New(store, options...)
	require.NoError(t, err)

	return l
}

func TestNewValidatesStoreAndOptions(t *testing.T) {
	t.Parallel()

	var typedNil *fakeStore
	_, err := locker.New(typedNil)
	require.Error(t, err)

	store := &fakeStore{}
	tests := map[string]locker.Option{
		"lease TTL":         locker.WithLeaseTTL(0),
		"refresh interval":  locker.WithRefreshInterval(0),
		"operation timeout": locker.WithOperationTimeout(0),
		"refresh window":    locker.WithLeaseTTL(20 * time.Second),
		"retry initial":     locker.WithRetryBackoff(0, time.Second),
		"retry maximum":     locker.WithRetryBackoff(time.Second, time.Millisecond),
		"jitter negative":   locker.WithJitterRatio(-0.1),
		"jitter large":      locker.WithJitterRatio(1.1),
		"jitter NaN":        locker.WithJitterRatio(math.NaN()),
	}
	for name, option := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, optionErr := locker.New(store, option)
			require.Error(t, optionErr)
		})
	}

	valid, err := locker.New(store, nil, locker.WithObserver(nil))
	require.NoError(t, err)
	require.NotNil(t, valid)

	_, err = locker.New(
		store,
		locker.WithLeaseTTL(31*time.Millisecond),
		locker.WithRefreshInterval(10*time.Millisecond),
		locker.WithOperationTimeout(20*time.Millisecond),
	)
	require.Error(t, err, "lease budget must include both Store operation legs")
}

func TestDefaultLeaseAndToken(t *testing.T) {
	t.Parallel()

	store := &fakeStore{acquireFn: func(context.Context, storeCall) (bool, error) { return true, nil }}
	l := newLocker(t, store)
	lease, err := l.Acquire(context.Background(), "orders:42")
	require.NoError(t, err)
	require.Equal(t, "orders:42", lease.Key())

	acquires, _, _ := store.calls()
	require.Len(t, acquires, 1)
	require.Equal(t, time.Minute, acquires[0].ttl)
	require.Len(t, acquires[0].token, 40)
	decoded, err := hex.DecodeString(acquires[0].token)
	require.NoError(t, err)
	require.Len(t, decoded, 20)
	require.NoError(t, lease.Release(context.Background()))
	require.NoError(t, lease.Err())
	select {
	case <-lease.Lost():
		t.Fatal("successful release reported lease loss")
	default:
	}
}

func TestAcquireRetriesUntilContextCancellation(t *testing.T) {
	t.Parallel()

	store := &fakeStore{acquireFn: func(context.Context, storeCall) (bool, error) { return false, nil }}
	l := newLocker(t, store, fastOptions()...)
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Millisecond)
	defer cancel()

	lease, err := l.Acquire(ctx, "busy")
	require.Nil(t, lease)
	require.ErrorIs(t, err, locker.ErrNotAcquired)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	acquires, _, _ := store.calls()
	require.GreaterOrEqual(t, len(acquires), 2)
}

func TestTryAcquireBusyAndSuccess(t *testing.T) {
	t.Parallel()

	var attempts atomic.Int32
	store := &fakeStore{acquireFn: func(context.Context, storeCall) (bool, error) {
		return attempts.Add(1) == 2, nil
	}}
	l := newLocker(t, store)

	lease, acquired, err := l.TryAcquire(context.Background(), "try")
	require.NoError(t, err)
	require.False(t, acquired)
	require.Nil(t, lease)

	lease, acquired, err = l.TryAcquire(context.Background(), "try")
	require.NoError(t, err)
	require.True(t, acquired)
	require.NotNil(t, lease)
	require.NoError(t, lease.Release(context.Background()))
}

func TestAcquireStoreErrorAndOperationTimeoutAreJoined(t *testing.T) {
	t.Parallel()

	boom := errors.New("acquire unavailable")
	store := &fakeStore{acquireFn: func(context.Context, storeCall) (bool, error) { return false, boom }}
	l := newLocker(t, store)
	const sensitiveKey = "customer-secret:acquire:42"
	_, _, err := l.TryAcquire(context.Background(), sensitiveKey)
	require.ErrorIs(t, err, locker.ErrNotAcquired)
	require.ErrorIs(t, err, boom)
	require.NotContains(t, err.Error(), sensitiveKey)

	timeoutStore := &fakeStore{acquireFn: func(ctx context.Context, _ storeCall) (bool, error) {
		<-ctx.Done()
		return false, ctx.Err()
	}}
	timeoutLocker := newLocker(t, timeoutStore, fastOptions()...)
	_, err = timeoutLocker.Acquire(context.Background(), "timeout")
	require.ErrorIs(t, err, locker.ErrNotAcquired)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestKeepaliveRefreshesAndReleasesWithSameToken(t *testing.T) {
	t.Parallel()

	refreshed := make(chan struct{})
	var refreshOnce sync.Once
	store := &fakeStore{
		acquireFn: func(context.Context, storeCall) (bool, error) { return true, nil },
		refreshFn: func(context.Context, storeCall) (bool, error) {
			refreshOnce.Do(func() { close(refreshed) })
			return true, nil
		},
	}
	l := newLocker(t, store, fastOptions()...)
	lease, err := l.Acquire(context.Background(), "keepalive")
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		select {
		case <-refreshed:
			return true
		default:
			return false
		}
	}, time.Second, time.Millisecond)
	require.NoError(t, lease.Release(context.Background()))

	acquires, refreshes, releases := store.calls()
	require.NotEmpty(t, refreshes)
	require.Len(t, releases, 1)
	require.Equal(t, acquires[0].token, refreshes[0].token)
	require.Equal(t, acquires[0].token, releases[0].token)
}

func TestReleaseCancelsAndJoinsInflightRefreshBeforeRelease(t *testing.T) {
	t.Parallel()

	refreshStarted := make(chan struct{})
	refreshStopped := make(chan struct{})
	var refreshOnce sync.Once
	store := &fakeStore{
		acquireFn: func(context.Context, storeCall) (bool, error) { return true, nil },
		refreshFn: func(ctx context.Context, _ storeCall) (bool, error) {
			refreshOnce.Do(func() { close(refreshStarted) })
			<-ctx.Done()
			close(refreshStopped)

			return false, ctx.Err()
		},
		releaseFn: func(context.Context, storeCall) (bool, error) {
			select {
			case <-refreshStopped:
				return true, nil
			default:
				return false, errors.New("release overlapped an in-flight refresh")
			}
		},
	}
	l := newLocker(t, store, fastOptions()...)
	lease, err := l.Acquire(context.Background(), "serialize-refresh-release")
	require.NoError(t, err)
	<-refreshStarted

	require.NoError(t, lease.Release(context.Background()))
}

func TestRefreshFailureMarksLostAndCancelsCallback(t *testing.T) {
	t.Parallel()

	store := &fakeStore{
		acquireFn: func(context.Context, storeCall) (bool, error) { return true, nil },
		refreshFn: func(context.Context, storeCall) (bool, error) { return false, nil },
	}
	l := newLocker(t, store, fastOptions()...)
	callbackStarted := make(chan struct{})

	ran, err := l.WithTryLock(context.Background(), "lost", func(ctx context.Context) error {
		close(callbackStarted)
		<-ctx.Done()
		return ctx.Err()
	})
	require.True(t, ran)
	require.ErrorIs(t, err, locker.ErrLost)
	require.ErrorIs(t, err, locker.ErrNotOwner)
	require.ErrorIs(t, err, context.Canceled)
	select {
	case <-callbackStarted:
	default:
		t.Fatal("callback did not run")
	}
}

func TestRefreshErrorKeepsFirstLossCause(t *testing.T) {
	t.Parallel()

	refreshBoom := errors.New("refresh ambiguous")
	releaseBoom := errors.New("release ambiguous")
	store := &fakeStore{
		acquireFn: func(context.Context, storeCall) (bool, error) { return true, nil },
		refreshFn: func(context.Context, storeCall) (bool, error) { return false, refreshBoom },
		releaseFn: func(context.Context, storeCall) (bool, error) { return false, releaseBoom },
	}
	l := newLocker(t, store, fastOptions()...)
	const sensitiveKey = "customer-secret:refresh:42"
	lease, err := l.Acquire(context.Background(), sensitiveKey)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		select {
		case <-lease.Lost():
			return true
		default:
			return false
		}
	}, time.Second, time.Millisecond)

	require.ErrorIs(t, lease.Err(), locker.ErrLost)
	require.ErrorIs(t, lease.Err(), refreshBoom)
	require.NotErrorIs(t, lease.Err(), releaseBoom)
	require.NotContains(t, lease.Err().Error(), sensitiveKey)
	err = lease.Release(context.Background())
	require.ErrorIs(t, err, refreshBoom)
	require.ErrorIs(t, err, releaseBoom)
	require.NotErrorIs(t, lease.Err(), releaseBoom)
}

func TestReleaseFalseReturnsLostAndNotOwner(t *testing.T) {
	t.Parallel()

	store := &fakeStore{
		acquireFn: func(context.Context, storeCall) (bool, error) { return true, nil },
		releaseFn: func(context.Context, storeCall) (bool, error) { return false, nil },
	}
	l := newLocker(t, store)
	lease, err := l.Acquire(context.Background(), "owner")
	require.NoError(t, err)
	err = lease.Release(context.Background())
	require.ErrorIs(t, err, locker.ErrLost)
	require.ErrorIs(t, err, locker.ErrNotOwner)
	require.Equal(t, 1, strings.Count(err.Error(), locker.ErrNotOwner.Error()))
	require.ErrorIs(t, lease.Err(), locker.ErrLost)
	require.ErrorIs(t, lease.Err(), locker.ErrNotOwner)
	select {
	case <-lease.Lost():
	default:
		t.Fatal("Lost channel is open")
	}
}

func TestReleaseFalseAfterAmbiguousErrorAddsNotOwnerOnce(t *testing.T) {
	t.Parallel()

	boom := errors.New("release connection reset")
	var calls atomic.Int32
	store := &fakeStore{
		acquireFn: func(context.Context, storeCall) (bool, error) { return true, nil },
		releaseFn: func(context.Context, storeCall) (bool, error) {
			if calls.Add(1) == 1 {
				return false, boom
			}

			return false, nil
		},
	}
	l := newLocker(t, store)
	lease, err := l.Acquire(context.Background(), "owner-after-error")
	require.NoError(t, err)

	err = lease.Release(context.Background())
	require.ErrorIs(t, err, locker.ErrLost)
	require.ErrorIs(t, err, boom)

	err = lease.Release(context.Background())
	require.ErrorIs(t, err, locker.ErrLost)
	require.ErrorIs(t, err, locker.ErrNotOwner)
	require.ErrorIs(t, err, boom)
	require.Equal(t, 1, strings.Count(err.Error(), locker.ErrNotOwner.Error()))
	require.EqualValues(t, 2, calls.Load())
}

func TestReleaseErrorAllowsCleanupRetryButLossRemains(t *testing.T) {
	t.Parallel()

	boom := errors.New("release connection reset")
	var calls atomic.Int32
	store := &fakeStore{
		acquireFn: func(context.Context, storeCall) (bool, error) { return true, nil },
		releaseFn: func(context.Context, storeCall) (bool, error) {
			if calls.Add(1) == 1 {
				return false, boom
			}
			return true, nil
		},
	}
	l := newLocker(t, store)
	const sensitiveKey = "customer-secret:release:42"
	lease, err := l.Acquire(context.Background(), sensitiveKey)
	require.NoError(t, err)

	err = lease.Release(context.Background())
	require.ErrorIs(t, err, locker.ErrLost)
	require.ErrorIs(t, err, boom)
	require.NotContains(t, err.Error(), sensitiveKey)
	err = lease.Release(context.Background())
	require.ErrorIs(t, err, locker.ErrLost)
	require.ErrorIs(t, lease.Err(), boom)
	require.ErrorIs(t, lease.Release(context.Background()), locker.ErrInvalidLease)
	require.EqualValues(t, 2, calls.Load())
}

func TestReleaseHonorsCanceledCallerContext(t *testing.T) {
	t.Parallel()

	store := &fakeStore{
		acquireFn: func(context.Context, storeCall) (bool, error) { return true, nil },
		releaseFn: func(ctx context.Context, _ storeCall) (bool, error) {
			<-ctx.Done()

			return false, ctx.Err()
		},
	}
	l := newLocker(t, store, locker.WithOperationTimeout(time.Second))
	lease, err := l.Acquire(context.Background(), "canceled-release")
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	started := time.Now()
	err = lease.Release(ctx)
	require.ErrorIs(t, err, locker.ErrLost)
	require.ErrorIs(t, err, context.Canceled)
	require.Less(t, time.Since(started), 100*time.Millisecond)
}

func TestConcurrentDoubleRelease(t *testing.T) {
	t.Parallel()

	releaseStarted := make(chan struct{})
	releaseContinue := make(chan struct{})
	store := &fakeStore{
		acquireFn: func(context.Context, storeCall) (bool, error) { return true, nil },
		releaseFn: func(context.Context, storeCall) (bool, error) {
			close(releaseStarted)
			<-releaseContinue
			return true, nil
		},
	}
	l := newLocker(t, store)
	lease, err := l.Acquire(context.Background(), "concurrent")
	require.NoError(t, err)

	firstResult := make(chan error, 1)
	go func() { firstResult <- lease.Release(context.Background()) }()
	<-releaseStarted
	require.ErrorIs(t, lease.Release(context.Background()), locker.ErrInvalidLease)
	close(releaseContinue)
	require.NoError(t, <-firstResult)
	require.ErrorIs(t, lease.Release(context.Background()), locker.ErrInvalidLease)
}

func TestWithLockJoinsCallbackAndReleaseErrors(t *testing.T) {
	t.Parallel()

	callbackBoom := errors.New("callback failed")
	releaseBoom := errors.New("release failed")
	store := &fakeStore{
		acquireFn: func(context.Context, storeCall) (bool, error) { return true, nil },
		releaseFn: func(context.Context, storeCall) (bool, error) { return false, releaseBoom },
	}
	l := newLocker(t, store)
	err := l.WithLock(context.Background(), "joined", func(context.Context) error { return callbackBoom })
	require.ErrorIs(t, err, callbackBoom)
	require.ErrorIs(t, err, releaseBoom)
	require.ErrorIs(t, err, locker.ErrLost)
}

func TestCallbackPanicRepanicsOriginalValueAfterCleanup(t *testing.T) {
	t.Parallel()

	type panicMarker struct{ value string }
	marker := &panicMarker{value: "original"}
	var released atomic.Bool
	store := &fakeStore{
		acquireFn: func(context.Context, storeCall) (bool, error) { return true, nil },
		releaseFn: func(context.Context, storeCall) (bool, error) {
			released.Store(true)
			return false, errors.New("cleanup failed")
		},
	}
	l := newLocker(t, store)

	defer func() {
		require.Same(t, marker, recover())
		require.True(t, released.Load())
	}()
	_ = l.WithLock(context.Background(), "panic", func(context.Context) error { panic(marker) })
	t.Fatal("expected panic")
}

func TestCallbackPanicSurvivesCleanupPanic(t *testing.T) {
	t.Parallel()

	callbackPanic := &struct{ value string }{value: "callback"}
	store := &fakeStore{
		acquireFn: func(context.Context, storeCall) (bool, error) { return true, nil },
		releaseFn: func(context.Context, storeCall) (bool, error) { panic("cleanup panic") },
	}
	l := newLocker(t, store)

	defer func() { require.Same(t, callbackPanic, recover()) }()
	_ = l.WithLock(context.Background(), "panic-priority", func(context.Context) error {
		panic(callbackPanic)
	})
	t.Fatal("expected panic")
}

func TestCallbackGoexitStillReleases(t *testing.T) {
	t.Parallel()

	var released atomic.Bool
	store := &fakeStore{
		acquireFn: func(context.Context, storeCall) (bool, error) { return true, nil },
		releaseFn: func(context.Context, storeCall) (bool, error) {
			released.Store(true)

			return true, nil
		},
	}
	l := newLocker(t, store)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = l.WithLock(context.Background(), "goexit-cleanup", func(context.Context) error {
			runtime.Goexit()

			return nil
		})
	}()
	<-done
	require.True(t, released.Load())
}

type eventRecorder struct {
	mu     sync.Mutex
	events []locker.Event
	on     func(locker.Event)
}

func (r *eventRecorder) Observe(_ context.Context, event locker.Event) {
	r.mu.Lock()
	r.events = append(r.events, event)
	on := r.on
	r.mu.Unlock()
	if on != nil {
		on(event)
	}
}

func (r *eventRecorder) types() []locker.EventType {
	r.mu.Lock()
	defer r.mu.Unlock()
	types := make([]locker.EventType, 0, len(r.events))
	for _, event := range r.events {
		types = append(types, event.Type)
	}

	return types
}

func (r *eventRecorder) snapshot() []locker.Event {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]locker.Event(nil), r.events...)
}

func requireEventTypesEventually(t *testing.T, recorder *eventRecorder, want []locker.EventType) {
	t.Helper()
	require.Eventually(t, func() bool {
		return slices.Equal(recorder.types(), want)
	}, time.Second, time.Millisecond)
}

func TestObserverEventOrder(t *testing.T) {
	t.Parallel()

	var acquireCalls atomic.Int32
	refreshed := make(chan struct{})
	var refreshOnce sync.Once
	recorder := &eventRecorder{on: func(event locker.Event) {
		if event.Type == locker.EventRefreshed {
			refreshOnce.Do(func() { close(refreshed) })
		}
	}}
	store := &fakeStore{
		acquireFn: func(context.Context, storeCall) (bool, error) { return acquireCalls.Add(1) > 1, nil },
		refreshFn: func(context.Context, storeCall) (bool, error) { return true, nil },
	}
	options := fastOptions()
	options = append(options, locker.WithObserver(recorder))
	l := newLocker(t, store, options...)
	lease, err := l.Acquire(context.Background(), "events")
	require.NoError(t, err)
	<-refreshed
	require.NoError(t, lease.Release(context.Background()))

	requireEventTypesEventually(t, recorder, []locker.EventType{
		locker.EventAcquireAttempt,
		locker.EventContention,
		locker.EventAcquireAttempt,
		locker.EventAcquired,
		locker.EventRefreshed,
		locker.EventReleased,
	})
	events := recorder.snapshot()
	for _, event := range events {
		require.Equal(t, "events", event.Key)
		require.True(t, event.Blocking)
		require.GreaterOrEqual(t, event.Attempts, 1)
		require.GreaterOrEqual(t, event.Duration, time.Duration(0))
	}
	require.Equal(t, 1, events[0].Attempts)
	require.Equal(t, 1, events[1].Attempts)
	require.Equal(t, 2, events[2].Attempts)
	require.Equal(t, 2, events[3].Attempts)
}

func TestSlowObserverDoesNotDelayKeepaliveOrRelease(t *testing.T) {
	t.Parallel()

	observerEntered := make(chan struct{})
	observerContinue := make(chan struct{})
	var observerOnce sync.Once
	refreshed := make(chan struct{})
	var refreshOnce sync.Once
	recorder := &eventRecorder{on: func(event locker.Event) {
		if event.Type == locker.EventAcquired {
			observerOnce.Do(func() { close(observerEntered) })
			<-observerContinue
		}
	}}
	store := &fakeStore{
		acquireFn: func(context.Context, storeCall) (bool, error) { return true, nil },
		refreshFn: func(context.Context, storeCall) (bool, error) {
			refreshOnce.Do(func() { close(refreshed) })

			return true, nil
		},
	}
	options := fastOptions()
	options = append(options, locker.WithObserver(recorder))
	l := newLocker(t, store, options...)
	type lockResult struct {
		lease *locker.Lease
		err   error
	}
	resultCh := make(chan lockResult, 1)
	go func() {
		lease, err := l.Acquire(context.Background(), "slow-observer")
		resultCh <- lockResult{lease: lease, err: err}
	}()
	<-observerEntered
	var result lockResult
	select {
	case result = <-resultCh:
	case <-time.After(100 * time.Millisecond):
		close(observerContinue)
		<-resultCh
		t.Fatal("Acquire was blocked by observer")
	}
	require.NoError(t, result.err)

	select {
	case <-refreshed:
	case <-time.After(time.Second):
		t.Fatal("keepalive was blocked by observer")
	}
	require.NoError(t, result.lease.Release(context.Background()))
	close(observerContinue)
	require.Eventually(t, func() bool {
		types := recorder.types()

		return len(types) >= 4 && types[len(types)-1] == locker.EventReleased
	}, time.Second, time.Millisecond)
}

func TestObserverCallsAreSerializedAcrossLockerFlows(t *testing.T) {
	observerEntered := make(chan struct{})
	observerConcurrent := make(chan struct{})
	observerContinue := make(chan struct{})
	var (
		active         atomic.Int32
		enteredOnce    sync.Once
		concurrentOnce sync.Once
	)
	observer := locker.ObserverFunc(func(context.Context, locker.Event) {
		if active.Add(1) > 1 {
			concurrentOnce.Do(func() { close(observerConcurrent) })
		}
		enteredOnce.Do(func() { close(observerEntered) })
		<-observerContinue
		active.Add(-1)
	})
	store := &fakeStore{
		acquireFn: func(context.Context, storeCall) (bool, error) { return true, nil },
	}
	l := newLocker(t, store,
		locker.WithLeaseTTL(5*time.Second),
		locker.WithRefreshInterval(2*time.Second),
		locker.WithOperationTimeout(100*time.Millisecond),
		locker.WithObserver(observer),
	)

	first, err := l.Acquire(context.Background(), "observer-flow-1")
	require.NoError(t, err)
	<-observerEntered
	second, err := l.Acquire(context.Background(), "observer-flow-2")
	require.NoError(t, err)

	concurrent := false
	select {
	case <-observerConcurrent:
		concurrent = true
	case <-time.After(50 * time.Millisecond):
	}

	close(observerContinue)
	require.NoError(t, first.Release(context.Background()))
	require.NoError(t, second.Release(context.Background()))
	require.False(t, concurrent, "Observer was called concurrently by one Locker")
}

func TestSuccessfulInflightRefreshIsObservedBeforeConcurrentRelease(t *testing.T) {
	refreshStarted := make(chan struct{})
	var refreshOnce sync.Once
	recorder := &eventRecorder{}
	store := &fakeStore{
		acquireFn: func(context.Context, storeCall) (bool, error) { return true, nil },
		refreshFn: func(ctx context.Context, _ storeCall) (bool, error) {
			refreshOnce.Do(func() { close(refreshStarted) })
			<-ctx.Done()

			return true, nil
		},
	}
	options := fastOptions()
	options = append(options, locker.WithObserver(recorder))
	l := newLocker(t, store, options...)
	lease, err := l.Acquire(context.Background(), "refresh-release-order")
	require.NoError(t, err)
	<-refreshStarted
	require.NoError(t, lease.Release(context.Background()))

	requireEventTypesEventually(t, recorder, []locker.EventType{
		locker.EventAcquireAttempt,
		locker.EventAcquired,
		locker.EventRefreshed,
		locker.EventReleased,
	})
}

func TestObserverRefreshAndReleaseCleanupOrder(t *testing.T) {
	t.Parallel()

	recorder := &eventRecorder{}
	store := &fakeStore{
		acquireFn: func(context.Context, storeCall) (bool, error) { return true, nil },
		refreshFn: func(context.Context, storeCall) (bool, error) { return false, nil },
	}
	options := fastOptions()
	options = append(options, locker.WithObserver(recorder))
	l := newLocker(t, store, options...)
	lease, err := l.Acquire(context.Background(), "refresh-events")
	require.NoError(t, err)
	require.Eventually(t, func() bool { return len(recorder.types()) >= 4 }, time.Second, time.Millisecond)
	require.Equal(t, []locker.EventType{
		locker.EventAcquireAttempt,
		locker.EventAcquired,
		locker.EventRefreshFailed,
		locker.EventLost,
	}, recorder.types())
	require.ErrorIs(t, lease.Release(context.Background()), locker.ErrLost)
	requireEventTypesEventually(t, recorder, []locker.EventType{
		locker.EventAcquireAttempt,
		locker.EventAcquired,
		locker.EventRefreshFailed,
		locker.EventLost,
		locker.EventReleased,
	})
}

func TestObserverLossEventsPrecedeCallbackReleaseUnderBackpressure(t *testing.T) {
	t.Parallel()

	refreshFailedEntered := make(chan struct{})
	refreshFailedContinue := make(chan struct{})
	var refreshFailedOnce sync.Once
	recorder := &eventRecorder{on: func(event locker.Event) {
		if event.Type == locker.EventRefreshFailed {
			refreshFailedOnce.Do(func() { close(refreshFailedEntered) })
			<-refreshFailedContinue
		}
	}}
	store := &fakeStore{
		acquireFn: func(context.Context, storeCall) (bool, error) { return true, nil },
		refreshFn: func(context.Context, storeCall) (bool, error) { return false, nil },
	}
	options := fastOptions()
	options = append(options, locker.WithObserver(recorder))
	l := newLocker(t, store, options...)
	resultCh := make(chan error, 1)
	go func() {
		resultCh <- l.WithLock(context.Background(), "loss-event-order", func(ctx context.Context) error {
			<-ctx.Done()

			return nil
		})
	}()
	<-refreshFailedEntered
	require.ErrorIs(t, <-resultCh, locker.ErrLost)
	close(refreshFailedContinue)
	require.Eventually(t, func() bool { return len(recorder.types()) >= 5 }, time.Second, time.Millisecond)
	require.Equal(t, []locker.EventType{
		locker.EventAcquireAttempt,
		locker.EventAcquired,
		locker.EventRefreshFailed,
		locker.EventLost,
		locker.EventReleased,
	}, recorder.types())
}

func TestObserverFailureOrdersAndPanicIsolation(t *testing.T) {
	t.Parallel()

	t.Run("try lock contention", func(t *testing.T) {
		recorder := &eventRecorder{}
		store := &fakeStore{acquireFn: func(context.Context, storeCall) (bool, error) { return false, nil }}
		l := newLocker(t, store, locker.WithObserver(recorder))
		_, acquired, err := l.TryAcquire(context.Background(), "busy")
		require.NoError(t, err)
		require.False(t, acquired)
		requireEventTypesEventually(t, recorder, []locker.EventType{
			locker.EventAcquireAttempt,
			locker.EventContention,
			locker.EventAcquireFailed,
		})
	})

	t.Run("release not owner", func(t *testing.T) {
		recorder := &eventRecorder{}
		store := &fakeStore{
			acquireFn: func(context.Context, storeCall) (bool, error) { return true, nil },
			releaseFn: func(context.Context, storeCall) (bool, error) { return false, nil },
		}
		l := newLocker(t, store, locker.WithObserver(recorder))
		lease, err := l.Acquire(context.Background(), "release-events")
		require.NoError(t, err)
		require.ErrorIs(t, lease.Release(context.Background()), locker.ErrNotOwner)
		requireEventTypesEventually(t, recorder, []locker.EventType{
			locker.EventAcquireAttempt,
			locker.EventAcquired,
			locker.EventReleaseFailed,
			locker.EventLost,
		})
	})

	t.Run("observer panic", func(t *testing.T) {
		store := &fakeStore{acquireFn: func(context.Context, storeCall) (bool, error) { return true, nil }}
		l := newLocker(t, store, locker.WithObserver(locker.ObserverFunc(func(context.Context, locker.Event) {
			panic("observer bug")
		})))
		lease, err := l.Acquire(context.Background(), "panic-observer")
		require.NoError(t, err)
		require.NoError(t, lease.Release(context.Background()))
	})
}

func TestLeaseNilMethods(t *testing.T) {
	t.Parallel()

	var lease *locker.Lease
	require.Empty(t, lease.Key())
	require.Nil(t, lease.Lost())
	require.ErrorIs(t, lease.Err(), locker.ErrInvalidLease)
	require.ErrorIs(t, lease.Release(context.Background()), locker.ErrInvalidLease)

	zero := &locker.Lease{}
	require.Empty(t, zero.Key())
	require.Nil(t, zero.Lost())
	require.ErrorIs(t, zero.Err(), locker.ErrInvalidLease)
	require.ErrorIs(t, zero.Release(context.Background()), locker.ErrInvalidLease)
}
