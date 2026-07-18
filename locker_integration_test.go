//go:build integration

package locker_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/assurrussa/locker"
	"github.com/assurrussa/locker/internal/testutil/integrationredis"
	lockerredis "github.com/assurrussa/locker/redis"
	"github.com/stretchr/testify/require"
)

func integrationPool(t *testing.T) *integrationredis.Pool {
	t.Helper()
	pool, err := integrationredis.NewFromEnv(context.Background())
	if errors.Is(err, integrationredis.ErrNotConfigured) {
		t.Skip(err)
	}
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, pool.Close()) })

	return pool
}

func integrationLocker(t *testing.T, pool *integrationredis.Pool) (*locker.Locker, *lockerredis.Store) {
	t.Helper()
	store, err := lockerredis.NewSharded(pool.Resolve)
	require.NoError(t, err)
	l, err := locker.New(
		store,
		locker.WithLeaseTTL(2*time.Second),
		locker.WithRefreshInterval(250*time.Millisecond),
		locker.WithOperationTimeout(250*time.Millisecond),
		locker.WithRetryBackoff(time.Millisecond, 20*time.Millisecond),
		locker.WithJitterRatio(0.1),
	)
	require.NoError(t, err)

	return l, store
}

func TestIntegrationMutualExclusionAndReacquire(t *testing.T) {
	pool := integrationPool(t)
	l, _ := integrationLocker(t, pool)
	key := fmt.Sprintf("integration:mutex:%d", time.Now().UnixNano())

	const workers = 12
	var active atomic.Int32
	var maximum atomic.Int32
	var waitGroup sync.WaitGroup
	errorsCh := make(chan error, workers)
	for range workers {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			err := l.WithLock(context.Background(), key, func(context.Context) error {
				current := active.Add(1)
				for {
					seen := maximum.Load()
					if current <= seen || maximum.CompareAndSwap(seen, current) {
						break
					}
				}
				time.Sleep(3 * time.Millisecond)
				active.Add(-1)
				return nil
			})
			if err != nil {
				errorsCh <- err
			}
		}()
	}
	waitGroup.Wait()
	close(errorsCh)
	for err := range errorsCh {
		require.NoError(t, err)
	}
	require.EqualValues(t, 1, maximum.Load())

	lease, acquired, err := l.TryAcquire(context.Background(), key)
	require.NoError(t, err)
	require.True(t, acquired)
	require.NoError(t, lease.Release(context.Background()))
}

func TestIntegrationLeaseLossAndWrongToken(t *testing.T) {
	pool := integrationPool(t)
	l, store := integrationLocker(t, pool)
	key := fmt.Sprintf("integration:loss:%d", time.Now().UnixNano())
	lease, err := l.Acquire(context.Background(), key)
	require.NoError(t, err)

	storageKey := lockerredis.StorageKey(key)
	client := pool.Client(pool.Index(storageKey))
	require.NoError(t, client.Del(context.Background(), storageKey).Err())
	require.Eventually(t, func() bool {
		select {
		case <-lease.Lost():
			return true
		default:
			return false
		}
	}, 2*time.Second, 10*time.Millisecond)
	require.ErrorIs(t, lease.Err(), locker.ErrLost)
	require.ErrorIs(t, lease.Err(), locker.ErrNotOwner)
	require.ErrorIs(t, lease.Release(context.Background()), locker.ErrNotOwner)

	wrongKey := fmt.Sprintf("integration:token:%d", time.Now().UnixNano())
	acquired, err := store.Acquire(context.Background(), wrongKey, "right-token", time.Second)
	require.NoError(t, err)
	require.True(t, acquired)
	refreshed, err := store.Refresh(context.Background(), wrongKey, "wrong-token", time.Second)
	require.NoError(t, err)
	require.False(t, refreshed)
	released, err := store.Release(context.Background(), wrongKey, "wrong-token")
	require.NoError(t, err)
	require.False(t, released)
	released, err = store.Release(context.Background(), wrongKey, "right-token")
	require.NoError(t, err)
	require.True(t, released)
}

func TestIntegrationShardedRoutingUsesTwoRedisInstances(t *testing.T) {
	pool := integrationPool(t)
	_, store := integrationLocker(t, pool)

	keys := make(map[int]string)
	for candidate := 0; len(keys) < 2 && candidate < 10_000; candidate++ {
		raw := fmt.Sprintf("integration:shard:%d:%d", time.Now().UnixNano(), candidate)
		index := pool.Index(lockerredis.StorageKey(raw))
		keys[index] = raw
	}
	require.Len(t, keys, 2)

	for index, raw := range keys {
		acquired, err := store.Acquire(context.Background(), raw, "route-token", time.Second)
		require.NoError(t, err)
		require.True(t, acquired)
		storageKey := lockerredis.StorageKey(raw)
		exists, err := pool.Client(index).Exists(context.Background(), storageKey).Result()
		require.NoError(t, err)
		require.EqualValues(t, 1, exists)
		for other := 0; other < pool.Len(); other++ {
			if other == index {
				continue
			}
			exists, err = pool.Client(other).Exists(context.Background(), storageKey).Result()
			require.NoError(t, err)
			require.Zero(t, exists)
		}
		released, err := store.Release(context.Background(), raw, "route-token")
		require.NoError(t, err)
		require.True(t, released)
	}
}
