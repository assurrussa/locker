package redis_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	lockerredis "github.com/assurrussa/locker/redis"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

type setCall struct {
	key   string
	value any
	args  goredis.SetArgs
}

type evalCall struct {
	script string
	keys   []string
	args   []any
}

type fakeClient struct {
	mu sync.Mutex

	setResult  string
	setErr     error
	evalResult any
	evalErr    error
	sets       []setCall
	evals      []evalCall
}

func (f *fakeClient) SetArgs(_ context.Context, key string, value any, args goredis.SetArgs) *goredis.StatusCmd {
	f.mu.Lock()
	f.sets = append(f.sets, setCall{key: key, value: value, args: args})
	result, err := f.setResult, f.setErr
	f.mu.Unlock()

	return goredis.NewStatusResult(result, err)
}

func (f *fakeClient) Eval(_ context.Context, script string, keys []string, args ...any) *goredis.Cmd {
	f.mu.Lock()
	f.evals = append(f.evals, evalCall{script: script, keys: append([]string(nil), keys...), args: append([]any(nil), args...)})
	result, err := f.evalResult, f.evalErr
	f.mu.Unlock()

	return goredis.NewCmdResult(result, err)
}

func (f *fakeClient) calls() ([]setCall, []evalCall) {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]setCall(nil), f.sets...), append([]evalCall(nil), f.evals...)
}

func TestConstructorsValidateDependencies(t *testing.T) {
	t.Parallel()

	var typedNil *fakeClient
	_, err := lockerredis.New(typedNil)
	require.Error(t, err)
	_, err = lockerredis.NewSharded(nil)
	require.Error(t, err)

	store, err := lockerredis.New(&fakeClient{})
	require.NoError(t, err)
	require.NotNil(t, store)
}

func TestAcquireUsesNXTTLAndStableStorageKey(t *testing.T) {
	t.Parallel()

	client := &fakeClient{setResult: "OK"}
	store, err := lockerredis.New(client)
	require.NoError(t, err)

	acquired, err := store.Acquire(context.Background(), "update_configs", "token", 1500*time.Millisecond)
	require.NoError(t, err)
	require.True(t, acquired)
	sets, _ := client.calls()
	require.Len(t, sets, 1)
	require.Equal(t, lockerredis.StorageKey("update_configs"), sets[0].key)
	require.Equal(t, "token", sets[0].value)
	require.Equal(t, "NX", sets[0].args.Mode)
	require.Equal(t, 1500*time.Millisecond, sets[0].args.TTL)
}

func TestAcquireContentionAndError(t *testing.T) {
	t.Parallel()

	client := &fakeClient{setErr: goredis.Nil}
	store, err := lockerredis.New(client)
	require.NoError(t, err)
	const sensitiveKey = "customer-secret:redis-acquire:42"
	acquired, err := store.Acquire(context.Background(), sensitiveKey, "token", time.Second)
	require.NoError(t, err)
	require.False(t, acquired)

	boom := errors.New("redis unavailable")
	client.setErr = boom
	_, err = store.Acquire(context.Background(), sensitiveKey, "token", time.Second)
	require.ErrorIs(t, err, boom)
	require.NotContains(t, err.Error(), sensitiveKey)
	require.NotContains(t, err.Error(), lockerredis.StorageKey(sensitiveKey))
}

func TestRefreshAndReleaseAreTokenChecked(t *testing.T) {
	t.Parallel()

	client := &fakeClient{evalResult: int64(1)}
	store, err := lockerredis.New(client)
	require.NoError(t, err)

	refreshed, err := store.Refresh(context.Background(), "lease", "owner-token", 2500*time.Millisecond)
	require.NoError(t, err)
	require.True(t, refreshed)
	released, err := store.Release(context.Background(), "lease", "owner-token")
	require.NoError(t, err)
	require.True(t, released)

	_, evals := client.calls()
	require.Len(t, evals, 2)
	storageKey := lockerredis.StorageKey("lease")
	require.Equal(t, []string{storageKey}, evals[0].keys)
	require.Contains(t, evals[0].script, "pexpire")
	require.Equal(t, []any{"owner-token", int64(2500)}, evals[0].args)
	require.Equal(t, []string{storageKey}, evals[1].keys)
	require.Contains(t, evals[1].script, "del")
	require.Equal(t, []any{"owner-token"}, evals[1].args)
}

func TestTTLRoundsUpToRedisMilliseconds(t *testing.T) {
	t.Parallel()

	client := &fakeClient{setResult: "OK", evalResult: int64(1)}
	store, err := lockerredis.New(client)
	require.NoError(t, err)

	const ttl = 1500 * time.Microsecond
	acquired, err := store.Acquire(context.Background(), "fractional-ttl", "token", ttl)
	require.NoError(t, err)
	require.True(t, acquired)
	refreshed, err := store.Refresh(context.Background(), "fractional-ttl", "token", ttl)
	require.NoError(t, err)
	require.True(t, refreshed)

	sets, evals := client.calls()
	require.Len(t, sets, 1)
	require.Equal(t, 2*time.Millisecond, sets[0].args.TTL)
	require.Len(t, evals, 1)
	require.Equal(t, []any{"token", int64(2)}, evals[0].args)
}

func TestRefreshAndReleaseFalseAndErrors(t *testing.T) {
	t.Parallel()

	client := &fakeClient{evalResult: int64(0)}
	store, err := lockerredis.New(client)
	require.NoError(t, err)
	const sensitiveKey = "customer-secret:redis-lifecycle:42"
	refreshed, err := store.Refresh(context.Background(), sensitiveKey, "wrong", time.Second)
	require.NoError(t, err)
	require.False(t, refreshed)
	released, err := store.Release(context.Background(), sensitiveKey, "wrong")
	require.NoError(t, err)
	require.False(t, released)

	boom := errors.New("eval failed")
	client.evalErr = boom
	_, err = store.Refresh(context.Background(), sensitiveKey, "wrong", time.Second)
	require.ErrorIs(t, err, boom)
	require.NotContains(t, err.Error(), lockerredis.StorageKey(sensitiveKey))
	_, err = store.Release(context.Background(), sensitiveKey, "wrong")
	require.ErrorIs(t, err, boom)
	require.NotContains(t, err.Error(), lockerredis.StorageKey(sensitiveKey))
}

func TestTTLAndTokenValidationPrecedesRouting(t *testing.T) {
	t.Parallel()

	var routed atomicCounter
	store, err := lockerredis.NewSharded(func(context.Context, string) (lockerredis.Client, error) {
		routed.Increment()
		return &fakeClient{}, nil
	})
	require.NoError(t, err)

	_, err = store.Acquire(context.Background(), "key", "token", 0)
	require.Error(t, err)
	_, err = store.Refresh(context.Background(), "key", "token", time.Nanosecond)
	require.Error(t, err)
	_, err = store.Acquire(context.Background(), "key", "", time.Second)
	require.Error(t, err)
	_, err = store.Release(context.Background(), "key", "")
	require.Error(t, err)
	require.Zero(t, routed.Value())
}

type atomicCounter struct {
	mu    sync.Mutex
	value int
}

func (c *atomicCounter) Increment() {
	c.mu.Lock()
	c.value++
	c.mu.Unlock()
}

func (c *atomicCounter) Value() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.value
}

func TestShardedResolverReceivesStorageKey(t *testing.T) {
	t.Parallel()

	client := &fakeClient{setResult: "OK", evalResult: int64(1)}
	var mu sync.Mutex
	var routed []string
	store, err := lockerredis.NewSharded(func(_ context.Context, storageKey string) (lockerredis.Client, error) {
		mu.Lock()
		routed = append(routed, storageKey)
		mu.Unlock()
		return client, nil
	})
	require.NoError(t, err)

	_, err = store.Acquire(context.Background(), "шардированный", "token", time.Second)
	require.NoError(t, err)
	_, err = store.Refresh(context.Background(), "шардированный", "token", time.Second)
	require.NoError(t, err)
	_, err = store.Release(context.Background(), "шардированный", "token")
	require.NoError(t, err)

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []string{
		lockerredis.StorageKey("шардированный"),
		lockerredis.StorageKey("шардированный"),
		lockerredis.StorageKey("шардированный"),
	}, routed)
}

func TestResolverErrorsAndNilClients(t *testing.T) {
	t.Parallel()

	boom := errors.New("no shard")
	store, err := lockerredis.NewSharded(func(context.Context, string) (lockerredis.Client, error) {
		return nil, boom
	})
	require.NoError(t, err)
	const sensitiveKey = "customer-secret:resolver:42"
	_, err = store.Acquire(context.Background(), sensitiveKey, "token", time.Second)
	require.ErrorIs(t, err, boom)
	require.NotContains(t, err.Error(), sensitiveKey)
	require.NotContains(t, err.Error(), lockerredis.StorageKey(sensitiveKey))

	store, err = lockerredis.NewSharded(func(context.Context, string) (lockerredis.Client, error) {
		return nil, nil
	})
	require.NoError(t, err)
	_, err = store.Release(context.Background(), "key", "token")
	require.Error(t, err)
}

func TestStorageKeyGoldenCompatibility(t *testing.T) {
	t.Parallel()

	tests := map[string]string{
		"":                    "locker:none:ef46db3751d8e999",
		"abc":                 "locker:abc:44bc2cf5ad770999",
		"update_configs":      "locker:update:becfe150a2e0c205",
		"cleaner_multi_items": "locker:cleane:fa70a67c3705ef8d",
		"Привет-мир":          "locker:Привет:ff4a5230385ba7ae",
	}
	for raw, want := range tests {
		if got := lockerredis.StorageKey(raw); got != want {
			t.Errorf("raw key %q: got %q, want %q", raw, got, want)
		}
	}
}
