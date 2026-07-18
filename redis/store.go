package redis

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/assurrussa/locker"
	goredis "github.com/redis/go-redis/v9"
)

const (
	refreshScript = `if redis.call('get', KEYS[1]) == ARGV[1] then
	return redis.call('pexpire', KEYS[1], ARGV[2])
else
	return 0
end`

	releaseScript = `if redis.call('get', KEYS[1]) == ARGV[1] then
	return redis.call('del', KEYS[1])
else
	return 0
end`
)

// Client is the narrow go-redis API used by Store. *redis.Client,
// *redis.ClusterClient, and *redis.Ring implement it.
type Client interface {
	SetArgs(ctx context.Context, key string, value any, args goredis.SetArgs) *goredis.StatusCmd
	Eval(ctx context.Context, script string, keys []string, args ...any) *goredis.Cmd
}

// Resolver selects a Redis client for an already computed storage key. Locker
// may call a resolver concurrently, including for the same storage key. The
// mapping must be deterministic and identical in every process for the entire
// lease lifetime. Remapping a live storage key can create an independent lock
// on another shard.
type Resolver func(ctx context.Context, storageKey string) (Client, error)

// Store implements locker.Store with token-checked Redis operations. Store
// does not own or close clients.
type Store struct {
	resolve Resolver
}

var (
	_ locker.Store = (*Store)(nil)
	_ Client       = (*goredis.Client)(nil)
	_ Client       = (*goredis.ClusterClient)(nil)
	_ Client       = (*goredis.Ring)(nil)
)

// New creates a Store that sends every operation to client.
func New(client Client) (*Store, error) {
	if nilClient(client) {
		return nil, errors.New("locker/redis: client must not be nil")
	}

	return NewSharded(func(context.Context, string) (Client, error) { return client, nil })
}

// NewSharded creates a Store whose resolver selects a client from the computed
// Redis storage key. Callers must keep that mapping stable across processes and
// lease operations, including during shard-topology migrations.
func NewSharded(resolver Resolver) (*Store, error) {
	if resolver == nil {
		return nil, errors.New("locker/redis: resolver must not be nil")
	}

	return &Store{resolve: resolver}, nil
}

// Acquire creates a TTL lease with Redis NX semantics. Sub-millisecond TTL
// fractions are rounded up to Redis millisecond precision.
func (s *Store) Acquire(ctx context.Context, key, token string, ttl time.Duration) (bool, error) {
	redisTTL, _, err := normalizeTokenTTL(token, ttl)
	if err != nil {
		return false, err
	}

	storageKey := StorageKey(key)
	client, err := s.client(ctx, storageKey)
	if err != nil {
		return false, err
	}

	result, err := client.SetArgs(ctx, storageKey, token, goredis.SetArgs{Mode: "NX", TTL: redisTTL}).Result()
	if errors.Is(err, goredis.Nil) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("locker/redis: acquire: %w", err)
	}

	return result == "OK", nil
}

// Refresh extends a lease only when token still owns it. Sub-millisecond TTL
// fractions are rounded up to Redis millisecond precision.
func (s *Store) Refresh(ctx context.Context, key, token string, ttl time.Duration) (bool, error) {
	_, ttlMilliseconds, err := normalizeTokenTTL(token, ttl)
	if err != nil {
		return false, err
	}

	storageKey := StorageKey(key)
	client, err := s.client(ctx, storageKey)
	if err != nil {
		return false, err
	}

	result, err := client.Eval(ctx, refreshScript, []string{storageKey}, token, ttlMilliseconds).Int64()
	if errors.Is(err, goredis.Nil) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("locker/redis: refresh: %w", err)
	}

	return result == 1, nil
}

// Release deletes a lease only when token still owns it.
func (s *Store) Release(ctx context.Context, key, token string) (bool, error) {
	if token == "" {
		return false, errors.New("locker/redis: token must not be empty")
	}

	storageKey := StorageKey(key)
	client, err := s.client(ctx, storageKey)
	if err != nil {
		return false, err
	}

	result, err := client.Eval(ctx, releaseScript, []string{storageKey}, token).Int64()
	if errors.Is(err, goredis.Nil) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("locker/redis: release: %w", err)
	}

	return result == 1, nil
}

func (s *Store) client(ctx context.Context, storageKey string) (Client, error) {
	if s == nil || s.resolve == nil {
		return nil, errors.New("locker/redis: store is nil or uninitialized")
	}

	client, err := s.resolve(ctx, storageKey)
	if err != nil {
		return nil, fmt.Errorf("locker/redis: resolve: %w", err)
	}
	if nilClient(client) {
		return nil, errors.New("locker/redis: resolver returned nil client")
	}

	return client, nil
}

func normalizeTokenTTL(token string, ttl time.Duration) (time.Duration, int64, error) {
	if token == "" {
		return 0, 0, errors.New("locker/redis: token must not be empty")
	}
	if ttl < time.Millisecond {
		return 0, 0, errors.New("locker/redis: TTL must be at least one millisecond")
	}

	if remainder := ttl % time.Millisecond; remainder != 0 {
		rounding := time.Millisecond - remainder
		const maxDuration = time.Duration(1<<63 - 1)
		if ttl > maxDuration-rounding {
			return 0, 0, errors.New("locker/redis: TTL is too large to round to milliseconds")
		}
		ttl += rounding
	}

	return ttl, ttl.Milliseconds(), nil
}

func nilClient(client Client) bool {
	if client == nil {
		return true
	}

	value := reflect.ValueOf(client)
	return value.Kind() == reflect.Pointer && value.IsNil()
}
