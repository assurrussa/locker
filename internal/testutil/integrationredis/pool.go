//go:build integration

package integrationredis

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	lockerredis "github.com/assurrussa/locker/redis"
	"github.com/cespare/xxhash/v2"
	goredis "github.com/redis/go-redis/v9"
)

// ErrNotConfigured reports that LOCKER_REDIS_ADDRS is absent or incomplete.
var ErrNotConfigured = errors.New("redis integration environment is not configured")

// Pool owns Redis clients created for integration tests.
type Pool struct {
	clients []*goredis.Client
}

// NewFromEnv connects to the comma-separated LOCKER_REDIS_ADDRS list.
func NewFromEnv(ctx context.Context) (*Pool, error) {
	parts := strings.Split(os.Getenv("LOCKER_REDIS_ADDRS"), ",")
	addresses := make([]string, 0, len(parts))
	for _, part := range parts {
		if address := strings.TrimSpace(part); address != "" {
			addresses = append(addresses, address)
		}
	}
	if len(addresses) < 2 {
		return nil, fmt.Errorf("%w: LOCKER_REDIS_ADDRS must contain at least two addresses", ErrNotConfigured)
	}

	database := 0
	if rawDatabase := strings.TrimSpace(os.Getenv("LOCKER_REDIS_DB")); rawDatabase != "" {
		parsed, err := strconv.Atoi(rawDatabase)
		if err != nil {
			return nil, fmt.Errorf("invalid LOCKER_REDIS_DB: %w", err)
		}
		database = parsed
	}

	pool := &Pool{clients: make([]*goredis.Client, 0, len(addresses))}
	for _, address := range addresses {
		client := goredis.NewClient(&goredis.Options{
			Addr:        address,
			Password:    os.Getenv("LOCKER_REDIS_PASSWORD"),
			DB:          database,
			DialTimeout: 2 * time.Second,
		})
		pingCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		err := client.Ping(pingCtx).Err()
		cancel()
		if err != nil {
			_ = client.Close()
			_ = pool.Close()
			return nil, fmt.Errorf("ping Redis %q: %w", address, err)
		}
		pool.clients = append(pool.clients, client)
	}

	return pool, nil
}

// Len returns the number of connected Redis instances.
func (p *Pool) Len() int {
	if p == nil {
		return 0
	}

	return len(p.clients)
}

// Index returns the deterministic shard index for a storage key.
func (p *Pool) Index(storageKey string) int {
	return int(xxhash.Sum64String(storageKey) % uint64(len(p.clients)))
}

// Client returns a Redis client by shard index.
func (p *Pool) Client(index int) *goredis.Client {
	return p.clients[index]
}

// Resolve implements redis.Resolver for the test pool.
func (p *Pool) Resolve(_ context.Context, storageKey string) (lockerredis.Client, error) {
	if p == nil || len(p.clients) == 0 {
		return nil, errors.New("redis integration pool is empty")
	}

	return p.clients[p.Index(storageKey)], nil
}

// Close closes the integration-test clients. Production adapters do not call it.
func (p *Pool) Close() error {
	if p == nil {
		return nil
	}

	var result error
	for _, client := range p.clients {
		result = errors.Join(result, client.Close())
	}

	return result
}
