# Locker

`locker` is a small Go library for lease-based distributed mutual exclusion.
It separates lifecycle management from storage through a narrow `Store`
interface and ships a token-safe Redis adapter.

Supported import paths:

```text
github.com/assurrussa/locker
github.com/assurrussa/locker/redis
```

Go 1.24 or newer is required.

Russian documentation: [docs/README.ru.md](docs/README.ru.md).

## Quick start with Redis

The Redis adapter accepts the ordinary go-redis client, cluster client, or
ring. It borrows the client and never closes it.

```go
package main

import (
	"context"
	"errors"
	"time"

	"github.com/assurrussa/locker"
	lockerredis "github.com/assurrussa/locker/redis"
	goredis "github.com/redis/go-redis/v9"
)

func run(ctx context.Context) error {
	client := goredis.NewClient(&goredis.Options{Addr: "127.0.0.1:6379"})
	defer client.Close()

	store, err := lockerredis.New(client)
	if err != nil {
		return err
	}
	locks, err := locker.New(store)
	if err != nil {
		return err
	}

	err = locks.WithLock(ctx, "jobs:reindex", func(lockCtx context.Context) error {
		// Work must stop if lockCtx is canceled.
		return reindex(lockCtx)
	})
	if errors.Is(err, locker.ErrLost) {
		// The protected work can no longer be assumed exclusive.
	}
	return err
}

func reindex(context.Context) error { return nil }
```

```sh
go get github.com/assurrussa/locker@v0.1.0
```

## API

The storage boundary is intentionally small:

```go
type Store interface {
	Acquire(context.Context, string, string, time.Duration) (bool, error)
	Refresh(context.Context, string, string, time.Duration) (bool, error)
	Release(context.Context, string, string) (bool, error)
}
```

Store implementations must be safe for concurrent use, honor context
cancellation, and provide leases that are not shorter than the requested TTL.

The public lifecycle API is lease-oriented:

```go
func (*Locker) Acquire(context.Context, string) (*Lease, error)
func (*Locker) TryAcquire(context.Context, string) (*Lease, bool, error)
func (*Lease) Release(context.Context) error
```

- `Acquire` retries with exponential backoff until it acquires the key or its
  context ends. After success, the acquisition context no longer controls the
  lease lifetime.
- `TryAcquire` performs one attempt; contention is `(nil, false, nil)`.
- `Lease.Release` performs a token-checked release and respects its caller
  context.
- `WithLock` and `WithTryLock` cancel the callback context if the lease is
  lost, then run cleanup on a detached context bounded by the operation timeout
  and join callback and cleanup errors with `errors.Join`.
- A callback panic triggers best-effort cleanup and then re-panics with the
  original panic value unchanged, even if cleanup also panics. Deferred cleanup
  also runs when a callback exits its goroutine with `runtime.Goexit`.

`WithLock` and `WithTryLock` are recommended for ordinary critical sections.
Use `Acquire` or `TryAcquire` when the lease must integrate with a longer-lived
component lifecycle or when the caller needs to select on `Lease.Lost()`.

The public sentinel errors are `ErrNotAcquired`, `ErrLost`,
`ErrInvalidLease`, and `ErrNotOwner`. Always inspect returned errors with
`errors.Is`; one result can preserve several causes.

## Lease and loss semantics

An acquired `*Lease` exposes:

```go
lease.Key()  // raw application key
lease.Lost() // channel closed once on permanent lease loss
lease.Err()  // ErrLost joined with the first loss cause
```

`Lost()` and `Err()` report unexpected ownership loss. A successful
`Release()` does not close `Lost()`, and `Err()` remains nil.

The lease must not be copied. Any ambiguous refresh or release error makes it
permanently unsafe.
`Refresh=false` also means ownership is lost. `Release=false` returns an error
satisfying both `ErrLost` and `ErrNotOwner`.

When release returns an error, another `lease.Release` is allowed only as a
cleanup attempt. A successful cleanup does not restore safety and still
returns `ErrLost`. Ownership is encapsulated by the lease, so callers do not
pass it back to a `Locker`; nil, incomplete, concurrent, and already released
leases return `ErrInvalidLease`.

## Configuration

Defaults:

- lease TTL: 1 minute (`WithLeaseTTL`)
- refresh interval: 10 seconds (`WithRefreshInterval`)
- per-operation timeout: 10 seconds (`WithOperationTimeout`)
- retry backoff: nominally 10 milliseconds initially, with a hard 1-second
  maximum (`WithRetryBackoff`)
- retry jitter: 20%, bounded by a positive delay and the configured hard
  maximum (`WithJitterRatio`)
- ownership token: 20 cryptographically random bytes

The refresh interval plus twice the operation timeout must be less than the
lease TTL. The extra operation budget covers TTL time already consumed before
an acquire or refresh response reaches the caller. The caller's context limits
the total blocking acquisition time.

## Observer

Use `WithObserver` with an `Observer` or `ObserverFunc` to receive acquisition,
contention, refresh, release, failure, and loss events.

```go
observer := locker.ObserverFunc(func(ctx context.Context, event locker.Event) {
	// Export a bounded metric or trace event quickly.
})
```

Observer delivery is asynchronous and never blocks acquisition, refresh, or
release. Calls emitted by one `Locker` are serialized through one bounded
best-effort queue; the same observer shared by separate `Locker` instances may
still be called concurrently. Delivered events within one lock flow preserve
order. Routine attempt, contention, and successful-refresh events may be
dropped when an observer applies sustained backpressure. Observers must still
return quickly. A panic is recovered and cannot break lock lifecycle.
`Event.Key` is the raw application key: it may be sensitive and
high-cardinality, so never use it as an uncontrolled metric label.
Library-generated wrapper errors omit raw and Redis storage keys. Custom
`Store` and Redis `Resolver` implementations remain responsible for sanitizing
the errors they return.

## Sharded Redis

`redis.NewSharded` receives a context-aware resolver. The resolver gets the
computed storage key, not the raw key:

```go
store, err := lockerredis.NewSharded(func(
	ctx context.Context,
	storageKey string,
) (lockerredis.Client, error) {
	return shardFor(storageKey)
})
```

The resolver must map each storage key deterministically and identically in
every process for the complete lease lifetime. Acquire, refresh, and release
are routed independently; changing the mapping while leases are live can send
later operations to another shard and create two independent locks for one
logical resource. Shard topology changes therefore need an explicit migration
strategy that preserves routing for live keys.

`redis.StorageKey` intentionally preserves the previous format:

```text
locker:<first-six-runes-or-none>:<16-character-xxhash64>
```

This prevents old and new deployments from creating independent locks for the
same raw key. Acquire uses `SET ... NX` with a TTL; refresh and release use Lua
scripts that compare the ownership token atomically. TTL fractions are rounded
up to Redis millisecond precision so the stored lease is never shorter than
requested.

## Safety boundary

This library implements a single-store lease mutex. It is not reentrant and
does not guarantee fairness. `v0.1` does **not** provide fencing tokens,
quorum/Redlock semantics, or protection from split-brain during Redis failover.
If stale writers are unacceptable, enforce fencing or another monotonic guard
in the protected resource itself.

Callbacks must react to context cancellation. No library can stop a callback
that ignores its context after the lease has been lost.

## Development

```sh
make verify
```

Integration tests require two disposable Redis instances:

```sh
LOCKER_REDIS_ADDRS=127.0.0.1:6379,127.0.0.1:6380 make test-integration-race
```

See [CONTRIBUTING.md](CONTRIBUTING.md). The published-consumer
probe is intentionally a post-tag gate:

```sh
scripts/check-published-consumer.sh v0.1.0
```

## License

Locker is available under the [MIT License](LICENSE).
