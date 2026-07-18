# Contributing

Thank you for improving Locker.

## Requirements

- Go 1.24 or newer
- `golangci-lint` 2.x for the lint gate
- two disposable Redis instances for integration tests

## Local checks

Run the non-networked repository gates:

```sh
make verify
```

Run the networked vulnerability gate with the current patched Go toolchain:

```sh
make vulncheck
```

Run Redis integration tests with two addresses:

```sh
LOCKER_REDIS_ADDRS=127.0.0.1:6379,127.0.0.1:6380 make test-integration-race
```

## Compatibility and safety rules

- Keep the supported public surface limited to
  `github.com/assurrussa/locker` and `github.com/assurrussa/locker/redis`.
- Preserve `redis.StorageKey` output. Changing it can split one logical lock
  into independent old/new keys during a rollout.
- Keep sharded resolver mappings deterministic and identical across processes
  for every acquire/refresh/release of a live lease. Topology changes require
  an explicit migration strategy.
- Redis refresh and release must remain atomic and token checked.
- Store implementations must remain concurrency-safe, context-aware, and must
  not shorten the requested TTL.
- Treat every ambiguous refresh/release outcome as permanent lease loss.
- Keep the jittered retry delay positive and bounded by the hard maximum passed
  to `WithRetryBackoff`, including for sub-millisecond configurations.
- Keep explicit ownership on `Lease.Release`; do not add a second public
  release path on `Locker` or compatibility aliases for pre-release names.
- Observer delivery must stay off acquisition, renewal, and release critical
  paths, use one bounded serial dispatcher per `Locker`, and preserve the order
  of delivered events within one lock flow.
- Add tests for lifecycle, cancellation, ownership, error joining, panic, and
  observer ordering when changing the core.
- Do not add fencing, quorum, or failover guarantees to documentation unless
  they are implemented and tested.
- Do not add raw application keys or Redis storage keys to library-generated
  wrapper errors. Custom Store and Resolver errors must be sanitized by their
  implementations.
- Do not commit credentials, private hosts, dashboards, or corporate tooling.

## Pull request checklist

- `go mod tidy -diff`
- `gofmt` produces no diff
- `go vet ./...`
- `go test ./...`
- `go test -race ./...`
- `golangci-lint run --build-tags integration ./...`
- `make vulncheck` on Go 1.26.5 or newer
- `go build ./examples/...`
- integration-race passes against two Redis instances when Redis behavior changes
- README, Russian documentation, changelog, and release notes match public behavior

Do not create a release until publication rights and the correct license notice
have been confirmed.
