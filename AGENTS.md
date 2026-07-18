# Repository Guidelines

## Project Structure & Module Organization

The module root (`github.com/assurrussa/locker`) contains the public lease API:
`locker.go`, `lease.go`, `store.go`, `options.go`, `observer.go`, and
`errors.go`. The `redis/` package provides the token-checked Redis adapter.
Runnable usage lives in `examples/simple/`. Unit tests are colocated with their
packages; Redis integration tests are in `locker_integration_test.go`, with
helpers under `internal/testutil/integrationredis/`. Keep English documentation
in `README.md`, Russian documentation in `docs/README.ru.md`, and contributor
and release checks in `CONTRIBUTING.md`.

## Build, Test, and Development Commands

- `make verify` runs tidy and formatting checks, vet, unit tests, race tests,
  golangci-lint, and example builds. Run it before every PR.
- `make test` or `go test ./...` runs the normal test suite.
- `go test ./... -run TestRelease -count=1` runs a focused regression test.
- `LOCKER_REDIS_ADDRS=127.0.0.1:6379,127.0.0.1:6380 make test-integration-race`
  runs integration tests against two disposable Redis instances.
- `make vulncheck` runs the pinned `govulncheck` security gate.

## Coding Style & Naming Conventions

Target Go 1.24 or newer. Use tabs and `gofmt`; `make fmt-check` must produce no
output. Follow idiomatic Go naming: exported identifiers use `MixedCaps` and
have concise GoDoc, package names are lowercase, and sentinel errors use
`ErrName`. Inspect joined errors with `errors.Is`. Preserve the supported root
and `/redis` imports, token-checked lease semantics, and `redis.StorageKey`
compatibility unless an explicit contract change is approved.

## Testing Guidelines

Tests use Go's `testing` package and `testify/require`. Name tests `TestXxx` and
prefer table-driven cases where inputs share one contract. Use external test
packages for public behavior and same-package tests only for unexported
mechanics. Add regression coverage for lifecycle, cancellation, ownership,
error joining, observer ordering, and Redis TTL/token behavior. Integration
tests require the `integration` build tag; there is no fixed coverage target.

## Commit & Pull Request Guidelines

Without an issue, use a specific imperative subject. Do not reuse an unrelated
historical message. PRs should describe behavioral and safety impact, link the
issue, list validation run, and update README, Russian docs, and the changelog
when public behavior changes. Redis changes require integration-race evidence.

## Security & Release Safety

Never commit credentials, private hosts, dashboards, or sensitive raw/storage
keys. Keep observer labels bounded and sanitize custom Store/Resolver errors.
Do not tag or publish until publication rights and the correct license notice
are confirmed.
