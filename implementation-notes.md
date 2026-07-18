# Implementation notes

## 2026-07-18 — v0.1.0 release preparation

- Publication under the MIT License with
  `Copyright (c) 2026 assurrussa` was explicitly confirmed before release.
- Added the canonical license text, synchronized the English and Russian
  public documentation, and replaced the changelog's `Unreleased` marker with
  the release date.
- The resulting release commit is the intended `v0.1.0` tag target after its
  exact GitHub Actions run passes. The clean published-consumer probe remains
  a required post-tag verification.
- Local `make verify` passed. The default Go 1.26.3 vulnerability scan exposed
  two fixed standard-library advisories; the required Go 1.26.5 scan then
  completed with `No vulnerabilities found`, matching the CI security gate.

## Lease-oriented public API decision

- This decision supersedes the earlier pre-release `Handle` and
  `Locker.Acquire`/`TryAcquire`/`Release` surface recorded above. No released consumer
  contract exists, so compatibility aliases would only preserve terminology
  that misrepresents a fallible distributed lease as an in-process mutex.
- The explicit lifecycle is now `Locker.Acquire`, `Locker.TryAcquire`, and
  `Lease.Release`. Ownership stays inside `Lease`, eliminating foreign-locker
  release calls from the public model while preserving concurrent/double
  release rejection and cleanup retry after an ambiguous Store error.
- `WithLock` and `WithTryLock` remain public and are documented as the
  recommended path for ordinary critical sections. Explicit acquisition stays
  available for leader election, component lifecycles, and callers that must
  select on `Lease.Lost()`.
- `ErrInvalidHandle` becomes `ErrInvalidLease`; nil and zero-value leases are
  fail-safe. Error strings use lease terminology, while `errors.Is` remains the
  required inspection mechanism for joined causes.
- The final lease-oriented working tree passed Go 1.24.0 tests, the Go 1.26.3
  tidy/format/vet/unit/race/lint/example gate, fresh gopls diagnostics, and
  integration-race against two temporary Redis 7 instances. The first
  integration invocation was blocked only by sandbox access to the system Go
  cache; the required rerun with repository-local caches passed, and both
  containers were removed.
- Shared wiki health and streams lint passed after the new immutable API
  snapshot; the remaining 12 stale-source warnings belong to unrelated
  platform snapshots.
