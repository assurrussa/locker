# Implementation notes

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
