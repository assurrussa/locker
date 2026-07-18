// Package locker implements lease-based distributed mutual exclusion over a
// pluggable Store.
//
// Locker is not reentrant or fair. Version v0.1 does not provide fencing
// tokens, quorum/Redlock semantics, or protection from split-brain caused by a
// storage failover. Code running under a lease must stop when its context is
// canceled or Lease.Lost is closed. Callback helpers are the recommended API;
// Acquire and TryAcquire support explicit lifecycle integration.
package locker
