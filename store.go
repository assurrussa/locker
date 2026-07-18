package locker

import (
	"context"
	"time"
)

// Store provides the atomic storage operations required by Locker.
//
// Acquire must create a lease only when key does not already exist. Refresh and
// Release must succeed only when token still owns the lease. A false result is
// an expected contention or ownership result; an error means the outcome may
// be ambiguous. Implementations must be safe for concurrent use, including
// concurrent operations for the same key, must honor context cancellation, and
// must not create a lease shorter than the requested TTL.
type Store interface {
	Acquire(ctx context.Context, key, token string, ttl time.Duration) (bool, error)
	Refresh(ctx context.Context, key, token string, ttl time.Duration) (bool, error)
	Release(ctx context.Context, key, token string) (bool, error)
}
