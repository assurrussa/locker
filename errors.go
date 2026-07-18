package locker

import "errors"

var (
	// ErrNotAcquired reports that a lease could not be acquired.
	ErrNotAcquired = errors.New("locker: lease not acquired")
	// ErrLost reports that a previously acquired lease is no longer safe to use.
	ErrLost = errors.New("locker: lease lost")
	// ErrInvalidLease reports a nil, incomplete, concurrently releasing, or
	// already released lease.
	ErrInvalidLease = errors.New("locker: invalid lease")
	// ErrNotOwner reports that the stored lease is not owned by the lease token.
	ErrNotOwner = errors.New("locker: not lease owner")
)
