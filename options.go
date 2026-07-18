package locker

import (
	"errors"
	"math"
	"time"
)

const tokenBytes = 20

// Option configures a Locker.
type Option func(*config)

type config struct {
	leaseTTL         time.Duration
	refreshInterval  time.Duration
	operationTimeout time.Duration
	retryBackoff     time.Duration
	retryBackoffMax  time.Duration
	jitterRatio      float64
	observer         Observer
}

func defaultConfig() config {
	return config{
		leaseTTL:         time.Minute,
		refreshInterval:  10 * time.Second,
		operationTimeout: 10 * time.Second,
		retryBackoff:     10 * time.Millisecond,
		retryBackoffMax:  time.Second,
		jitterRatio:      0.20,
	}
}

func (c config) validate() error {
	switch {
	case c.leaseTTL <= 0:
		return errors.New("lease TTL must be positive")
	case c.refreshInterval <= 0:
		return errors.New("refresh interval must be positive")
	case c.operationTimeout <= 0:
		return errors.New("operation timeout must be positive")
	case c.refreshInterval >= c.leaseTTL:
		return errors.New("refresh interval must be less than lease TTL")
	}

	remaining := c.leaseTTL - c.refreshInterval
	if c.operationTimeout >= remaining || c.operationTimeout >= remaining-c.operationTimeout {
		return errors.New("refresh interval plus twice operation timeout must be less than lease TTL")
	}

	switch {
	case c.retryBackoff <= 0:
		return errors.New("initial retry backoff must be positive")
	case c.retryBackoffMax < c.retryBackoff:
		return errors.New("maximum retry backoff must be at least the initial backoff")
	case math.IsNaN(c.jitterRatio) || c.jitterRatio < 0 || c.jitterRatio > 1:
		return errors.New("jitter ratio must be in [0, 1]")
	default:
		return nil
	}
}

// WithLeaseTTL sets the storage lease duration.
func WithLeaseTTL(ttl time.Duration) Option {
	return func(c *config) { c.leaseTTL = ttl }
}

// WithRefreshInterval sets how often an acquired lease is refreshed.
func WithRefreshInterval(interval time.Duration) Option {
	return func(c *config) { c.refreshInterval = interval }
}

// WithOperationTimeout bounds each Store operation.
func WithOperationTimeout(timeout time.Duration) Option {
	return func(c *config) { c.operationTimeout = timeout }
}

// WithRetryBackoff sets the initial and maximum delay between blocking
// acquisition attempts.
func WithRetryBackoff(initial, maximum time.Duration) Option {
	return func(c *config) {
		c.retryBackoff = initial
		c.retryBackoffMax = maximum
	}
}

// WithJitterRatio sets retry jitter as a ratio in the inclusive range [0, 1].
func WithJitterRatio(ratio float64) Option {
	return func(c *config) { c.jitterRatio = ratio }
}

// WithObserver registers a lock lifecycle observer. A nil observer disables
// observations.
func WithObserver(observer Observer) Option {
	return func(c *config) { c.observer = observer }
}
