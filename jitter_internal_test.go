package locker

import (
	"testing"
	"time"
)

func TestJitterBoundsRespectConfiguredMaximum(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		base      time.Duration
		ratio     float64
		maximum   time.Duration
		wantLower time.Duration
		wantUpper time.Duration
	}{
		{
			name: "default initial",
			base: 10 * time.Millisecond, ratio: 0.2, maximum: time.Second,
			wantLower: 8 * time.Millisecond, wantUpper: 12 * time.Millisecond,
		},
		{
			name: "default maximum",
			base: time.Second, ratio: 0.2, maximum: time.Second,
			wantLower: 800 * time.Millisecond, wantUpper: time.Second,
		},
		{
			name: "sub-millisecond",
			base: 100 * time.Microsecond, ratio: 0.2, maximum: 200 * time.Microsecond,
			wantLower: 80 * time.Microsecond, wantUpper: 120 * time.Microsecond,
		},
		{
			name: "full jitter minimum",
			base: time.Nanosecond, ratio: 1, maximum: time.Nanosecond,
			wantLower: time.Nanosecond, wantUpper: time.Nanosecond,
		},
		{
			name: "disabled",
			base: 100 * time.Microsecond, ratio: 0, maximum: 200 * time.Microsecond,
			wantLower: 100 * time.Microsecond, wantUpper: 100 * time.Microsecond,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			lower, upper := jitterBounds(test.base, test.ratio, test.maximum)
			if lower != test.wantLower || upper != test.wantUpper {
				t.Fatalf("jitter bounds = [%s, %s], want [%s, %s]", lower, upper, test.wantLower, test.wantUpper)
			}
		})
	}
}

func TestJitterAlwaysStaysInsideBounds(t *testing.T) {
	t.Parallel()

	const iterations = 1_000
	base := 100 * time.Microsecond
	maximum := 200 * time.Microsecond
	lower, upper := jitterBounds(base, 0.2, maximum)
	for range iterations {
		got := jitter(base, 0.2, maximum)
		if got < lower || got > upper {
			t.Fatalf("jitter = %s, want value in [%s, %s]", got, lower, upper)
		}
	}
}
