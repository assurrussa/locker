#!/bin/sh

set -eu

# Keep the post-tag probe isolated from any ambient Go workspace.
export GOWORK=off

version=${1:-}
if ! printf '%s\n' "$version" | grep -Eq '^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-[0-9A-Za-z.-]+)?$'; then
	echo "usage: $0 vMAJOR.MINOR.PATCH[-PRERELEASE]" >&2
	exit 2
fi

probe_dir=$(mktemp -d "${TMPDIR:-/tmp}/locker-consumer.XXXXXX")
trap 'rm -rf "$probe_dir"' EXIT HUP INT TERM

cd "$probe_dir"
go mod init example.com/locker-published-consumer
go get \
	"github.com/assurrussa/locker@$version" \
	"github.com/assurrussa/locker/redis@$version"

cat > consumer_test.go <<'EOF'
package consumer

import (
	"context"
	"testing"
	"time"

	"github.com/assurrussa/locker"
	lockerredis "github.com/assurrussa/locker/redis"
)

type store struct{}

func (store) Acquire(context.Context, string, string, time.Duration) (bool, error) { return true, nil }
func (store) Refresh(context.Context, string, string, time.Duration) (bool, error) { return true, nil }
func (store) Release(context.Context, string, string) (bool, error)                { return true, nil }

func TestPublishedAPICompiles(t *testing.T) {
	locks, err := locker.New(store{})
	if err != nil || locks == nil {
		t.Fatalf("locker.New: %v", err)
	}
	lease, err := locks.Acquire(context.Background(), "probe:blocking")
	if err != nil {
		t.Fatalf("Locker.Acquire: %v", err)
	}
	if err := lease.Release(context.Background()); err != nil {
		t.Fatalf("Lease.Release: %v", err)
	}
	lease, acquired, err := locks.TryAcquire(context.Background(), "probe:try")
	if err != nil || !acquired {
		t.Fatalf("Locker.TryAcquire: acquired=%v err=%v", acquired, err)
	}
	if err := lease.Release(context.Background()); err != nil {
		t.Fatalf("Lease.Release after TryAcquire: %v", err)
	}
	if lockerredis.StorageKey("probe") == "" {
		t.Fatal("empty Redis storage key")
	}
}
EOF

go test ./...
