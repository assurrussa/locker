// Package redis provides a go-redis backed locker.Store.
package redis

import (
	"fmt"

	"github.com/cespare/xxhash/v2"
)

// StorageKey returns the Redis key used for a raw lock name.
//
// The format is intentionally stable for interoperability with the previous
// implementation: locker:<first-six-runes-or-none>:<xxhash64-hex>.
func StorageKey(lockName string) string {
	if lockName == "" {
		return formatStorageKey("none", lockName)
	}

	runes := []rune(lockName)
	if len(runes) > 6 {
		runes = runes[:6]
	}

	return formatStorageKey(string(runes), lockName)
}

func formatStorageKey(prefix, lockName string) string {
	return fmt.Sprintf("locker:%s:%016x", prefix, xxhash.Sum64String(lockName))
}
