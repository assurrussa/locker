package main

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestMemoryStorageRejectsExpiredOwnership(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		run  func(*memoryStorage) (bool, error)
	}{
		{
			name: "refresh",
			run: func(storage *memoryStorage) (bool, error) {
				return storage.Refresh(context.Background(), "expired", "owner", time.Second)
			},
		},
		{
			name: "release",
			run: func(storage *memoryStorage) (bool, error) {
				return storage.Release(context.Background(), "expired", "owner")
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			storage := newMemoryStorage()
			storage.locks["expired"] = lockRecord{token: "owner", exp: time.Now().Add(-time.Second)}
			owned, err := test.run(storage)
			if err != nil {
				t.Fatalf("expired operation: %v", err)
			}
			if owned {
				t.Fatal("expired record was reported as owned")
			}
			if _, exists := storage.locks["expired"]; exists {
				t.Fatal("expired record was not removed")
			}
		})
	}
}

func TestMemoryStorageActiveLifecycleAndCancellation(t *testing.T) {
	t.Parallel()

	storage := newMemoryStorage()
	acquired, err := storage.Acquire(context.Background(), "active", "owner", time.Second)
	if err != nil || !acquired {
		t.Fatalf("acquire: acquired=%v err=%v", acquired, err)
	}
	refreshed, err := storage.Refresh(context.Background(), "active", "owner", time.Second)
	if err != nil || !refreshed {
		t.Fatalf("refresh: refreshed=%v err=%v", refreshed, err)
	}
	released, err := storage.Release(context.Background(), "active", "owner")
	if err != nil || !released {
		t.Fatalf("release: released=%v err=%v", released, err)
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = storage.Acquire(canceled, "canceled", "owner", time.Second)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled acquire error = %v, want context.Canceled", err)
	}
}
