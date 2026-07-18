package main

import (
	"context"
	"fmt"
	"time"

	"github.com/assurrussa/locker"
)

func main() {
	ctx := context.Background()
	storage := newMemoryStorage()

	m, err := locker.New(
		storage,
		locker.WithLeaseTTL(30*time.Second),
		locker.WithRefreshInterval(5*time.Second),
		locker.WithOperationTimeout(5*time.Second),
	)
	if err != nil {
		panic(err)
	}

	err = m.WithLock(ctx, "job:reindex", func(_ context.Context) error {
		fmt.Println("critical section under lock")
		return nil
	})
	if err != nil {
		panic(err)
	}

	ran, err := m.WithTryLock(ctx, "job:reindex", func(_ context.Context) error {
		fmt.Println("try lock callback executed")
		return nil
	})
	if err != nil {
		panic(err)
	}

	fmt.Printf("with try lock ran: %v\n", ran)

	lease, err := m.Acquire(ctx, "worker:leader")
	if err != nil {
		panic(err)
	}
	fmt.Printf("explicit lease acquired for %s\n", lease.Key())
	if err := lease.Release(ctx); err != nil {
		panic(err)
	}
}

type lockRecord struct {
	token string
	exp   time.Time
}

type memoryStorage struct {
	gate  chan struct{}
	locks map[string]lockRecord
}

func newMemoryStorage() *memoryStorage {
	gate := make(chan struct{}, 1)
	gate <- struct{}{}

	return &memoryStorage{
		gate:  gate,
		locks: make(map[string]lockRecord),
	}
}

func (s *memoryStorage) Acquire(ctx context.Context, key, token string, ttl time.Duration) (bool, error) {
	if err := s.enter(ctx); err != nil {
		return false, err
	}
	defer s.leave()

	now := time.Now()
	if rec, ok := s.locks[key]; ok && now.Before(rec.exp) {
		return false, nil
	}

	s.locks[key] = lockRecord{token: token, exp: now.Add(ttl)}

	return true, nil
}

func (s *memoryStorage) Refresh(ctx context.Context, key, token string, ttl time.Duration) (bool, error) {
	if err := s.enter(ctx); err != nil {
		return false, err
	}
	defer s.leave()

	rec, ok := s.locks[key]
	now := time.Now()
	if !ok || rec.token != token || !now.Before(rec.exp) {
		if ok && !now.Before(rec.exp) {
			delete(s.locks, key)
		}

		return false, nil
	}

	rec.exp = now.Add(ttl)
	s.locks[key] = rec

	return true, nil
}

func (s *memoryStorage) Release(ctx context.Context, key, token string) (bool, error) {
	if err := s.enter(ctx); err != nil {
		return false, err
	}
	defer s.leave()

	rec, ok := s.locks[key]
	now := time.Now()
	if !ok || rec.token != token || !now.Before(rec.exp) {
		if ok && !now.Before(rec.exp) {
			delete(s.locks, key)
		}

		return false, nil
	}

	delete(s.locks, key)

	return true, nil
}

func (s *memoryStorage) enter(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.gate:
	}

	if err := ctx.Err(); err != nil {
		s.leave()

		return err
	}

	return nil
}

func (s *memoryStorage) leave() {
	s.gate <- struct{}{}
}
