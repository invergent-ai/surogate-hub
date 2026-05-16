package stats

// Object-level deletes are deliberately NOT hooked through the accountant: block.Adapter.Remove
// does not report the size of the deleted object, and rewiring the call sites to look it up first
// would couple every delete path to a metadata read. The StorageReconciler pass corrects the
// resulting drift within one interval (default 1h). Repository-level deletes ARE hooked via
// DeleteRepo, because the per-repo counter is read once and the user total is updated atomically.

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/invergent-ai/surogate-hub/pkg/kv"
	"github.com/invergent-ai/surogate-hub/pkg/logging"
)

// StorageAccountant buffers per-repo byte deltas in memory and periodically flushes them to the
// KV store. It is best-effort: a missed Add call leaks drift that the StorageReconciler corrects
// within one reconciler interval.
type StorageAccountant struct {
	storage kv.Store

	mu     sync.Mutex
	deltas map[accountantKey]int64
}

type accountantKey struct {
	owner string
	repo  string
}

// NewStorageAccountant constructs an accountant backed by the given KV store.
func NewStorageAccountant(storage kv.Store) *StorageAccountant {
	return &StorageAccountant{
		storage: storage,
		deltas:  make(map[accountantKey]int64),
	}
}

// Add records a byte delta for (owner, repo). Negative deltas are valid (deletes).
// Add never blocks on KV; deltas are applied on the next Flush.
func (a *StorageAccountant) Add(_ context.Context, owner, repo string, delta int64) {
	if delta == 0 || owner == "" || repo == "" {
		return
	}
	a.mu.Lock()
	a.deltas[accountantKey{owner: owner, repo: repo}] += delta
	a.mu.Unlock()
}

// Start launches a goroutine that calls Flush every interval until ctx is cancelled.
// A zero interval disables the ticker (test/dev only).
func (a *StorageAccountant) Start(ctx context.Context, interval time.Duration, logger logging.Logger) {
	if interval == 0 {
		return
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := a.Flush(ctx); err != nil {
					logger.WithError(err).Error("failed to flush storage accountant")
				}
			}
		}
	}()
}

// Flush drains the in-memory deltas into the KV store. Each affected repo counter is updated via
// SetIf with predicate retry. The corresponding per-user denormalized total is updated in the same
// flush. If a per-repo write fails after the delta has been removed from memory, the failed delta
// is re-added to the map so it will be retried on the next flush.
func (a *StorageAccountant) Flush(ctx context.Context) error {
	a.mu.Lock()
	if len(a.deltas) == 0 {
		a.mu.Unlock()
		return nil
	}
	pending := a.deltas
	a.deltas = make(map[accountantKey]int64)
	a.mu.Unlock()

	// Aggregate per-user totals for this flush.
	perUser := map[string]int64{}
	var errs []error
	for k, delta := range pending {
		if err := a.applyDelta(ctx, StorageRepoKey(k.owner, k.repo), delta); err != nil {
			// Re-add unflushed delta so the next flush retries it.
			a.mu.Lock()
			a.deltas[k] += delta
			a.mu.Unlock()
			errs = append(errs, fmt.Errorf("repo %s/%s: %w", k.owner, k.repo, err))
			continue
		}
		perUser[k.owner] += delta
	}
	for owner, delta := range perUser {
		if err := a.applyDelta(ctx, StorageUserKey(owner), delta); err != nil {
			errs = append(errs, fmt.Errorf("user %s: %w", owner, err))
		}
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

// DeleteRepo removes the repo counter and decrements the denormalized user total by the
// repo's recorded bytes. It does not call Add(owner, repo, -n), because that would recreate
// the repo key on the next flush.
//
// Concurrency: an Add() call concurrent with DeleteRepo may queue a delta into the in-memory
// map between the time we wipe it and the time the KV delete lands. We close that window by
// clearing the in-memory delta a second time, after every KV mutation has completed. A delta
// queued by an Add() that fires AFTER DeleteRepo returns will recreate the repo key on the
// next flush; the spec accepts this "late write after delete" drift and relies on the next
// reconciler pass to correct it.
func (a *StorageAccountant) DeleteRepo(ctx context.Context, owner, repo string) error {
	accKey := accountantKey{owner: owner, repo: repo}
	a.mu.Lock()
	delete(a.deltas, accKey)
	a.mu.Unlock()

	defer func() {
		// Second clear: drops any delta that snuck in during the KV ops above. Any delta
		// queued after this point recreates the key (acceptable per spec).
		a.mu.Lock()
		delete(a.deltas, accKey)
		a.mu.Unlock()
	}()

	key := StorageRepoKey(owner, repo)
	got, err := a.storage.Get(ctx, StoragePartition, key)
	if errors.Is(err, kv.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	current, err := strconv.ParseInt(string(got.Value), 10, 64)
	if err != nil {
		return fmt.Errorf("parse repo counter %s/%s: %w", owner, repo, err)
	}
	if err := a.storage.Delete(ctx, StoragePartition, key); err != nil {
		return err
	}
	if current == 0 {
		return nil
	}
	return a.applyDelta(ctx, StorageUserKey(owner), -current)
}

// InitRepo seeds the per-repo counter to zero. This makes the repo appear in GET
// /auth/users/{userId}/storage even before any uploads. Safe to call when the key already exists:
// it is a no-op in that case (returns nil without overwriting non-zero counters).
func (a *StorageAccountant) InitRepo(ctx context.Context, owner, repo string) error {
	if _, err := a.storage.Get(ctx, StoragePartition, StorageRepoKey(owner, repo)); err == nil {
		return nil
	} else if !errors.Is(err, kv.ErrNotFound) {
		return err
	}
	return a.storage.SetIf(ctx, StoragePartition, StorageRepoKey(owner, repo), []byte("0"), nil)
}

// applyDelta increments the int64 value at key by delta using SetIf + predicate retry.
// If the key does not exist, it is created with the delta value.
func (a *StorageAccountant) applyDelta(ctx context.Context, key []byte, delta int64) error {
	const retryDuration = 200 * time.Millisecond
	bo := NewConstantWithJitterBackOff(retryDuration)
	return backoff.Retry(func() error {
		var predicate kv.Predicate
		current := int64(0)
		got, err := a.storage.Get(ctx, StoragePartition, key)
		if err != nil {
			if !errors.Is(err, kv.ErrNotFound) {
				return backoff.Permanent(err)
			}
		} else {
			predicate = got.Predicate
			n, parseErr := strconv.ParseInt(string(got.Value), 10, 64)
			if parseErr != nil {
				return backoff.Permanent(fmt.Errorf("parse counter %q: %w", key, parseErr))
			}
			current = n
		}
		newValue := strconv.FormatInt(current+delta, 10)
		err = a.storage.SetIf(ctx, StoragePartition, key, []byte(newValue), predicate)
		if err != nil {
			if errors.Is(err, kv.ErrPredicateFailed) {
				return err // retry
			}
			return backoff.Permanent(err)
		}
		return nil
	}, backoff.WithContext(bo, ctx))
}
