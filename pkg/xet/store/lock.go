package store

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/invergent-ai/surogate-hub/pkg/kv"
)

var ErrGCLocked = errors.New("xet gc is already running")

var gcLockKey = []byte("xet/gc/lock")

// AcquireGCLock keeps two xet GC runs from overlapping. The lock expires after
// ttl, so a run that dies without releasing it blocks the next ones only that long.
func (r *Registry) AcquireGCLock(ctx context.Context, now time.Time, ttl time.Duration) (func(context.Context) error, error) {
	value := []byte(strconv.FormatInt(now.Add(ttl).Unix(), 10))
	release := func(ctx context.Context) error {
		return r.store.Delete(ctx, []byte(Partition), gcLockKey)
	}
	err := r.store.SetIf(ctx, []byte(Partition), gcLockKey, value, nil)
	if err == nil {
		return release, nil
	}
	if !errors.Is(err, kv.ErrPredicateFailed) {
		return nil, err
	}
	held, err := r.store.Get(ctx, []byte(Partition), gcLockKey)
	if err != nil {
		return nil, err
	}
	expires, err := strconv.ParseInt(string(held.Value), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("decode xet gc lock: %w", err)
	}
	if now.Unix() < expires {
		return nil, ErrGCLocked
	}
	err = r.store.SetIf(ctx, []byte(Partition), gcLockKey, value, held.Predicate)
	if errors.Is(err, kv.ErrPredicateFailed) {
		return nil, ErrGCLocked
	}
	if err != nil {
		return nil, err
	}
	return release, nil
}
