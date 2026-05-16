package stats_test

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/invergent-ai/surogate-hub/pkg/kv"
	"github.com/invergent-ai/surogate-hub/pkg/kv/kvtest"
	"github.com/invergent-ai/surogate-hub/pkg/logging"
	"github.com/invergent-ai/surogate-hub/pkg/stats"
)

func TestStorageAccountant_AddBuffersInMemory(t *testing.T) {
	ctx := context.Background()
	store := kvtest.GetStore(ctx, t)
	a := stats.NewStorageAccountant(store)

	a.Add(ctx, "alice", "training", 100)
	a.Add(ctx, "alice", "training", 50)
	a.Add(ctx, "alice", "evals", 200)
	a.Add(ctx, "bob", "main", 7)

	// Without a flush, no KV writes have happened.
	if _, err := store.Get(ctx, stats.StoragePartition, stats.StorageRepoKey("alice", "training")); !errors.Is(err, kv.ErrNotFound) {
		t.Errorf("expected ErrNotFound before flush, got %v", err)
	}
}

func TestStorageAccountant_FlushPersistsCounters(t *testing.T) {
	ctx := context.Background()
	store := kvtest.GetStore(ctx, t)
	a := stats.NewStorageAccountant(store)

	a.Add(ctx, "alice", "training", 100)
	a.Add(ctx, "alice", "training", 50)
	a.Add(ctx, "alice", "evals", 200)

	if err := a.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	requireInt64(t, store, stats.StorageRepoKey("alice", "training"), 150)
	requireInt64(t, store, stats.StorageRepoKey("alice", "evals"), 200)
	requireInt64(t, store, stats.StorageUserKey("alice"), 350)
}

func TestStorageAccountant_FlushAccumulatesAcrossFlushes(t *testing.T) {
	ctx := context.Background()
	store := kvtest.GetStore(ctx, t)
	a := stats.NewStorageAccountant(store)

	a.Add(ctx, "alice", "training", 100)
	mustFlush(t, ctx, a)
	a.Add(ctx, "alice", "training", 25)
	a.Add(ctx, "alice", "training", -50)
	mustFlush(t, ctx, a)

	requireInt64(t, store, stats.StorageRepoKey("alice", "training"), 75)
	requireInt64(t, store, stats.StorageUserKey("alice"), 75)
}

func TestStorageAccountant_FlushIgnoresZeroAndEmptyKey(t *testing.T) {
	ctx := context.Background()
	store := kvtest.GetStore(ctx, t)
	a := stats.NewStorageAccountant(store)

	a.Add(ctx, "alice", "training", 0)
	a.Add(ctx, "", "training", 100)
	a.Add(ctx, "alice", "", 100)

	if err := a.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	for _, key := range [][]byte{
		stats.StorageRepoKey("alice", "training"),
		stats.StorageUserKey("alice"),
	} {
		if _, err := store.Get(ctx, stats.StoragePartition, key); !errors.Is(err, kv.ErrNotFound) {
			t.Errorf("expected no write for %s, got err=%v", key, err)
		}
	}
}

func TestStorageAccountant_DeleteRepoDropsKeyAndDecrementsUser(t *testing.T) {
	ctx := context.Background()
	store := kvtest.GetStore(ctx, t)
	a := stats.NewStorageAccountant(store)

	if err := store.Set(ctx, stats.StoragePartition, stats.StorageRepoKey("alice", "training"), []byte("500")); err != nil {
		t.Fatalf("seed repo: %v", err)
	}
	if err := store.Set(ctx, stats.StoragePartition, stats.StorageUserKey("alice"), []byte("500")); err != nil {
		t.Fatalf("seed user: %v", err)
	}
	// Also seed an in-memory delta that should be dropped.
	a.Add(ctx, "alice", "training", 999)

	if err := a.DeleteRepo(ctx, "alice", "training"); err != nil {
		t.Fatalf("DeleteRepo: %v", err)
	}

	if _, err := store.Get(ctx, stats.StoragePartition, stats.StorageRepoKey("alice", "training")); !errors.Is(err, kv.ErrNotFound) {
		t.Errorf("expected ErrNotFound for repo key after delete, got %v", err)
	}
	requireInt64(t, store, stats.StorageUserKey("alice"), 0)

	// A subsequent flush must not recreate the repo key, because Add was wiped from the map.
	if err := a.Flush(ctx); err != nil {
		t.Fatalf("Flush after DeleteRepo: %v", err)
	}
	if _, err := store.Get(ctx, stats.StoragePartition, stats.StorageRepoKey("alice", "training")); !errors.Is(err, kv.ErrNotFound) {
		t.Errorf("expected repo key to stay deleted, got %v", err)
	}
}

func TestStorageAccountant_DeleteRepoMissingIsNoop(t *testing.T) {
	ctx := context.Background()
	store := kvtest.GetStore(ctx, t)
	a := stats.NewStorageAccountant(store)
	if err := a.DeleteRepo(ctx, "ghost", "nope"); err != nil {
		t.Fatalf("DeleteRepo on missing repo: %v", err)
	}
}

func TestStorageAccountant_InitRepoSeedsZeroIdempotent(t *testing.T) {
	ctx := context.Background()
	store := kvtest.GetStore(ctx, t)
	a := stats.NewStorageAccountant(store)

	if err := a.InitRepo(ctx, "alice", "training"); err != nil {
		t.Fatalf("InitRepo: %v", err)
	}
	requireInt64(t, store, stats.StorageRepoKey("alice", "training"), 0)

	// Bring the counter up via the accountant.
	a.Add(ctx, "alice", "training", 100)
	mustFlush(t, ctx, a)
	requireInt64(t, store, stats.StorageRepoKey("alice", "training"), 100)

	// InitRepo a second time must not overwrite.
	if err := a.InitRepo(ctx, "alice", "training"); err != nil {
		t.Fatalf("InitRepo (second): %v", err)
	}
	requireInt64(t, store, stats.StorageRepoKey("alice", "training"), 100)
}

func TestStorageAccountant_StartTickerFlushes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := kvtest.GetStore(ctx, t)
	a := stats.NewStorageAccountant(store)
	a.Start(ctx, 5*time.Millisecond, logging.ContextUnavailable())

	a.Add(ctx, "alice", "training", 42)

	// Wait for at least one tick.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		got, err := store.Get(ctx, stats.StoragePartition, stats.StorageRepoKey("alice", "training"))
		if err == nil && got != nil {
			n, _ := strconv.ParseInt(string(got.Value), 10, 64)
			if n == 42 {
				return
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("ticker did not flush 42 to KV within deadline")
}

func mustFlush(t *testing.T, ctx context.Context, a *stats.StorageAccountant) {
	t.Helper()
	if err := a.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
}

func requireInt64(t *testing.T, store kv.Store, key []byte, want int64) {
	t.Helper()
	got, err := store.Get(context.Background(), stats.StoragePartition, key)
	if err != nil {
		t.Fatalf("Get(%s): %v", key, err)
	}
	n, err := strconv.ParseInt(string(got.Value), 10, 64)
	if err != nil {
		t.Fatalf("parse %s: %v", key, err)
	}
	if n != want {
		t.Errorf("%s = %d, want %d", key, n, want)
	}
}
