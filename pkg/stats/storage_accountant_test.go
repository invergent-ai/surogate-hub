package stats_test

import (
	"context"
	"errors"
	"strconv"
	"sync"
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

// TestStorageAccountant_DeleteRepoClearsDeltaQueuedDuringKVOps reproduces the TOCTOU between
// the in-memory delta wipe and the KV repo-delete: an Add() that lands while DeleteRepo is
// mid-flight would otherwise sit in the deltas map and the next Flush would recreate the
// just-deleted repo key. The two-stage wipe (before KV ops, deferred after KV ops) closes that
// window. We can't easily race "Add during KV op" deterministically, so we simulate it by
// calling Add() immediately after DeleteRepo has finished its first in-memory clear — the
// deferred second clear should still drop it.
func TestStorageAccountant_DeleteRepoClearsDeltaQueuedDuringKVOps(t *testing.T) {
	ctx := context.Background()
	store := kvtest.GetStore(ctx, t)
	a := stats.NewStorageAccountant(store)

	// Seed: repo has 500 bytes, user has 500.
	if err := store.Set(ctx, stats.StoragePartition, stats.StorageRepoKey("alice", "training"), []byte("500")); err != nil {
		t.Fatalf("seed repo: %v", err)
	}
	if err := store.Set(ctx, stats.StoragePartition, stats.StorageUserKey("alice"), []byte("500")); err != nil {
		t.Fatalf("seed user: %v", err)
	}

	// Concurrent Add() racing against DeleteRepo. Run several concurrent Adds during the
	// expected DeleteRepo execution window. The exact ordering is non-deterministic, but the
	// invariant "after DeleteRepo returns, the repo key stays deleted across a flush" must
	// hold for any interleaving — the deferred second wipe enforces it.
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := a.DeleteRepo(ctx, "alice", "training"); err != nil {
			t.Errorf("DeleteRepo: %v", err)
		}
	}()
	for i := 0; i < 50; i++ {
		a.Add(ctx, "alice", "training", 1)
	}
	<-done

	// Flush — must NOT recreate the repo key.
	if err := a.Flush(ctx); err != nil {
		t.Fatalf("Flush after DeleteRepo: %v", err)
	}
	if _, err := store.Get(ctx, stats.StoragePartition, stats.StorageRepoKey("alice", "training")); !errors.Is(err, kv.ErrNotFound) {
		t.Errorf("repo key must stay deleted after DeleteRepo + flush; got err=%v", err)
	}
}

// TestStorageAccountant_ConcurrentDeleteRepoUserTotalIsConsistent runs two DeleteRepo calls in
// parallel against different repos of the same owner. The user total must end up exactly equal
// to "initial - (sum of deleted repo counters)" with no double-decrement, even though the two
// goroutines race on the StorageUserKey update (which goes through applyDelta's predicate
// retry).
func TestStorageAccountant_ConcurrentDeleteRepoUserTotalIsConsistent(t *testing.T) {
	ctx := context.Background()
	store := kvtest.GetStore(ctx, t)
	a := stats.NewStorageAccountant(store)

	// Seed: two repos with 300 + 700 bytes, user total = 1000.
	const repoA, repoB = "training", "evals"
	const owner = "alice"
	for k, v := range map[string]string{
		string(stats.StorageRepoKey(owner, repoA)): "300",
		string(stats.StorageRepoKey(owner, repoB)): "700",
		string(stats.StorageUserKey(owner)):        "1000",
	} {
		if err := store.Set(ctx, stats.StoragePartition, []byte(k), []byte(v)); err != nil {
			t.Fatalf("seed %s: %v", k, err)
		}
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		if err := a.DeleteRepo(ctx, owner, repoA); err != nil {
			t.Errorf("DeleteRepo A: %v", err)
		}
	}()
	go func() {
		defer wg.Done()
		if err := a.DeleteRepo(ctx, owner, repoB); err != nil {
			t.Errorf("DeleteRepo B: %v", err)
		}
	}()
	wg.Wait()

	// Both repo keys gone.
	if _, err := store.Get(ctx, stats.StoragePartition, stats.StorageRepoKey(owner, repoA)); !errors.Is(err, kv.ErrNotFound) {
		t.Errorf("repo A key must be deleted, got err=%v", err)
	}
	if _, err := store.Get(ctx, stats.StoragePartition, stats.StorageRepoKey(owner, repoB)); !errors.Is(err, kv.ErrNotFound) {
		t.Errorf("repo B key must be deleted, got err=%v", err)
	}
	// User total = 1000 - 300 - 700 = 0 exactly; no double-decrement, no leak.
	requireInt64(t, store, stats.StorageUserKey(owner), 0)
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
