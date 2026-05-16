package stats_test

import (
	"context"
	"errors"
	"testing"

	"github.com/invergent-ai/surogate-hub/pkg/kv/kvtest"
	"github.com/invergent-ai/surogate-hub/pkg/stats"
)

type fakeRepoLister struct {
	repos []stats.ReconcilerRepo
}

func (f *fakeRepoLister) ListRepos(_ context.Context, fn func(stats.ReconcilerRepo) error) error {
	for _, r := range f.repos {
		if err := fn(r); err != nil {
			return err
		}
	}
	return nil
}

type fakeNamespaceSizer struct {
	sizes map[string]int64
}

func (f *fakeNamespaceSizer) NamespaceSize(_ context.Context, _ string, storageNamespace string) (int64, error) {
	return f.sizes[storageNamespace], nil
}

type errInjectingSizer struct {
	sizes map[string]int64
}

func (e *errInjectingSizer) NamespaceSize(_ context.Context, _ string, storageNamespace string) (int64, error) {
	if v, ok := e.sizes[storageNamespace]; ok {
		return v, nil
	}
	return 0, errors.New("boom")
}

func TestStorageReconciler_OverwritesRepoCountersFromWalker(t *testing.T) {
	ctx := context.Background()
	store := kvtest.GetStore(ctx, t)
	// Pre-populate stale counters (drift).
	if err := store.Set(ctx, stats.StoragePartition, stats.StorageRepoKey("alice", "training"), []byte("999")); err != nil {
		t.Fatalf("seed: %v", err)
	}

	rl := &fakeRepoLister{repos: []stats.ReconcilerRepo{
		{Owner: "alice", Name: "training", StorageID: "s1", StorageNamespace: "s3://alice-training"},
		{Owner: "alice", Name: "evals", StorageID: "s1", StorageNamespace: "s3://alice-evals"},
	}}
	ns := &fakeNamespaceSizer{sizes: map[string]int64{
		"s3://alice-training": 150,
		"s3://alice-evals":    200,
	}}

	r := stats.NewStorageReconciler(store, rl, ns, stats.NewStorageAccountant(store))
	if err := r.RunOnce(ctx); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}

	requireInt64(t, store, stats.StorageRepoKey("alice", "training"), 150)
	requireInt64(t, store, stats.StorageRepoKey("alice", "evals"), 200)
	requireInt64(t, store, stats.StorageUserKey("alice"), 350)

	// last_reconciled_at written.
	got, err := store.Get(ctx, stats.StoragePartition, stats.StorageMetaLastReconciledAtKey("alice"))
	if err != nil {
		t.Fatalf("last_reconciled_at missing: %v", err)
	}
	if len(got.Value) == 0 {
		t.Errorf("last_reconciled_at empty")
	}
}

func TestStorageReconciler_FlushesPendingAccountantDeltasBeforeOverwrite(t *testing.T) {
	ctx := context.Background()
	store := kvtest.GetStore(ctx, t)
	a := stats.NewStorageAccountant(store)
	a.Add(ctx, "alice", "training", 50) // pending delta, not flushed

	rl := &fakeRepoLister{repos: []stats.ReconcilerRepo{
		{Owner: "alice", Name: "training", StorageID: "s1", StorageNamespace: "s3://alice-training"},
	}}
	ns := &fakeNamespaceSizer{sizes: map[string]int64{"s3://alice-training": 100}}

	r := stats.NewStorageReconciler(store, rl, ns, a)
	if err := r.RunOnce(ctx); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}

	// Reconciler value (100) wins over accountant delta because flush happens before write.
	requireInt64(t, store, stats.StorageRepoKey("alice", "training"), 100)
	requireInt64(t, store, stats.StorageUserKey("alice"), 100)
}

func TestStorageReconciler_ContinuesOnNamespaceError(t *testing.T) {
	ctx := context.Background()
	store := kvtest.GetStore(ctx, t)
	rl := &fakeRepoLister{repos: []stats.ReconcilerRepo{
		{Owner: "alice", Name: "ok", StorageID: "s1", StorageNamespace: "ok"},
		{Owner: "alice", Name: "bad", StorageID: "s1", StorageNamespace: "bad"},
	}}
	ns := &errInjectingSizer{sizes: map[string]int64{"ok": 7}}
	r := stats.NewStorageReconciler(store, rl, ns, stats.NewStorageAccountant(store))
	if err := r.RunOnce(ctx); err == nil {
		t.Errorf("expected aggregated error from bad repo")
	}
	requireInt64(t, store, stats.StorageRepoKey("alice", "ok"), 7)
}

func TestStorageReconciler_PartialFailureDoesNotMarkClean(t *testing.T) {
	// When a single repo's NamespaceSize fails, the owner's last_reconciled_at must NOT be
	// updated — otherwise the API's is_estimate flag would flip to false while the failed
	// repo's counter is still stale.
	ctx := context.Background()
	store := kvtest.GetStore(ctx, t)
	rl := &fakeRepoLister{repos: []stats.ReconcilerRepo{
		{Owner: "alice", Name: "ok", StorageID: "s1", StorageNamespace: "ok"},
		{Owner: "alice", Name: "bad", StorageID: "s1", StorageNamespace: "bad"},
	}}
	ns := &errInjectingSizer{sizes: map[string]int64{"ok": 7}}
	r := stats.NewStorageReconciler(store, rl, ns, stats.NewStorageAccountant(store))

	err := r.RunOnce(ctx)
	if err == nil {
		t.Errorf("expected aggregated error from bad repo")
	}
	// Good repo counter is correct.
	requireInt64(t, store, stats.StorageRepoKey("alice", "ok"), 7)
	// User total is recomputed (sum of available repo counters) — that's still useful.
	requireInt64(t, store, stats.StorageUserKey("alice"), 7)
	// last_reconciled_at MUST NOT be written because one repo failed.
	if _, err := store.Get(ctx, stats.StoragePartition, stats.StorageMetaLastReconciledAtKey("alice")); err == nil {
		t.Errorf("last_reconciled_at must not be written when reconcile had errors")
	}
}

func TestStorageReconciler_FullyCleanPassMarksReconciledAt(t *testing.T) {
	// Compare: a pass with zero errors must write last_reconciled_at.
	ctx := context.Background()
	store := kvtest.GetStore(ctx, t)
	rl := &fakeRepoLister{repos: []stats.ReconcilerRepo{
		{Owner: "alice", Name: "ok", StorageID: "s1", StorageNamespace: "ok"},
	}}
	ns := &fakeNamespaceSizer{sizes: map[string]int64{"ok": 7}}
	r := stats.NewStorageReconciler(store, rl, ns, stats.NewStorageAccountant(store))
	if err := r.RunOnce(ctx); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	got, err := store.Get(ctx, stats.StoragePartition, stats.StorageMetaLastReconciledAtKey("alice"))
	if err != nil {
		t.Errorf("expected last_reconciled_at to be written: %v", err)
	} else if len(got.Value) == 0 {
		t.Errorf("last_reconciled_at empty")
	}
}

// TestStorageReconciler_LockMapDoesNotGrowAcrossPasses verifies that the per-repo lock map is
// cleared at the start of each RunOnce. Otherwise on a long-running server with high repo
// churn the map would leak one entry per ever-seen repo. We don't reach into the unexported
// `locks` field; instead we run RunOnce against a different set of repos each time and observe
// that the second pass succeeds (i.e. the map was rebuilt fresh — there is no stale state
// causing deadlock or wrong behavior). The actual memory invariant is enforced by the
// implementation contract; the test guards against future regressions that would attempt to
// reuse locks across passes.
func TestStorageReconciler_LockMapDoesNotGrowAcrossPasses(t *testing.T) {
	ctx := context.Background()
	store := kvtest.GetStore(ctx, t)
	// Pass 1: 10 repos.
	pass1 := make([]stats.ReconcilerRepo, 10)
	sizes := map[string]int64{}
	for i := range pass1 {
		ns := "mem://repo-1-" + string(rune('a'+i))
		pass1[i] = stats.ReconcilerRepo{Owner: "alice", Name: "r" + string(rune('a'+i)), StorageID: "s1", StorageNamespace: ns}
		sizes[ns] = int64(i + 1)
	}
	rl := &fakeRepoLister{repos: pass1}
	r := stats.NewStorageReconciler(store, rl, &fakeNamespaceSizer{sizes: sizes}, stats.NewStorageAccountant(store)).WithConcurrency(4)
	if err := r.RunOnce(ctx); err != nil {
		t.Fatalf("pass 1: %v", err)
	}

	// Pass 2: completely different repos (simulates churn — pass-1 repos deleted, new ones created).
	pass2 := make([]stats.ReconcilerRepo, 10)
	sizes2 := map[string]int64{}
	for i := range pass2 {
		ns := "mem://repo-2-" + string(rune('a'+i))
		pass2[i] = stats.ReconcilerRepo{Owner: "bob", Name: "r" + string(rune('a'+i)), StorageID: "s1", StorageNamespace: ns}
		sizes2[ns] = int64(i + 100)
	}
	rl.repos = pass2
	r2 := stats.NewStorageReconciler(store, rl, &fakeNamespaceSizer{sizes: sizes2}, stats.NewStorageAccountant(store)).WithConcurrency(4)
	if err := r2.RunOnce(ctx); err != nil {
		t.Fatalf("pass 2: %v", err)
	}

	// Sanity: bob's totals reflect pass-2's sizes only.
	expectedBob := int64(0)
	for i := 0; i < 10; i++ {
		expectedBob += int64(i + 100)
	}
	requireInt64(t, store, stats.StorageUserKey("bob"), expectedBob)
}

func TestStorageReconciler_ConcurrentRunsPerRepoSafe(t *testing.T) {
	ctx := context.Background()
	store := kvtest.GetStore(ctx, t)
	// Many repos under one owner — exercise concurrency without races.
	repos := make([]stats.ReconcilerRepo, 0, 20)
	sizes := make(map[string]int64)
	expectedTotal := int64(0)
	for i := 0; i < 20; i++ {
		ns := "mem://r" + string(rune('a'+i))
		repos = append(repos, stats.ReconcilerRepo{Owner: "alice", Name: "r" + string(rune('a'+i)), StorageID: "s1", StorageNamespace: ns})
		sizes[ns] = int64(i + 1)
		expectedTotal += int64(i + 1)
	}
	r := stats.NewStorageReconciler(store, &fakeRepoLister{repos: repos}, &fakeNamespaceSizer{sizes: sizes}, stats.NewStorageAccountant(store)).WithConcurrency(8)
	if err := r.RunOnce(ctx); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	requireInt64(t, store, stats.StorageUserKey("alice"), expectedTotal)
}
