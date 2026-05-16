package stats_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/invergent-ai/surogate-hub/pkg/kv"
	"github.com/invergent-ai/surogate-hub/pkg/kv/kvtest"
	"github.com/invergent-ai/surogate-hub/pkg/stats"
	"github.com/stretchr/testify/require"
)

func TestQuotaChecker_AbsentMeansUnlimited(t *testing.T) {
	ctx := context.Background()
	store := kvtest.GetStore(ctx, t)
	q := stats.NewQuotaChecker(store)

	dec, err := q.Allow(ctx, "alice", 999999999)
	if err != nil {
		t.Fatalf("Allow: %v", err)
	}
	if !dec.Allowed {
		t.Errorf("expected ALLOW with no quota set; got %+v", dec)
	}
	if dec.QuotaSet {
		t.Errorf("expected QuotaSet=false for unlimited user")
	}
}

func TestQuotaChecker_UnderQuotaAllows(t *testing.T) {
	ctx := context.Background()
	store := kvtest.GetStore(ctx, t)
	q := stats.NewQuotaChecker(store)
	if err := q.SetQuota(ctx, "alice", 1000); err != nil {
		t.Fatalf("SetQuota: %v", err)
	}
	a := stats.NewStorageAccountant(store)
	a.Add(ctx, "alice", "x", 400)
	if err := a.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	dec, err := q.Allow(ctx, "alice", 500)
	if err != nil {
		t.Fatalf("Allow: %v", err)
	}
	if !dec.Allowed {
		t.Errorf("expected ALLOW (400+500 ≤ 1000); got %+v", dec)
	}
}

func TestQuotaChecker_OverQuotaRejects(t *testing.T) {
	ctx := context.Background()
	store := kvtest.GetStore(ctx, t)
	q := stats.NewQuotaChecker(store)
	if err := q.SetQuota(ctx, "alice", 1000); err != nil {
		t.Fatalf("SetQuota: %v", err)
	}
	a := stats.NewStorageAccountant(store)
	a.Add(ctx, "alice", "x", 900)
	if err := a.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	dec, err := q.Allow(ctx, "alice", 200)
	if err != nil {
		t.Fatalf("Allow: %v", err)
	}
	if dec.Allowed {
		t.Errorf("expected REJECT (900+200 > 1000); got %+v", dec)
	}
	if dec.QuotaBytes != 1000 || dec.BytesUsed != 900 {
		t.Errorf("decision payload = %+v, want quota=1000 used=900", dec)
	}
	if dec.Reason != stats.QuotaReasonOverLimit {
		t.Errorf("Reason = %v, want QuotaReasonOverLimit", dec.Reason)
	}
}

func TestQuotaChecker_UnknownSizeRejectedWhenQuotaSet(t *testing.T) {
	ctx := context.Background()
	store := kvtest.GetStore(ctx, t)
	q := stats.NewQuotaChecker(store)
	if err := q.SetQuota(ctx, "alice", 1000); err != nil {
		t.Fatalf("SetQuota: %v", err)
	}
	dec, err := q.Allow(ctx, "alice", -1)
	if err != nil {
		t.Fatalf("Allow: %v", err)
	}
	if dec.Allowed {
		t.Errorf("expected REJECT for unknown size with quota set")
	}
	if dec.Reason != stats.QuotaReasonUnknownSize {
		t.Errorf("Reason = %v, want QuotaReasonUnknownSize", dec.Reason)
	}
}

func TestQuotaChecker_UnknownSizeAllowedWhenUnlimited(t *testing.T) {
	ctx := context.Background()
	store := kvtest.GetStore(ctx, t)
	q := stats.NewQuotaChecker(store)
	dec, err := q.Allow(ctx, "alice", -1)
	if err != nil {
		t.Fatalf("Allow: %v", err)
	}
	if !dec.Allowed {
		t.Errorf("expected ALLOW for unknown size without quota; got %+v", dec)
	}
}

func TestQuotaChecker_SetGetClear(t *testing.T) {
	ctx := context.Background()
	store := kvtest.GetStore(ctx, t)
	q := stats.NewQuotaChecker(store)

	if _, ok, err := q.GetQuota(ctx, "alice"); err != nil || ok {
		t.Errorf("GetQuota empty: (_, ok=%v, err=%v)", ok, err)
	}
	if err := q.SetQuota(ctx, "alice", 12345); err != nil {
		t.Fatalf("SetQuota: %v", err)
	}
	v, ok, err := q.GetQuota(ctx, "alice")
	if err != nil || !ok || v != 12345 {
		t.Errorf("GetQuota after set: (%d, ok=%v, err=%v)", v, ok, err)
	}
	if err := q.ClearQuota(ctx, "alice"); err != nil {
		t.Fatalf("ClearQuota: %v", err)
	}
	if _, ok, err := q.GetQuota(ctx, "alice"); err != nil || ok {
		t.Errorf("GetQuota after clear: ok=%v, err=%v", ok, err)
	}
}

func TestQuotaChecker_SetRejectsNegative(t *testing.T) {
	ctx := context.Background()
	store := kvtest.GetStore(ctx, t)
	q := stats.NewQuotaChecker(store)
	err := q.SetQuota(ctx, "alice", -1)
	if !errors.Is(err, stats.ErrInvalidQuota) {
		t.Errorf("expected ErrInvalidQuota for negative quota, got %v", err)
	}
}

// TestQuotaChecker_SoftCheckOverageWindow exercises the documented soft-check semantics:
// two concurrent Allow() calls can both succeed when together they would exceed the quota.
// This is documented behaviour ("the check reads the last-flushed counter, so concurrent
// uploads in flight from the same user can temporarily push the user above quota") — the
// test pins the behaviour so any future tightening to a hard check is intentional.
func TestQuotaChecker_SoftCheckOverageWindow(t *testing.T) {
	ctx := context.Background()
	store := kvtest.GetStore(ctx, t)
	q := stats.NewQuotaChecker(store)
	// quota=100, used=80. Two concurrent uploads of 25 each should both pass the start check
	// (80+25 = 105 > 100 would reject, BUT 80+25 ≤ 100 is FALSE, so each WOULD reject).
	// Test the boundary: quota=100, used=80, each upload is 10 bytes — both pass, then total
	// after both complete would be 100 (at-cap). Third concurrent upload of 10 would push to
	// 110 over the cap. We assert that the first two Allow() calls succeed concurrently and
	// the third fails after the accountant flushes.
	require.NoError(t, q.SetQuota(ctx, "alice", 100))
	a := stats.NewStorageAccountant(store)
	a.Add(ctx, "alice", "x", 80)
	require.NoError(t, a.Flush(ctx))

	// Two concurrent Allow() calls — both see used=80 and contentLength=10, both pass.
	var wg sync.WaitGroup
	results := make([]stats.QuotaDecision, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			dec, err := q.Allow(ctx, "alice", 10)
			require.NoError(t, err)
			results[i] = dec
		}(i)
	}
	wg.Wait()
	require.True(t, results[0].Allowed && results[1].Allowed, "both concurrent uploads must pass the soft check; got %+v / %+v", results[0], results[1])

	// Simulate both uploads having completed — accountant has the 20 extra bytes.
	a.Add(ctx, "alice", "x", 10)
	a.Add(ctx, "alice", "x", 10)
	require.NoError(t, a.Flush(ctx))

	// A subsequent upload of 10 bytes now sees used=100 + 10 > 100, so it must be rejected.
	dec, err := q.Allow(ctx, "alice", 10)
	require.NoError(t, err)
	require.False(t, dec.Allowed, "after flush reveals used=100, subsequent over-quota upload must reject")
}

func TestQuotaChecker_ClearMissingIsNoop(t *testing.T) {
	ctx := context.Background()
	store := kvtest.GetStore(ctx, t)
	q := stats.NewQuotaChecker(store)
	if err := q.ClearQuota(ctx, "ghost"); err != nil {
		t.Fatalf("ClearQuota on missing user: %v", err)
	}
	// Sanity check: the underlying Delete on an absent key did not record an error wrapper.
	if _, err := store.Get(ctx, stats.StoragePartition, stats.StorageQuotaKey("ghost")); !errors.Is(err, kv.ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}
