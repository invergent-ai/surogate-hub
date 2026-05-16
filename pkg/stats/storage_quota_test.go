package stats_test

import (
	"context"
	"errors"
	"testing"

	"github.com/invergent-ai/surogate-hub/pkg/kv"
	"github.com/invergent-ai/surogate-hub/pkg/kv/kvtest"
	"github.com/invergent-ai/surogate-hub/pkg/stats"
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
