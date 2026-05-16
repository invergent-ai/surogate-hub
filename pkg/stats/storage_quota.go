package stats

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/invergent-ai/surogate-hub/pkg/kv"
)

// QuotaReason describes why a QuotaDecision is allowed or rejected.
type QuotaReason int

const (
	// QuotaReasonUnlimited indicates no quota is configured for the owner.
	QuotaReasonUnlimited QuotaReason = iota
	// QuotaReasonUnderLimit indicates the requested write fits under the configured quota.
	QuotaReasonUnderLimit
	// QuotaReasonOverLimit indicates the requested write would exceed the configured quota.
	QuotaReasonOverLimit
	// QuotaReasonUnknownSize indicates the request has no content length and a quota is set.
	QuotaReasonUnknownSize
)

// QuotaDecision is the result of QuotaChecker.Allow.
type QuotaDecision struct {
	Allowed    bool
	QuotaSet   bool
	QuotaBytes int64
	BytesUsed  int64
	Reason     QuotaReason
}

// ErrInvalidQuota is returned by SetQuota when the quota_bytes is negative.
var ErrInvalidQuota = errors.New("quota_bytes must be non-negative")

// QuotaChecker reads (and admin code writes) per-user storage quotas.
type QuotaChecker struct {
	storage kv.Store
}

// NewQuotaChecker constructs a QuotaChecker backed by the given KV store.
func NewQuotaChecker(storage kv.Store) *QuotaChecker {
	return &QuotaChecker{storage: storage}
}

// Allow checks whether the owner has capacity for contentLength more bytes.
//
// If no quota is set for the owner, the request is always allowed.
// If quota is set and contentLength < 0 (unknown size), the request is rejected with
// QuotaReasonUnknownSize, because we cannot guarantee the upload will fit.
// If quota is set and contentLength is known, the request is allowed iff used+contentLength <= quota.
//
// The used figure is read from the last-flushed user counter (soft check, see spec).
func (q *QuotaChecker) Allow(ctx context.Context, owner string, contentLength int64) (QuotaDecision, error) {
	quota, ok, err := q.GetQuota(ctx, owner)
	if err != nil {
		return QuotaDecision{}, err
	}
	if !ok {
		return QuotaDecision{Allowed: true, Reason: QuotaReasonUnlimited}, nil
	}
	if contentLength < 0 {
		return QuotaDecision{
			Allowed:    false,
			QuotaSet:   true,
			QuotaBytes: quota,
			Reason:     QuotaReasonUnknownSize,
		}, nil
	}
	used, err := q.readUserUsage(ctx, owner)
	if err != nil {
		return QuotaDecision{}, err
	}
	dec := QuotaDecision{
		QuotaSet:   true,
		QuotaBytes: quota,
		BytesUsed:  used,
	}
	if used+contentLength > quota {
		dec.Allowed = false
		dec.Reason = QuotaReasonOverLimit
	} else {
		dec.Allowed = true
		dec.Reason = QuotaReasonUnderLimit
	}
	return dec, nil
}

// GetQuota returns the quota for owner. The bool is false when no quota is configured.
func (q *QuotaChecker) GetQuota(ctx context.Context, owner string) (int64, bool, error) {
	got, err := q.storage.Get(ctx, StoragePartition, StorageQuotaKey(owner))
	if errors.Is(err, kv.ErrNotFound) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	n, err := strconv.ParseInt(string(got.Value), 10, 64)
	if err != nil {
		return 0, false, fmt.Errorf("parse quota for %q: %w", owner, err)
	}
	return n, true, nil
}

// SetQuota stores a new quota for owner. quotaBytes must be ≥ 0.
func (q *QuotaChecker) SetQuota(ctx context.Context, owner string, quotaBytes int64) error {
	if quotaBytes < 0 {
		return ErrInvalidQuota
	}
	return q.storage.Set(ctx, StoragePartition, StorageQuotaKey(owner), []byte(strconv.FormatInt(quotaBytes, 10)))
}

// ClearQuota removes the quota for owner, reverting them to unlimited.
// kv.Store.Delete is idempotent for missing keys, so no extra not-found handling is needed.
func (q *QuotaChecker) ClearQuota(ctx context.Context, owner string) error {
	return q.storage.Delete(ctx, StoragePartition, StorageQuotaKey(owner))
}

func (q *QuotaChecker) readUserUsage(ctx context.Context, owner string) (int64, error) {
	got, err := q.storage.Get(ctx, StoragePartition, StorageUserKey(owner))
	if errors.Is(err, kv.ErrNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	n, err := strconv.ParseInt(string(got.Value), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse user usage for %q: %w", owner, err)
	}
	return n, nil
}
