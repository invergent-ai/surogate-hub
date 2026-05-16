package stats_test

import (
	"testing"

	"github.com/invergent-ai/surogate-hub/pkg/stats"
)

func TestStorageKeyFormatters(t *testing.T) {
	if got, want := string(stats.StoragePartition), "storage"; got != want {
		t.Errorf("StoragePartition = %q, want %q", got, want)
	}
	if got, want := string(stats.StorageRepoKey("alice", "training")), "storage/repo/alice/training"; got != want {
		t.Errorf("StorageRepoKey = %q, want %q", got, want)
	}
	if got, want := string(stats.StorageUserKey("alice")), "storage/user/alice"; got != want {
		t.Errorf("StorageUserKey = %q, want %q", got, want)
	}
	if got, want := string(stats.StorageQuotaKey("alice")), "storage/quota/alice"; got != want {
		t.Errorf("StorageQuotaKey = %q, want %q", got, want)
	}
	if got, want := string(stats.StorageMetaLastReconciledAtKey("alice")), "storage/meta/alice/last_reconciled_at"; got != want {
		t.Errorf("StorageMetaLastReconciledAtKey = %q, want %q", got, want)
	}
	if got, want := string(stats.StorageRepoPrefix("alice")), "storage/repo/alice/"; got != want {
		t.Errorf("StorageRepoPrefix = %q, want %q", got, want)
	}
}

func TestParseStorageRepoKey(t *testing.T) {
	owner, repo, err := stats.ParseStorageRepoKey([]byte("storage/repo/alice/training"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if owner != "alice" || repo != "training" {
		t.Errorf("got (%q, %q), want (alice, training)", owner, repo)
	}
	if _, _, err := stats.ParseStorageRepoKey([]byte("nope")); err == nil {
		t.Errorf("expected error for malformed key")
	}
	if _, _, err := stats.ParseStorageRepoKey([]byte("storage/user/alice")); err == nil {
		t.Errorf("expected error for non-repo key")
	}
}
