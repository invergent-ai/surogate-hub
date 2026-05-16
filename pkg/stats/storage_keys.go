package stats

import (
	"errors"
	"fmt"
	"regexp"
)

// StoragePartition is the KV partition that holds per-user/per-repo storage counters and quotas.
var StoragePartition = []byte("storage")

// ErrInvalidStorageRepoKey is returned by ParseStorageRepoKey when the key does not match the
// expected layout.
var ErrInvalidStorageRepoKey = errors.New("invalid storage repo key")

// StorageRepoKey returns the key holding the bytes-allocated counter for a single repo.
// Layout: storage/repo/{owner}/{repo}
func StorageRepoKey(owner, repo string) []byte {
	return []byte(fmt.Sprintf("storage/repo/%s/%s", owner, repo))
}

// StorageRepoPrefix returns the scan prefix for all repos owned by an owner.
// Layout: storage/repo/{owner}/
func StorageRepoPrefix(owner string) []byte {
	return []byte(fmt.Sprintf("storage/repo/%s/", owner))
}

// StorageUserKey returns the key holding the denormalized per-user total.
// Layout: storage/user/{owner}
func StorageUserKey(owner string) []byte {
	return []byte(fmt.Sprintf("storage/user/%s", owner))
}

// StorageQuotaKey returns the key holding the per-user quota. Absence ⇒ unlimited.
// Layout: storage/quota/{owner}
func StorageQuotaKey(owner string) []byte {
	return []byte(fmt.Sprintf("storage/quota/%s", owner))
}

// StorageMetaLastReconciledAtKey returns the key holding the timestamp of the last reconciler pass.
// Layout: storage/meta/{owner}/last_reconciled_at
func StorageMetaLastReconciledAtKey(owner string) []byte {
	return []byte(fmt.Sprintf("storage/meta/%s/last_reconciled_at", owner))
}

var storageRepoKeyRegexp = regexp.MustCompile(`^storage/repo/([^/]+)/(.+)$`)

// ParseStorageRepoKey reverses StorageRepoKey. Returns ErrInvalidStorageRepoKey on mismatch.
func ParseStorageRepoKey(key []byte) (owner, repo string, err error) {
	m := storageRepoKeyRegexp.FindSubmatch(key)
	if m == nil {
		return "", "", fmt.Errorf("%w: %q", ErrInvalidStorageRepoKey, key)
	}
	return string(m[1]), string(m[2]), nil
}
