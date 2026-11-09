package store

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/invergent-ai/surogate-hub/pkg/kv"
	"github.com/invergent-ai/surogate-hub/pkg/kv/kvtest"
	"github.com/stretchr/testify/require"
)

var parkedNow = time.Unix(1_800_000_000, 0)

func TestParkThenRestoreFileRecord(t *testing.T) {
	ctx := context.Background()
	registry := NewRegistry(kvtest.GetStore(ctx, t))
	_, err := registry.RegisterFile(ctx, ShardFileInfo{FileHash: "file-a", SizeBytes: 12})
	require.NoError(t, err)
	original, err := registry.GetFile(ctx, "file-a")
	require.NoError(t, err)

	parked, err := registry.Park(ctx, ParkedFile, "file-a", parkedNow)
	require.NoError(t, err)
	require.True(t, parked)
	_, err = registry.GetFile(ctx, "file-a")
	require.ErrorIs(t, err, kv.ErrNotFound)
	list, err := registry.ListParked(ctx, ParkedFile, 0)
	require.NoError(t, err)
	require.Equal(t, StateParked, list["file-a"].State)
	require.Equal(t, parkedNow.Unix(), list["file-a"].ParkedAt)

	result, err := registry.Restore(ctx, ParkedFile, "file-a", nil, false)
	require.NoError(t, err)
	require.Equal(t, Restored, result)
	restored, err := registry.GetFile(ctx, "file-a")
	require.NoError(t, err)
	require.Equal(t, original, restored)
	list, err = registry.ListParked(ctx, ParkedFile, 0)
	require.NoError(t, err)
	require.Empty(t, list)
}

func TestParkNeedsALiveValueAndNoParkedKey(t *testing.T) {
	ctx := context.Background()
	registry := NewRegistry(kvtest.GetStore(ctx, t))

	parked, err := registry.Park(ctx, ParkedXorb, "xorb-a", parkedNow)
	require.NoError(t, err)
	require.False(t, parked)

	require.NoError(t, registry.PutXorbInfo(ctx, "xorb-a", []byte("footer")))
	_, err = registry.ParkOrphanXorb(ctx, "xorb-a", parkedNow)
	require.NoError(t, err)
	parked, err = registry.Park(ctx, ParkedXorb, "xorb-a", parkedNow)
	require.NoError(t, err)
	require.False(t, parked)
	footer, err := registry.GetXorbInfo(ctx, "xorb-a")
	require.NoError(t, err)
	require.Equal(t, []byte("footer"), footer)
}

func TestRestoreKeepsNewerLiveValue(t *testing.T) {
	ctx := context.Background()
	registry := NewRegistry(kvtest.GetStore(ctx, t))
	require.NoError(t, registry.PutXorbInfo(ctx, "xorb-a", []byte("old")))
	_, err := registry.Park(ctx, ParkedXorb, "xorb-a", parkedNow)
	require.NoError(t, err)
	require.NoError(t, registry.PutXorbInfo(ctx, "xorb-a", []byte("new")))

	result, err := registry.Restore(ctx, ParkedXorb, "xorb-a", nil, false)
	require.NoError(t, err)
	require.Equal(t, Restored, result)
	footer, err := registry.GetXorbInfo(ctx, "xorb-a")
	require.NoError(t, err)
	require.Equal(t, []byte("new"), footer)
	requireNotParked(t, registry, ParkedXorb, "xorb-a")
}

func TestDeleteParkedLosesToRestoring(t *testing.T) {
	ctx := context.Background()
	store := kvtest.GetStore(ctx, t)
	registry := NewRegistry(store)
	setParked(t, store, ParkedXorb, "xorb-a", Parked{State: StateRestoring, ParkedAt: parkedNow.Unix(), Data: []byte("footer")})

	deleted, err := registry.DeleteParked(ctx, ParkedXorb, "xorb-a", func() error {
		t.Fatal("must not remove the object of a restoring xorb")
		return nil
	})
	require.NoError(t, err)
	require.False(t, deleted)
	require.Equal(t, StateRestoring, getParked(t, registry, ParkedXorb, "xorb-a").State)
}

func TestRestoreLosesToDeleting(t *testing.T) {
	ctx := context.Background()
	store := kvtest.GetStore(ctx, t)
	registry := NewRegistry(store)
	setParked(t, store, ParkedXorb, "xorb-a", Parked{State: StateDeleting, ParkedAt: parkedNow.Unix(), Data: []byte("footer")})

	result, err := registry.Restore(ctx, ParkedXorb, "xorb-a", nil, true)
	require.NoError(t, err)
	require.Equal(t, Deleting, result)
	_, err = registry.GetXorbInfo(ctx, "xorb-a")
	require.ErrorIs(t, err, kv.ErrNotFound)
	require.Equal(t, StateDeleting, getParked(t, registry, ParkedXorb, "xorb-a").State)
}

func TestDeleteParkedRemovesObjectThenKey(t *testing.T) {
	ctx := context.Background()
	registry := NewRegistry(kvtest.GetStore(ctx, t))
	require.NoError(t, registry.PutXorbInfo(ctx, "xorb-a", []byte("footer")))
	_, err := registry.Park(ctx, ParkedXorb, "xorb-a", parkedNow)
	require.NoError(t, err)

	failing := errors.New("blockstore down")
	_, err = registry.DeleteParked(ctx, ParkedXorb, "xorb-a", func() error { return failing })
	require.ErrorIs(t, err, failing)
	require.Equal(t, StateDeleting, getParked(t, registry, ParkedXorb, "xorb-a").State)

	removed := 0
	deleted, err := registry.DeleteParked(ctx, ParkedXorb, "xorb-a", func() error { removed++; return nil })
	require.NoError(t, err)
	require.True(t, deleted)
	require.Equal(t, 1, removed)
	requireNotParked(t, registry, ParkedXorb, "xorb-a")
}

func TestRestoreFinishesRestoringKey(t *testing.T) {
	ctx := context.Background()
	store := kvtest.GetStore(ctx, t)
	registry := NewRegistry(store)
	setParked(t, store, ParkedXorb, "xorb-a", Parked{State: StateRestoring, ParkedAt: parkedNow.Unix(), Data: []byte("footer")})

	result, err := registry.Restore(ctx, ParkedXorb, "xorb-a", nil, false)
	require.NoError(t, err)
	require.Equal(t, Restored, result)
	footer, err := registry.GetXorbInfo(ctx, "xorb-a")
	require.NoError(t, err)
	require.Equal(t, []byte("footer"), footer)
	requireNotParked(t, registry, ParkedXorb, "xorb-a")
}

func TestRestoreClaimsOrphanXorb(t *testing.T) {
	ctx := context.Background()
	registry := NewRegistry(kvtest.GetStore(ctx, t))
	rebuild := func() ([]byte, error) { return []byte("rebuilt"), nil }

	result, err := registry.Restore(ctx, ParkedXorb, "xorb-a", rebuild, false)
	require.NoError(t, err)
	require.Equal(t, NotParked, result)
	_, err = registry.GetXorbInfo(ctx, "xorb-a")
	require.ErrorIs(t, err, kv.ErrNotFound)

	result, err = registry.Restore(ctx, ParkedXorb, "xorb-a", rebuild, true)
	require.NoError(t, err)
	require.Equal(t, Restored, result)
	footer, err := registry.GetXorbInfo(ctx, "xorb-a")
	require.NoError(t, err)
	require.Equal(t, []byte("rebuilt"), footer)
	requireNotParked(t, registry, ParkedXorb, "xorb-a")
}

func TestRestoreDatalessParkedXorbRebuilds(t *testing.T) {
	ctx := context.Background()
	registry := NewRegistry(kvtest.GetStore(ctx, t))
	parked, err := registry.ParkOrphanXorb(ctx, "xorb-a", parkedNow)
	require.NoError(t, err)
	require.True(t, parked)

	result, err := registry.Restore(ctx, ParkedXorb, "xorb-a", func() ([]byte, error) { return []byte("rebuilt"), nil }, false)
	require.NoError(t, err)
	require.Equal(t, Restored, result)
	footer, err := registry.GetXorbInfo(ctx, "xorb-a")
	require.NoError(t, err)
	require.Equal(t, []byte("rebuilt"), footer)
}

func TestGetFileRestoringBringsBackParkedFile(t *testing.T) {
	ctx := context.Background()
	registry := NewRegistry(kvtest.GetStore(ctx, t))
	_, err := registry.RegisterFile(ctx, ShardFileInfo{FileHash: "file-a", SizeBytes: 12})
	require.NoError(t, err)
	_, err = registry.Park(ctx, ParkedFile, "file-a", parkedNow)
	require.NoError(t, err)

	file, err := registry.GetFileRestoring(ctx, "file-a")
	require.NoError(t, err)
	require.Equal(t, uint64(12), file.SizeBytes)
	_, err = registry.GetFileRestoring(ctx, "file-missing")
	require.ErrorIs(t, err, kv.ErrNotFound)
}

func TestGetXorbInfoRestoringBringsBackParkedFooter(t *testing.T) {
	ctx := context.Background()
	registry := NewRegistry(kvtest.GetStore(ctx, t))
	require.NoError(t, registry.PutXorbInfo(ctx, "xorb-a", []byte("footer")))
	_, err := registry.Park(ctx, ParkedXorb, "xorb-a", parkedNow)
	require.NoError(t, err)

	footer, err := registry.GetXorbInfoRestoring(ctx, "xorb-a", nil)
	require.NoError(t, err)
	require.Equal(t, []byte("footer"), footer)
}

func setParked(t *testing.T, store kv.Store, kind ParkedKind, hash string, parked Parked) {
	t.Helper()
	data, err := json.Marshal(parked)
	require.NoError(t, err)
	require.NoError(t, store.Set(context.Background(), []byte(Partition), parkedKey(kind, hash), data))
}

func getParked(t *testing.T, registry *Registry, kind ParkedKind, hash string) Parked {
	t.Helper()
	list, err := registry.ListParked(context.Background(), kind, 0)
	require.NoError(t, err)
	parked, ok := list[hash]
	require.True(t, ok, "%s is not parked", hash)
	return parked
}

func requireNotParked(t *testing.T, registry *Registry, kind ParkedKind, hash string) {
	t.Helper()
	list, err := registry.ListParked(context.Background(), kind, 0)
	require.NoError(t, err)
	_, ok := list[hash]
	require.False(t, ok, "%s is still parked", hash)
}

func TestGCLockTakesOverExpiredLock(t *testing.T) {
	ctx := context.Background()
	registry := NewRegistry(kvtest.GetStore(ctx, t))
	_, err := registry.AcquireGCLock(ctx, parkedNow, time.Hour)
	require.NoError(t, err)

	_, err = registry.AcquireGCLock(ctx, parkedNow.Add(59*time.Minute), time.Hour)
	require.ErrorIs(t, err, ErrGCLocked)

	release, err := registry.AcquireGCLock(ctx, parkedNow.Add(61*time.Minute), time.Hour)
	require.NoError(t, err)
	require.NoError(t, release(ctx))
	_, err = registry.AcquireGCLock(ctx, parkedNow.Add(62*time.Minute), time.Hour)
	require.NoError(t, err)
}

func TestResumedDeleteKeepsReuploadedXorb(t *testing.T) {
	ctx := context.Background()
	store := kvtest.GetStore(ctx, t)
	registry := NewRegistry(store)
	setParked(t, store, ParkedXorb, "xorb-a", Parked{State: StateDeleting, ParkedAt: parkedNow.Unix(), Data: []byte("old")})
	require.NoError(t, registry.PutXorbInfo(ctx, "xorb-a", []byte("reuploaded")))

	deleted, err := registry.DeleteParked(ctx, ParkedXorb, "xorb-a", func() error {
		t.Fatal("must not remove a xorb that was uploaded again")
		return nil
	})

	require.NoError(t, err)
	require.True(t, deleted)
	requireNotParked(t, registry, ParkedXorb, "xorb-a")
	footer, err := registry.GetXorbInfo(ctx, "xorb-a")
	require.NoError(t, err)
	require.Equal(t, []byte("reuploaded"), footer)
}

// failParkedDelete fails deletes of parked keys, as a run killed after removing an object would.
type failParkedDelete struct {
	kv.Store
	fail bool
}

func (s *failParkedDelete) Delete(ctx context.Context, partitionKey, key []byte) error {
	if s.fail && strings.HasPrefix(string(key), "xet/gc/") {
		return errors.New("killed")
	}
	return s.Store.Delete(ctx, partitionKey, key)
}

func TestDeleteParkedNeverRemovesAnObjectTwice(t *testing.T) {
	ctx := context.Background()
	store := &failParkedDelete{Store: kvtest.GetStore(ctx, t), fail: true}
	registry := NewRegistry(store)
	require.NoError(t, registry.PutXorbInfo(ctx, "xorb-a", []byte("footer")))
	_, err := registry.Park(ctx, ParkedXorb, "xorb-a", parkedNow)
	require.NoError(t, err)
	removed := 0
	remove := func() error { removed++; return nil }

	_, err = registry.DeleteParked(ctx, ParkedXorb, "xorb-a", remove)
	require.Error(t, err)
	store.fail = false
	deleted, err := registry.DeleteParked(ctx, ParkedXorb, "xorb-a", remove)

	require.NoError(t, err)
	require.True(t, deleted)
	require.Equal(t, 1, removed)
	requireNotParked(t, registry, ParkedXorb, "xorb-a")
}
