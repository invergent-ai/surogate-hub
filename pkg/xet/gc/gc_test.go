package gc

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/invergent-ai/surogate-hub/pkg/kv"
	"github.com/invergent-ai/surogate-hub/pkg/kv/kvtest"
	xetstore "github.com/invergent-ai/surogate-hub/pkg/xet/store"
	"github.com/stretchr/testify/require"
)

var (
	sweepNow = time.Unix(2_000_000_000, 0)
	oldTime  = sweepNow.Add(-48 * time.Hour)
	young    = sweepNow.Add(-time.Hour)
)

type fixture struct {
	ctx      context.Context
	store    kv.Store
	registry *xetstore.Registry
	repos    map[string]error // nil error = exists; missing key = not found
	objects  []XorbRef
	listErr  error
	removed  []string
	now      time.Time
}

func newFixture(t *testing.T) *fixture {
	ctx := context.Background()
	store := kvtest.GetStore(ctx, t)
	return &fixture{ctx: ctx, store: store, registry: xetstore.NewRegistry(store), repos: map[string]error{}, now: sweepNow}
}

func (f *fixture) params(dryRun bool) Params {
	return Params{
		Registry: f.registry,
		RepoExists: func(_ context.Context, repo string) (bool, error) {
			err, ok := f.repos[repo]
			return ok && err == nil, err
		},
		ListXorbs: func(context.Context) ([]XorbRef, error) { return f.objects, f.listErr },
		RemoveXorb: func(_ context.Context, hash string) error {
			f.removed = append(f.removed, hash)
			return nil
		},
		StoredFooter: func(_ context.Context, hash string) ([]byte, error) { return []byte("rebuilt-" + hash), nil },
		FileMinAge:   24 * time.Hour,
		XorbMinAge:   168 * time.Hour,
		Now:          func() time.Time { return f.now },
		DryRun:       dryRun,
	}
}

func (f *fixture) sweep(t *testing.T) Report {
	t.Helper()
	report, err := Sweep(f.ctx, f.params(false))
	require.NoError(t, err)
	return report
}

// file registers a file record whose single segment uses xorb, with a footer and an object.
func (f *fixture) file(t *testing.T, hash, xorb string, registeredAt time.Time) {
	t.Helper()
	_, err := f.registry.RegisterFile(f.ctx, xetstore.ShardFileInfo{
		FileHash: hash, SizeBytes: 5, RegisteredAt: registeredAt.Unix(),
		Segments: []xetstore.ShardFileSegment{{XorbHash: xorb, SizeBytes: 5, ChunkIndexEnd: 1}},
	})
	require.NoError(t, err)
	f.xorb(t, xorb, registeredAt)
}

func (f *fixture) xorb(t *testing.T, hash string, mtime time.Time) {
	t.Helper()
	require.NoError(t, f.registry.PutXorbInfo(f.ctx, hash, []byte("footer-"+hash)))
	f.objects = append(f.objects, XorbRef{Prefix: "default", Hash: hash, Size: 100, Mtime: mtime})
}

func (f *fixture) hold(t *testing.T, repo, file string) {
	t.Helper()
	f.repos[repo] = nil
	_, err := f.registry.AddFileRef(f.ctx, xetstore.FileRef{FileHash: file, Repo: repo, SizeBytes: 5})
	require.NoError(t, err)
}

func hashes(items []Item) []string {
	out := []string{}
	for _, item := range items {
		out = append(out, item.Hash)
	}
	return out
}

func requireLiveFile(t *testing.T, f *fixture, hash string, live bool) {
	t.Helper()
	_, err := f.registry.GetFile(f.ctx, hash)
	if live {
		require.NoError(t, err)
	} else {
		require.ErrorIs(t, err, kv.ErrNotFound)
	}
}

func requireLiveFooter(t *testing.T, f *fixture, hash string, live bool) {
	t.Helper()
	_, err := f.registry.GetXorbInfo(f.ctx, hash)
	if live {
		require.NoError(t, err)
	} else {
		require.ErrorIs(t, err, kv.ErrNotFound)
	}
}

func TestSweepDeletesRefsOnlyForMissingRepositories(t *testing.T) {
	f := newFixture(t)
	f.file(t, "file-a", "xorb-a", oldTime)
	f.hold(t, "o/live", "file-a")
	f.hold(t, "o/gone", "file-a")
	delete(f.repos, "o/gone")

	report := f.sweep(t)

	require.Equal(t, []xetstore.FileRef{{FileHash: "file-a", Repo: "o/gone", SizeBytes: 5}}, report.StaleRefs)
	refs, err := f.registry.ListAllFileRefs(f.ctx, 0)
	require.NoError(t, err)
	require.Equal(t, []xetstore.FileRef{{FileHash: "file-a", Repo: "o/live", SizeBytes: 5}}, refs)
	requireLiveFile(t, f, "file-a", true)
}

func TestSweepKeepsRefWhenRepositoryLookupFails(t *testing.T) {
	f := newFixture(t)
	f.file(t, "file-a", "xorb-a", oldTime)
	f.hold(t, "o/flaky", "file-a")
	f.repos["o/flaky"] = errors.New("kv timeout")

	report := f.sweep(t)

	require.Empty(t, report.StaleRefs)
	require.NotEmpty(t, report.Errors)
	held, err := f.registry.HasFileRef(f.ctx, "o/flaky", "file-a")
	require.NoError(t, err)
	require.True(t, held)
	requireLiveFile(t, f, "file-a", true)
}

func TestSweepParksUnheldOldFilesOnly(t *testing.T) {
	f := newFixture(t)
	f.file(t, "file-held", "xorb-1", oldTime)
	f.hold(t, "o/r", "file-held")
	f.file(t, "file-young", "xorb-2", young)
	f.file(t, "file-old", "xorb-3", oldTime)
	f.file(t, "file-legacy", "xorb-4", time.Unix(0, 0))

	report := f.sweep(t)

	require.ElementsMatch(t, []string{"file-old", "file-legacy"}, hashes(report.FilesParked))
	requireLiveFile(t, f, "file-held", true)
	requireLiveFile(t, f, "file-young", true)
	requireLiveFile(t, f, "file-old", false)
}

func TestSweepRestoresParkedFileThatBecameHeld(t *testing.T) {
	f := newFixture(t)
	f.file(t, "file-a", "xorb-a", oldTime)
	f.sweep(t)
	requireLiveFile(t, f, "file-a", false)
	f.hold(t, "o/r", "file-a")

	report := f.sweep(t)

	require.Equal(t, []string{"file-a"}, hashes(report.FilesRestored))
	requireLiveFile(t, f, "file-a", true)
}

func TestSweepDeletesParkedFileAfterMinAge(t *testing.T) {
	f := newFixture(t)
	f.file(t, "file-a", "xorb-a", oldTime)
	f.sweep(t)

	f.now = sweepNow.Add(23 * time.Hour)
	require.Empty(t, f.sweep(t).FilesDeleted)

	f.now = sweepNow.Add(24 * time.Hour)
	report := f.sweep(t)
	require.Equal(t, []string{"file-a"}, hashes(report.FilesDeleted))
	parked, err := f.registry.ListParked(f.ctx, xetstore.ParkedFile, 0)
	require.NoError(t, err)
	require.Empty(t, parked)
}

func TestSweepParksUnusedXorbs(t *testing.T) {
	f := newFixture(t)
	f.file(t, "file-held", "xorb-used", oldTime)
	f.hold(t, "o/r", "file-held")
	f.xorb(t, "xorb-old", oldTime)
	f.xorb(t, "xorb-young", young)

	report := f.sweep(t)

	require.Equal(t, []string{"xorb-old"}, hashes(report.XorbsParked))
	requireLiveFooter(t, f, "xorb-used", true)
	requireLiveFooter(t, f, "xorb-young", true)
	requireLiveFooter(t, f, "xorb-old", false)
}

func TestSweepDeletesParkedXorbAfterXorbMinAge(t *testing.T) {
	f := newFixture(t)
	f.xorb(t, "xorb-a", oldTime)
	f.sweep(t)

	f.now = sweepNow.Add(167 * time.Hour)
	require.Empty(t, f.sweep(t).XorbsDeleted)
	require.Empty(t, f.removed)

	f.now = sweepNow.Add(168 * time.Hour)
	report := f.sweep(t)
	require.Equal(t, []Item{{Hash: "xorb-a", Bytes: 100}}, report.XorbsDeleted)
	require.Equal(t, []string{"xorb-a"}, f.removed)
	requireLiveFooter(t, f, "xorb-a", false)
	parked, err := f.registry.ListParked(f.ctx, xetstore.ParkedXorb, 0)
	require.NoError(t, err)
	require.Empty(t, parked)
}

func TestSweepRestoresParkedXorbThatBecameUsed(t *testing.T) {
	f := newFixture(t)
	f.xorb(t, "xorb-a", oldTime)
	f.sweep(t)
	requireLiveFooter(t, f, "xorb-a", false)
	_, err := f.registry.RegisterFile(f.ctx, xetstore.ShardFileInfo{FileHash: "file-new", SizeBytes: 5, RegisteredAt: young.Unix(),
		Segments: []xetstore.ShardFileSegment{{XorbHash: "xorb-a", SizeBytes: 5, ChunkIndexEnd: 1}}})
	require.NoError(t, err)

	report := f.sweep(t)

	require.Equal(t, []string{"xorb-a"}, hashes(report.XorbsRestored))
	requireLiveFooter(t, f, "xorb-a", true)
}

func TestSweepParksOrphanObjectsThenDeletesThem(t *testing.T) {
	f := newFixture(t)
	f.objects = append(f.objects, XorbRef{Prefix: "default", Hash: "xorb-orphan", Size: 7, Mtime: oldTime})

	report := f.sweep(t)
	require.Equal(t, []string{"xorb-orphan"}, hashes(report.OrphansParked))

	f.now = sweepNow.Add(168 * time.Hour)
	report = f.sweep(t)
	require.Equal(t, []Item{{Hash: "xorb-orphan", Bytes: 7}}, report.XorbsDeleted)
	require.Equal(t, []string{"xorb-orphan"}, f.removed)
}

func TestSweepFinishesInterruptedKeys(t *testing.T) {
	f := newFixture(t)
	f.file(t, "file-a", "xorb-a", oldTime)
	f.hold(t, "o/r", "file-a")
	_, err := f.registry.Park(f.ctx, xetstore.ParkedXorb, "xorb-a", f.now)
	require.NoError(t, err)
	_, err = f.registry.Park(f.ctx, xetstore.ParkedFile, "file-a", f.now)
	require.NoError(t, err)
	f.xorb(t, "xorb-b", oldTime)
	_, err = f.registry.Park(f.ctx, xetstore.ParkedXorb, "xorb-b", f.now)
	require.NoError(t, err)
	_, err = f.registry.DeleteParked(f.ctx, xetstore.ParkedXorb, "xorb-b", func() error { return errors.New("crash") })
	require.Error(t, err)

	report := f.sweep(t)

	require.ElementsMatch(t, []string{"xorb-b"}, hashes(report.Finished))
	require.Contains(t, f.removed, "xorb-b")
	requireLiveFile(t, f, "file-a", true)
	requireLiveFooter(t, f, "xorb-a", true)
}

func TestSweepStopsWhenXorbListingFails(t *testing.T) {
	f := newFixture(t)
	f.xorb(t, "xorb-a", oldTime)
	f.listErr = errors.New("walker not supported")

	_, err := Sweep(f.ctx, f.params(false))

	require.Error(t, err)
	requireLiveFooter(t, f, "xorb-a", true)
}

func TestSweepDryRunWritesNothing(t *testing.T) {
	f := newFixture(t)
	f.file(t, "file-old", "xorb-old", oldTime)
	f.xorb(t, "xorb-unused", oldTime)
	f.hold(t, "o/gone", "file-old")
	delete(f.repos, "o/gone")
	before := kvSnapshot(t, f)

	dry, err := Sweep(f.ctx, f.params(true))
	require.NoError(t, err)

	require.Equal(t, before, kvSnapshot(t, f))
	require.Empty(t, f.removed)
	real := f.sweep(t)
	dry.DryRun = false
	require.Equal(t, real, dry)
}

func TestSweepRefusesWhileLocked(t *testing.T) {
	f := newFixture(t)
	_, err := f.registry.AcquireGCLock(f.ctx, f.now, time.Hour)
	require.NoError(t, err)

	_, err = Sweep(f.ctx, f.params(false))

	require.ErrorIs(t, err, xetstore.ErrGCLocked)
}

func kvSnapshot(t *testing.T, f *fixture) map[string]string {
	t.Helper()
	iter, err := f.store.Scan(f.ctx, []byte(xetstore.Partition), kv.ScanOptions{})
	require.NoError(t, err)
	defer iter.Close()
	out := map[string]string{}
	for iter.Next() {
		out[string(iter.Entry().Key)] = string(iter.Entry().Value)
	}
	return out
}
