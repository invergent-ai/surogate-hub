package gc

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"time"

	xetstore "github.com/invergent-ai/surogate-hub/pkg/xet/store"
)

// lockTTL bounds how long a run that died without releasing its lock blocks the next.
const lockTTL = 6 * time.Hour

// xorbPrefix is the only CAS prefix hf_xet writes; objects under any other are left alone.
const xorbPrefix = "default"

type XorbRef struct {
	Prefix string
	Hash   string
	Size   int64
	Mtime  time.Time
}

type Params struct {
	Registry      *xetstore.Registry
	ScanBatchSize int
	// RepoExists reports whether a repository exists; an error keeps its refs.
	RepoExists func(ctx context.Context, repo string) (bool, error)
	// ListXorbs lists the xorb objects in the blockstore with their size and age.
	ListXorbs    func(ctx context.Context) ([]XorbRef, error)
	RemoveXorb   func(ctx context.Context, hash string) error
	StoredFooter func(ctx context.Context, hash string) ([]byte, error)
	// FileMinAge is how long a file record stays parked, and how old a file
	// record or xorb object must be before it is parked.
	FileMinAge time.Duration
	// XorbMinAge is how long a xorb stays parked before its object is deleted.
	XorbMinAge time.Duration
	Now        func() time.Time
	DryRun     bool
}

type Item struct {
	Hash  string `json:"hash"`
	Bytes int64  `json:"bytes"`
}

type Report struct {
	DryRun        bool               `json:"dry_run"`
	StaleRefs     []xetstore.FileRef `json:"stale_refs"`
	Finished      []Item             `json:"finished"`
	FilesRestored []Item             `json:"files_restored"`
	FilesDeleted  []Item             `json:"files_deleted"`
	FilesParked   []Item             `json:"files_parked"`
	XorbsRestored []Item             `json:"xorbs_restored"`
	XorbsDeleted  []Item             `json:"xorbs_deleted"`
	XorbsParked   []Item             `json:"xorbs_parked"`
	OrphansParked []Item             `json:"orphans_parked"`
	Errors        []string           `json:"errors"`
}

// Sweep removes xet data no existing repository holds, in two phases: a run
// parks what is unneeded, and a later run deletes what is still unneeded after
// a grace period. Every listing is taken before anything changes, so a dry run
// reports exactly what a real run does. A failed listing aborts the run; a
// failure on a single item is reported and the run continues.
func Sweep(ctx context.Context, params Params) (Report, error) {
	now := time.Now
	if params.Now != nil {
		now = params.Now
	}
	s := &sweep{params: params, now: now(), report: Report{DryRun: params.DryRun}}
	if !params.DryRun {
		release, err := params.Registry.AcquireGCLock(ctx, s.now, lockTTL)
		if err != nil {
			return Report{}, err
		}
		defer func() { _ = release(ctx) }()
	}
	if err := s.run(ctx); err != nil {
		return Report{}, err
	}
	return s.report, nil
}

type sweep struct {
	params Params
	now    time.Time
	report Report
}

type snapshot struct {
	held        map[string]struct{}
	parkedFiles map[string]xetstore.Parked
	parkedXorbs map[string]xetstore.Parked
	files       []xetstore.ShardFileInfo
	footers     map[string]struct{}
	objects     map[string]XorbRef
}

func (s *sweep) run(ctx context.Context) error {
	held, err := s.refs(ctx)
	if err != nil {
		return err
	}
	snap, err := s.list(ctx, held)
	if err != nil {
		return err
	}
	s.finishInterrupted(ctx, snap)
	live := s.files(ctx, snap)
	s.xorbs(ctx, snap, usedXorbs(live))
	return nil
}

// refs deletes refs of repositories that no longer exist and returns the held set.
func (s *sweep) refs(ctx context.Context) (map[string]struct{}, error) {
	refs, err := s.params.Registry.ListAllFileRefs(ctx, s.params.ScanBatchSize)
	if err != nil {
		return nil, fmt.Errorf("list file refs: %w", err)
	}
	held := make(map[string]struct{})
	for _, ref := range refs {
		exists, err := s.params.RepoExists(ctx, ref.Repo)
		if err != nil {
			s.fail("repository %s: %v", ref.Repo, err)
			held[ref.FileHash] = struct{}{}
			continue
		}
		if exists {
			held[ref.FileHash] = struct{}{}
			continue
		}
		s.report.StaleRefs = append(s.report.StaleRefs, ref)
		if s.params.DryRun {
			continue
		}
		if err := s.params.Registry.DeleteFileRef(ctx, ref); err != nil {
			s.fail("delete ref %s/%s: %v", ref.Repo, ref.FileHash, err)
			held[ref.FileHash] = struct{}{}
		}
	}
	return held, nil
}

func (s *sweep) list(ctx context.Context, held map[string]struct{}) (snapshot, error) {
	snap := snapshot{held: held, footers: make(map[string]struct{}), objects: make(map[string]XorbRef)}
	var err error
	if snap.parkedFiles, err = s.params.Registry.ListParked(ctx, xetstore.ParkedFile, s.params.ScanBatchSize); err != nil {
		return snapshot{}, fmt.Errorf("list parked files: %w", err)
	}
	if snap.parkedXorbs, err = s.params.Registry.ListParked(ctx, xetstore.ParkedXorb, s.params.ScanBatchSize); err != nil {
		return snapshot{}, fmt.Errorf("list parked xorbs: %w", err)
	}
	if snap.files, err = s.params.Registry.ListFiles(ctx, s.params.ScanBatchSize); err != nil {
		return snapshot{}, fmt.Errorf("list files: %w", err)
	}
	footers, err := s.params.Registry.ListXorbFooterHashes(ctx, s.params.ScanBatchSize)
	if err != nil {
		return snapshot{}, fmt.Errorf("list xorb footers: %w", err)
	}
	for _, hash := range footers {
		snap.footers[hash] = struct{}{}
	}
	objects, err := s.params.ListXorbs(ctx)
	if err != nil {
		return snapshot{}, fmt.Errorf("list xorb objects: %w", err)
	}
	for _, object := range objects {
		if object.Prefix == xorbPrefix {
			snap.objects[object.Hash] = object
		}
	}
	return snap, nil
}

// finishInterrupted completes parked keys a previous run or request left
// mid-restore or mid-delete, and drops them from the snapshot.
func (s *sweep) finishInterrupted(ctx context.Context, snap snapshot) {
	for _, kind := range []xetstore.ParkedKind{xetstore.ParkedFile, xetstore.ParkedXorb} {
		parked := snap.parkedFiles
		if kind == xetstore.ParkedXorb {
			parked = snap.parkedXorbs
		}
		for _, hash := range sortedKeys(parked) {
			p := parked[hash]
			switch p.State {
			case xetstore.StateRestoring:
				if s.restore(ctx, kind, hash) {
					s.report.Finished = append(s.report.Finished, Item{Hash: hash})
				}
			case xetstore.StateDeleting:
				if s.delete(ctx, kind, hash) {
					s.report.Finished = append(s.report.Finished, Item{Hash: hash, Bytes: snap.objects[hash].Size})
				}
			default:
				continue
			}
			delete(parked, hash)
		}
	}
}

// files restores parked files that are held again, deletes those parked long
// enough, parks old unheld ones, and returns the file records live after that.
func (s *sweep) files(ctx context.Context, snap snapshot) []xetstore.ShardFileInfo {
	var live []xetstore.ShardFileInfo
	for _, hash := range sortedKeys(snap.parkedFiles) {
		p := snap.parkedFiles[hash]
		var file xetstore.ShardFileInfo
		if err := json.Unmarshal(p.Data, &file); err != nil {
			s.fail("decode parked file %s: %v", hash, err)
			continue
		}
		if _, ok := snap.held[hash]; ok {
			if s.restore(ctx, xetstore.ParkedFile, hash) {
				s.report.FilesRestored = append(s.report.FilesRestored, Item{Hash: hash, Bytes: int64(file.SizeBytes)})
				live = append(live, file)
			}
			continue
		}
		if s.parkedFor(p) >= s.params.FileMinAge && s.delete(ctx, xetstore.ParkedFile, hash) {
			s.report.FilesDeleted = append(s.report.FilesDeleted, Item{Hash: hash, Bytes: int64(file.SizeBytes)})
		}
	}
	for _, file := range snap.files {
		_, held := snap.held[file.FileHash]
		if !held && s.olderThan(time.Unix(file.RegisteredAt, 0), s.params.FileMinAge) && s.park(ctx, xetstore.ParkedFile, file.FileHash) {
			s.report.FilesParked = append(s.report.FilesParked, Item{Hash: file.FileHash, Bytes: int64(file.SizeBytes)})
			continue
		}
		live = append(live, file)
	}
	return live
}

// xorbs restores parked xorbs that are used again, deletes those parked long
// enough, parks unused ones whose object is old, and parks old objects that
// have no footer at all.
func (s *sweep) xorbs(ctx context.Context, snap snapshot, used map[string]struct{}) {
	for _, hash := range sortedKeys(snap.parkedXorbs) {
		p := snap.parkedXorbs[hash]
		if _, ok := used[hash]; ok {
			if s.restore(ctx, xetstore.ParkedXorb, hash) {
				s.report.XorbsRestored = append(s.report.XorbsRestored, Item{Hash: hash, Bytes: snap.objects[hash].Size})
			}
			continue
		}
		if s.parkedFor(p) >= s.params.XorbMinAge && s.delete(ctx, xetstore.ParkedXorb, hash) {
			s.report.XorbsDeleted = append(s.report.XorbsDeleted, Item{Hash: hash, Bytes: snap.objects[hash].Size})
		}
	}
	for _, hash := range sortedKeys(snap.footers) {
		object, ok := snap.objects[hash]
		if _, inUse := used[hash]; inUse || !ok || !s.olderThan(object.Mtime, s.params.FileMinAge) {
			continue
		}
		if s.park(ctx, xetstore.ParkedXorb, hash) {
			s.report.XorbsParked = append(s.report.XorbsParked, Item{Hash: hash, Bytes: object.Size})
		}
	}
	for _, hash := range sortedKeys(snap.objects) {
		object := snap.objects[hash]
		_, hasFooter := snap.footers[hash]
		_, isParked := snap.parkedXorbs[hash]
		_, inUse := used[hash]
		if hasFooter || isParked || inUse || !s.olderThan(object.Mtime, s.params.FileMinAge) {
			continue
		}
		if s.parkOrphan(ctx, hash) {
			s.report.OrphansParked = append(s.report.OrphansParked, Item{Hash: hash, Bytes: object.Size})
		}
	}
}

func (s *sweep) park(ctx context.Context, kind xetstore.ParkedKind, hash string) bool {
	if s.params.DryRun {
		return true
	}
	parked, err := s.params.Registry.Park(ctx, kind, hash, s.now)
	if err != nil {
		s.fail("park %s %s: %v", kind, hash, err)
	}
	return parked
}

func (s *sweep) parkOrphan(ctx context.Context, hash string) bool {
	if s.params.DryRun {
		return true
	}
	parked, err := s.params.Registry.ParkOrphanXorb(ctx, hash, s.now)
	if err != nil {
		s.fail("park orphan xorb %s: %v", hash, err)
	}
	return parked
}

func (s *sweep) restore(ctx context.Context, kind xetstore.ParkedKind, hash string) bool {
	if s.params.DryRun {
		return true
	}
	result, err := s.params.Registry.Restore(ctx, kind, hash, func() ([]byte, error) {
		return s.params.StoredFooter(ctx, hash)
	}, false)
	if err != nil {
		s.fail("restore %s %s: %v", kind, hash, err)
		return false
	}
	return result == xetstore.Restored
}

func (s *sweep) delete(ctx context.Context, kind xetstore.ParkedKind, hash string) bool {
	if s.params.DryRun {
		return true
	}
	var removeObject func() error
	if kind == xetstore.ParkedXorb {
		removeObject = func() error { return s.params.RemoveXorb(ctx, hash) }
	}
	deleted, err := s.params.Registry.DeleteParked(ctx, kind, hash, removeObject)
	if err != nil {
		s.fail("delete %s %s: %v", kind, hash, err)
	}
	return deleted
}

func (s *sweep) parkedFor(p xetstore.Parked) time.Duration {
	return s.now.Sub(time.Unix(p.ParkedAt, 0))
}

func (s *sweep) olderThan(t time.Time, age time.Duration) bool {
	return s.now.Sub(t) >= age
}

func (s *sweep) fail(format string, args ...any) {
	s.report.Errors = append(s.report.Errors, fmt.Sprintf(format, args...))
}

// sortedKeys keeps the report, and the order of work, the same from run to run.
func sortedKeys[V any](m map[string]V) []string {
	return slices.Sorted(maps.Keys(m))
}

func usedXorbs(files []xetstore.ShardFileInfo) map[string]struct{} {
	used := make(map[string]struct{})
	for _, file := range files {
		for _, segment := range file.Segments {
			used[segment.XorbHash] = struct{}{}
		}
	}
	return used
}
