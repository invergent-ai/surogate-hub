package stats

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/invergent-ai/surogate-hub/pkg/kv"
	"github.com/invergent-ai/surogate-hub/pkg/logging"
)

// ReconcilerRepo is the minimal repo description the reconciler needs.
type ReconcilerRepo struct {
	Owner            string
	Name             string
	StorageID        string
	StorageNamespace string
}

// RepoLister enumerates repositories for the reconciler. Implementations may stream or paginate.
type RepoLister interface {
	ListRepos(ctx context.Context, fn func(ReconcilerRepo) error) error
}

// NamespaceSizer reports the total bytes allocated in a repo's storage namespace.
type NamespaceSizer interface {
	NamespaceSize(ctx context.Context, storageID, storageNamespace string) (int64, error)
}

// StorageReconciler walks every repo, asks the block adapter how many bytes are allocated, and
// overwrites the per-repo counter accordingly. After all of an owner's repos are processed, the
// per-user denormalized total is rewritten from the sum of repo counters.
type StorageReconciler struct {
	storage     kv.Store
	lister      RepoLister
	sizer       NamespaceSizer
	accountant  *StorageAccountant
	concurrency int

	mu    sync.Mutex
	locks map[string]*sync.Mutex // per-repo locks, keyed by "owner/name"
}

// NewStorageReconciler constructs a reconciler. Defaults to single-threaded; set Concurrency to
// fan out across repos.
func NewStorageReconciler(storage kv.Store, lister RepoLister, sizer NamespaceSizer, accountant *StorageAccountant) *StorageReconciler {
	return &StorageReconciler{
		storage:     storage,
		lister:      lister,
		sizer:       sizer,
		accountant:  accountant,
		concurrency: 1,
		locks:       make(map[string]*sync.Mutex),
	}
}

// WithConcurrency sets how many repos to reconcile in parallel. Returns the reconciler for chaining.
func (r *StorageReconciler) WithConcurrency(n int) *StorageReconciler {
	if n < 1 {
		n = 1
	}
	r.concurrency = n
	return r
}

// Start launches a goroutine that calls RunOnce every interval until ctx is cancelled. If a pass
// takes longer than the interval, the next pass starts immediately after the previous one finishes.
func (r *StorageReconciler) Start(ctx context.Context, interval time.Duration, logger logging.Logger) {
	if interval == 0 {
		return
	}
	go func() {
		for {
			start := time.Now()
			if err := r.RunOnce(ctx); err != nil {
				logger.WithError(err).Error("storage reconciler pass failed")
			}
			delay := interval - time.Since(start)
			if delay < 0 {
				delay = 0
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(delay):
			}
		}
	}()
}

// RunOnce reconciles every repo once. Errors are collected and returned as a joined error after
// every repo has been attempted.
func (r *StorageReconciler) RunOnce(ctx context.Context) error {
	// Flush accountant deltas before walking, so the per-user counter we will compute by summing
	// repo counters reflects any pending writes. We tolerate the flush error: if a per-repo
	// counter write fails, the per-repo overwrite below (or a future reconciler pass) will
	// converge to the correct value.
	_ = r.accountant.Flush(ctx)

	type repoResult struct {
		owner string
		err   error
	}
	results := make(chan repoResult, r.concurrency)
	work := make(chan ReconcilerRepo)

	var wg sync.WaitGroup
	for i := 0; i < r.concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for repo := range work {
				err := r.reconcileRepo(ctx, repo)
				results <- repoResult{owner: repo.Owner, err: err}
			}
		}()
	}

	listerDone := make(chan error, 1)
	go func() {
		listerDone <- r.lister.ListRepos(ctx, func(repo ReconcilerRepo) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case work <- repo:
				return nil
			}
		})
		close(work)
	}()

	go func() {
		wg.Wait()
		close(results)
	}()

	ownersTouched := make(map[string]struct{})
	var errs []error
	for res := range results {
		if res.err != nil {
			errs = append(errs, fmt.Errorf("reconcile %s: %w", res.owner, res.err))
		}
		ownersTouched[res.owner] = struct{}{}
	}
	if err := <-listerDone; err != nil {
		errs = append(errs, fmt.Errorf("list repos: %w", err))
	}

	// Recompute per-user totals and write last_reconciled_at.
	now := time.Now().UTC()
	for owner := range ownersTouched {
		if err := r.recomputeUserTotal(ctx, owner, now); err != nil {
			errs = append(errs, fmt.Errorf("user total %s: %w", owner, err))
		}
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

func (r *StorageReconciler) repoLock(owner, name string) *sync.Mutex {
	key := owner + "/" + name
	r.mu.Lock()
	defer r.mu.Unlock()
	if m, ok := r.locks[key]; ok {
		return m
	}
	m := &sync.Mutex{}
	r.locks[key] = m
	return m
}

func (r *StorageReconciler) reconcileRepo(ctx context.Context, repo ReconcilerRepo) error {
	lock := r.repoLock(repo.Owner, repo.Name)
	lock.Lock()
	defer lock.Unlock()

	size, err := r.sizer.NamespaceSize(ctx, repo.StorageID, repo.StorageNamespace)
	if err != nil {
		return err
	}
	return r.storage.Set(ctx, StoragePartition, StorageRepoKey(repo.Owner, repo.Name), []byte(strconv.FormatInt(size, 10)))
}

func (r *StorageReconciler) recomputeUserTotal(ctx context.Context, owner string, now time.Time) error {
	prefix := StorageRepoPrefix(owner)
	iter, err := r.storage.Scan(ctx, StoragePartition, kv.ScanOptions{KeyStart: prefix})
	if err != nil {
		return err
	}
	defer iter.Close()
	var total int64
	for iter.Next() {
		ent := iter.Entry()
		if !bytes.HasPrefix(ent.Key, prefix) {
			break
		}
		n, parseErr := strconv.ParseInt(string(ent.Value), 10, 64)
		if parseErr != nil {
			return fmt.Errorf("parse %q: %w", ent.Key, parseErr)
		}
		total += n
	}
	if err := iter.Err(); err != nil {
		return err
	}
	if err := r.storage.Set(ctx, StoragePartition, StorageUserKey(owner), []byte(strconv.FormatInt(total, 10))); err != nil {
		return err
	}
	return r.storage.Set(ctx, StoragePartition, StorageMetaLastReconciledAtKey(owner), []byte(now.Format(time.RFC3339)))
}
