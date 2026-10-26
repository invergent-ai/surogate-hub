// Package storagewiring contains the production implementations of stats.RepoLister and
// stats.RepoSizer that bridge the StorageReconciler to the catalog and block adapter.
// It lives in its own subpackage because pkg/stats sits below pkg/catalog and pkg/block/s3 in the
// dependency graph (those packages already import pkg/stats for usage counters), so the catalog-
// or block-aware code must live above pkg/stats to avoid an import cycle.
package storagewiring

import (
	"context"
	"fmt"
	"net/url"

	"github.com/invergent-ai/surogate-hub/pkg/block"
	"github.com/invergent-ai/surogate-hub/pkg/catalog"
	"github.com/invergent-ai/surogate-hub/pkg/stats"
	xetstore "github.com/invergent-ai/surogate-hub/pkg/xet/store"
)

// CatalogRepoLister implements stats.RepoLister by enumerating repositories from the catalog.
// Repository ids that are not in `{owner}/{name}` form are silently skipped — they fall outside
// the per-user accounting scheme.
type CatalogRepoLister struct {
	Catalog *catalog.Catalog
}

// ListRepos calls fn once for each repository owned by a parseable owner. It paginates internally
// using catalog.ListRepositories.
func (l *CatalogRepoLister) ListRepos(ctx context.Context, fn func(stats.ReconcilerRepo) error) error {
	const pageSize = 100
	var after string
	for {
		repos, hasMore, err := l.Catalog.ListRepositories(ctx, pageSize, "", "", after)
		if err != nil {
			return fmt.Errorf("list repositories: %w", err)
		}
		if len(repos) == 0 {
			return nil
		}
		for _, repo := range repos {
			after = repo.Name
			owner, name, splitErr := stats.SplitNamespacedRepo(repo.Name)
			if splitErr != nil {
				continue
			}
			if err := fn(stats.ReconcilerRepo{
				Owner:            owner,
				Name:             name,
				StorageID:        repo.StorageID,
				StorageNamespace: repo.StorageNamespace,
			}); err != nil {
				return err
			}
		}
		if !hasMore {
			return nil
		}
	}
}

// RepoSizer implements stats.RepoSizer by walking the block adapter's storage namespace and
// summing entry sizes, then adding the xet files the repo holds, which are stored outside every
// repo namespace. For backends that support cheap whole-namespace sizing (e.g. S3 inventory),
// wire a more efficient implementation behind the same interface.
type RepoSizer struct {
	Adapter block.Adapter
	// XET is nil when xet files are not tracked.
	XET *xetstore.Registry
}

// RepoSize walks every object under the repo's storage namespace and returns the sum of stored
// bytes plus the size of the repo's xet files. Walking happens via the adapter's GetWalker, which
// is the same path used by GC.
func (s *RepoSizer) RepoSize(ctx context.Context, repo stats.ReconcilerRepo) (int64, error) {
	total, err := s.namespaceSize(ctx, repo.StorageID, repo.StorageNamespace)
	if err != nil {
		return 0, err
	}
	if s.XET == nil {
		return total, nil
	}
	xetBytes, err := s.XET.RepoXetBytes(ctx, repo.Owner+"/"+repo.Name)
	if err != nil {
		return 0, fmt.Errorf("xet bytes of %s/%s: %w", repo.Owner, repo.Name, err)
	}
	return total + xetBytes, nil
}

func (s *RepoSizer) namespaceSize(ctx context.Context, storageID, storageNamespace string) (int64, error) {
	storageURI, err := url.Parse(storageNamespace)
	if err != nil {
		return 0, fmt.Errorf("parse storage namespace %s: %w", storageNamespace, err)
	}
	walker, err := s.Adapter.GetWalker(storageID, block.WalkerOptions{StorageURI: storageURI})
	if err != nil {
		return 0, fmt.Errorf("get walker for %s: %w", storageNamespace, err)
	}
	var total int64
	walkErr := walker.Walk(ctx, storageURI, block.WalkOptions{}, func(e block.ObjectStoreEntry) error {
		total += e.Size
		return nil
	})
	if walkErr != nil {
		return 0, fmt.Errorf("walk %s: %w", storageNamespace, walkErr)
	}
	return total, nil
}
