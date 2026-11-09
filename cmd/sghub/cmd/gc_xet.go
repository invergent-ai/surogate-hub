package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	blockfactory "github.com/invergent-ai/surogate-hub/modules/block/factory"
	"github.com/invergent-ai/surogate-hub/pkg/block"
	"github.com/invergent-ai/surogate-hub/pkg/catalog"
	"github.com/invergent-ai/surogate-hub/pkg/config"
	"github.com/invergent-ai/surogate-hub/pkg/graveler"
	"github.com/invergent-ai/surogate-hub/pkg/kv"
	"github.com/invergent-ai/surogate-hub/pkg/kv/kvparams"
	"github.com/invergent-ai/surogate-hub/pkg/stats"
	"github.com/invergent-ai/surogate-hub/pkg/upload"
	xetcas "github.com/invergent-ai/surogate-hub/pkg/xet/cas"
	xetgc "github.com/invergent-ai/surogate-hub/pkg/xet/gc"
	xetstore "github.com/invergent-ai/surogate-hub/pkg/xet/store"
	"github.com/spf13/cobra"
)

var gcXETCmd = &cobra.Command{
	Use:   "xet",
	Short: "Delete xet data no repository holds",
	Long: `Delete xet file records and xorbs that no existing repository holds.
A run parks what is unneeded and deletes what stayed parked past its grace
period (xet.gc.min_age for file records, xet.gc.xorb_min_age for xorbs), so
deleting anything takes at least two runs. Uploads, links and reads bring
parked data back.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		dryRun, err := cmd.Flags().GetBool("dry-run")
		if err != nil {
			return err
		}
		report, err := runXETGC(cmd.Context(), loadConfig(), dryRun)
		if err != nil {
			return err
		}
		encoder := json.NewEncoder(cmd.OutOrStdout())
		encoder.SetIndent("", "  ")
		if err := encoder.Encode(report); err != nil {
			return err
		}
		if len(report.Errors) > 0 {
			return fmt.Errorf("xet gc finished with %d errors", len(report.Errors))
		}
		return nil
	},
}

func runXETGC(ctx context.Context, cfg config.Config, dryRun bool) (xetgc.Report, error) {
	baseCfg := cfg.GetBaseConfig()
	storageNamespace := xetstore.StorageNamespace(cfg)
	if storageNamespace == "" {
		return xetgc.Report{}, fmt.Errorf("xet storage is not configured: set blockstore.default_namespace_prefix")
	}
	kvParams, err := kvparams.NewConfig(&baseCfg.Database)
	if err != nil {
		return xetgc.Report{}, fmt.Errorf("KV params: %w", err)
	}
	kvStore, err := kv.Open(ctx, kvParams)
	if err != nil {
		return xetgc.Report{}, fmt.Errorf("open KV store: %w", err)
	}
	defer kvStore.Close()

	blockAdapter, err := blockfactory.BuildBlockAdapter(ctx, &stats.NullCollector{}, cfg)
	if err != nil {
		return xetgc.Report{}, fmt.Errorf("build block adapter: %w", err)
	}
	cat, err := catalog.New(ctx, catalog.Config{
		Config:       cfg,
		KVStore:      kvStore,
		PathProvider: upload.DefaultPathProvider,
	})
	if err != nil {
		return xetgc.Report{}, fmt.Errorf("build catalog: %w", err)
	}
	defer func() { _ = cat.Close() }()

	xorbs := xetcas.NewXorbStore(blockAdapter, storageNamespace)
	return xetgc.Sweep(ctx, xetgc.Params{
		Registry:      xetstore.NewRegistry(kvStore),
		ScanBatchSize: baseCfg.XET.GC.ScanBatchSize,
		FileMinAge:    baseCfg.XET.GC.MinAge,
		XorbMinAge:    baseCfg.XET.GC.XorbMinAge,
		DryRun:        dryRun,
		RepoExists: func(ctx context.Context, repo string) (bool, error) {
			_, err := cat.GetRepository(ctx, repo)
			if errors.Is(err, graveler.ErrNotFound) {
				return false, nil
			}
			return err == nil, err
		},
		ListXorbs: func(ctx context.Context) ([]xetgc.XorbRef, error) {
			storageURI, err := xetstore.XorbWalkURI(cfg)
			if err != nil {
				return nil, err
			}
			walker, err := blockAdapter.GetWalker(config.SingleBlockstoreID, block.WalkerOptions{StorageURI: storageURI})
			if err != nil {
				return nil, fmt.Errorf("list xorbs in %s: %w", storageNamespace, err)
			}
			return xetgc.ListXorbsFromWalker(ctx, walker, storageURI)
		},
		RemoveXorb: func(ctx context.Context, hash string) error {
			return blockAdapter.Remove(ctx, xetstore.XorbObjectPointer(storageNamespace, "default", hash))
		},
		StoredFooter: xorbs.StoredFooter,
	})
}

//nolint:gochecknoinits
func init() {
	gcXETCmd.Flags().Bool("dry-run", false, "report what a run would do without changing anything")
	gcCmd.AddCommand(gcXETCmd)
}
