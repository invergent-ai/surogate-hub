package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"

	blockfactory "github.com/invergent-ai/surogate-hub/modules/block/factory"
	"github.com/invergent-ai/surogate-hub/pkg/block"
	"github.com/invergent-ai/surogate-hub/pkg/catalog"
	"github.com/invergent-ai/surogate-hub/pkg/config"
	"github.com/invergent-ai/surogate-hub/pkg/graveler"
	"github.com/invergent-ai/surogate-hub/pkg/kv"
	"github.com/invergent-ai/surogate-hub/pkg/kv/kvparams"
	"github.com/invergent-ai/surogate-hub/pkg/stats"
	"github.com/invergent-ai/surogate-hub/pkg/upload"
	xetgc "github.com/invergent-ai/surogate-hub/pkg/xet/gc"
	xetstore "github.com/invergent-ai/surogate-hub/pkg/xet/store"
	"github.com/spf13/cobra"
)

var gcXETCmd = &cobra.Command{
	Use:   "xet",
	Short: "Run XET garbage collection",
	RunE: func(cmd *cobra.Command, args []string) error {
		dryRun, err := cmd.Flags().GetBool("dry-run")
		if err != nil {
			return err
		}
		if !dryRun {
			return fmt.Errorf("xet gc currently requires --dry-run")
		}
		report, err := runXETGCDryRun(cmd.Context(), loadConfig())
		if err != nil {
			return err
		}
		encoder := json.NewEncoder(cmd.OutOrStdout())
		encoder.SetIndent("", "  ")
		return encoder.Encode(report)
	},
}

func runXETGCDryRun(ctx context.Context, cfg config.Config) (xetgc.Report, error) {
	baseCfg := cfg.GetBaseConfig()
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

	registry := xetstore.NewRegistry(kvStore)
	storageNamespace := xetGCStorageNamespace(cfg, blockAdapter)
	return xetgc.DryRun(ctx, xetgc.Params{
		Registry:      registry,
		ScanBatchSize: baseCfg.XET.Read.CapabilityScanBatchSize,
		MinAge:        baseCfg.XET.GC.MinAge,
		FileRefLive: func(ctx context.Context, ref xetstore.FileRef) (bool, error) {
			entry, err := cat.GetEntry(ctx, ref.Repo, ref.Ref, ref.Path, catalog.GetEntryParams{})
			if errors.Is(err, graveler.ErrNotFound) {
				return false, nil
			}
			if err != nil {
				return false, err
			}
			return entry.PhysicalAddress == "xet://"+ref.FileHash, nil
		},
		ListXorbs: func(ctx context.Context) ([]xetgc.XorbRef, error) {
			storageURI, err := url.Parse(strings.TrimRight(storageNamespace, "/"))
			if err != nil {
				return nil, err
			}
			walker, err := blockAdapter.GetWalker(config.SingleBlockstoreID, block.WalkerOptions{StorageURI: storageURI})
			if errors.Is(err, block.ErrOperationNotSupported) {
				return nil, nil
			}
			if err != nil {
				return nil, err
			}
			return xetgc.ListXorbsFromWalker(ctx, walker, storageURI)
		},
	})
}

func xetGCStorageNamespace(cfg config.Config, blockAdapter block.Adapter) string {
	if storage := cfg.StorageConfig().GetStorageByID(config.SingleBlockstoreID); storage != nil {
		if prefix := storage.GetDefaultNamespacePrefix(); prefix != nil && *prefix != "" {
			return strings.TrimRight(*prefix, "/") + "/_hub_xet"
		}
	}
	return blockAdapter.BlockstoreType() + "://_hub_xet"
}

//nolint:gochecknoinits
func init() {
	gcXETCmd.Flags().Bool("dry-run", false, "report XET garbage without deleting it")
	gcCmd.AddCommand(gcXETCmd)
}
