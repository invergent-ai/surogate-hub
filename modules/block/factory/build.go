package factory

import (
	"context"

	"github.com/invergent-ai/surogate-hub/pkg/block"
	"github.com/invergent-ai/surogate-hub/pkg/block/factory"
	"github.com/invergent-ai/surogate-hub/pkg/config"
	"github.com/invergent-ai/surogate-hub/pkg/stats"
)

func BuildBlockAdapter(ctx context.Context, statsCollector stats.Collector, c config.Config) (block.Adapter, error) {
	adapter, err := factory.BuildBlockAdapter(ctx, statsCollector, c.StorageConfig().GetStorageByID(config.SingleBlockstoreID))
	if err != nil {
		return nil, err
	}

	return block.NewMetricsAdapter(adapter), nil
}
