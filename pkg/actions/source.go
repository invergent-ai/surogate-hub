package actions

import (
	"context"

	"github.com/invergent-ai/surogate-hub/pkg/graveler"
)

type Source interface {
	List(ctx context.Context, record graveler.HookRecord) ([]string, error)
	Load(ctx context.Context, record graveler.HookRecord, name string) ([]byte, error)
}
