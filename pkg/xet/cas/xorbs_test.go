package cas

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/invergent-ai/surogate-hub/pkg/block/mem"
	"github.com/stretchr/testify/require"
)

func TestXorbStorePutIsIdempotentAndStoresBytes(t *testing.T) {
	ctx := context.Background()
	store := NewXorbStore(mem.New(ctx), "mem://xet-cas")

	exists, err := store.Exists(ctx, "default", "xorb-a")
	require.NoError(t, err)
	require.False(t, exists)

	first, err := store.Put(ctx, "default", "xorb-a", int64(len("xorb-bytes")), strings.NewReader("xorb-bytes"))
	require.NoError(t, err)
	require.True(t, first.WasInserted)

	exists, err = store.Exists(ctx, "default", "xorb-a")
	require.NoError(t, err)
	require.True(t, exists)

	second, err := store.Put(ctx, "default", "xorb-a", int64(len("different")), strings.NewReader("different"))
	require.NoError(t, err)
	require.False(t, second.WasInserted)

	reader, err := store.Get(ctx, "default", "xorb-a")
	require.NoError(t, err)
	defer reader.Close()
	body, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, []byte("xorb-bytes"), body)
}
