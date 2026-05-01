package local_test

import (
	"path"
	"regexp"
	"testing"

	"github.com/invergent-ai/surogate-hub/pkg/block"
	"github.com/invergent-ai/surogate-hub/pkg/block/blocktest"
	"github.com/invergent-ai/surogate-hub/pkg/block/local"
	"github.com/invergent-ai/surogate-hub/pkg/config"
	"github.com/stretchr/testify/require"
)

const testStorageNamespace = "local://test"

// TestLocalAdapter tests the Local Storage Adapter for basic storage functionality
func TestLocalAdapter(t *testing.T) {
	tmpDir := t.TempDir()
	localPath := path.Join(tmpDir, "sghub")
	externalPath := block.BlockstoreTypeLocal + "://" + path.Join(tmpDir, "sghub", "external")
	adapter, err := local.NewAdapter(localPath, local.WithRemoveEmptyDir(false))
	if err != nil {
		t.Fatal("Failed to create new adapter", err)
	}
	blocktest.AdapterTest(t, adapter, testStorageNamespace, externalPath, false)
}

// TestAdapterNamespace tests the namespace validity regex with various paths
func TestAdapterNamespace(t *testing.T) {
	tmpDir := t.TempDir()
	localPath := path.Join(tmpDir, "sghub")
	adapter, err := local.NewAdapter(localPath, local.WithRemoveEmptyDir(false))
	require.NoError(t, err, "create new adapter")
	expr, err := regexp.Compile(adapter.GetStorageNamespaceInfo(config.SingleBlockstoreID).ValidityRegex)
	require.NoError(t, err)

	tests := []struct {
		Name      string
		Namespace string
		Success   bool
	}{
		{
			Name:      "valid_path",
			Namespace: "local://test/path/to/repo1",
			Success:   true,
		},
		{
			Name:      "invalid_path",
			Namespace: "~/test/path/to/repo1",
			Success:   false,
		},
		{
			Name:      "s3",
			Namespace: "s3://test/adls/core/windows/net",
			Success:   false,
		},
		{
			Name:      "invalid_string",
			Namespace: "this is a bad string",
			Success:   false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			require.Equal(t, tt.Success, expr.MatchString(tt.Namespace))
		})
	}
}
