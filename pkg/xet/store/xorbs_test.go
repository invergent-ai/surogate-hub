package store

import (
	"testing"

	"github.com/invergent-ai/surogate-hub/pkg/config"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

func TestStorageNamespace(t *testing.T) {
	t.Cleanup(viper.Reset)
	for _, tc := range []struct {
		blockstore string
		prefix     string
		want       string
	}{
		{blockstore: "mem", want: "mem://_hub_xet"},
		{blockstore: "s3"},
		{blockstore: "s3", prefix: "s3://bucket/hub/", want: "s3://bucket/hub/_hub_xet"},
	} {
		viper.Set(config.BlockstoreTypeKey, tc.blockstore)
		viper.Set("blockstore.default_namespace_prefix", tc.prefix)
		cfg, err := config.NewConfig("", &config.BaseConfig{})
		require.NoError(t, err)

		require.Equal(t, tc.want, StorageNamespace(cfg), "%s %q", tc.blockstore, tc.prefix)
	}
}

func TestXorbWalkURI(t *testing.T) {
	t.Cleanup(viper.Reset)
	root := t.TempDir()
	viper.Set(config.BlockstoreTypeKey, "local")
	viper.Set("blockstore.local.path", root)
	viper.Set("blockstore.default_namespace_prefix", "")
	cfg, err := config.NewConfig("", &config.BaseConfig{})
	require.NoError(t, err)

	uri, err := XorbWalkURI(cfg)

	require.NoError(t, err)
	require.Equal(t, "local://"+root+"/_hub_xet", uri.String())

	viper.Set(config.BlockstoreTypeKey, "s3")
	viper.Set("blockstore.default_namespace_prefix", "s3://bucket/hub")
	cfg, err = config.NewConfig("", &config.BaseConfig{})
	require.NoError(t, err)
	uri, err = XorbWalkURI(cfg)
	require.NoError(t, err)
	require.Equal(t, "s3://bucket/hub/_hub_xet", uri.String())
}
