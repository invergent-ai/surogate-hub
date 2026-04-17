package cmd_test

import (
	"context"
	"testing"

	"github.com/treeverse/lakefs/cmd/lakefs/cmd"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/kv/kvtest"
	"github.com/treeverse/lakefs/pkg/logging"
)

func TestNewAuthService_ReturnsACLService(t *testing.T) {
	cfg := &config.BaseConfig{}
	kvStore := kvtest.GetStore(context.Background(), t)
	meta := auth.NewKVMetadataManager("serve_test", cfg.Installation.FixedID, cfg.Database.Type, kvStore)
	service := cmd.NewAuthService(context.Background(), cfg, logging.ContextUnavailable(), kvStore, meta)
	if service == nil {
		t.Fatal("expected auth service to be created")
	}
}
