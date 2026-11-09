package cmd_test

import (
	"context"
	"testing"
	"time"

	"github.com/invergent-ai/surogate-hub/cmd/sghub/cmd"
	"github.com/invergent-ai/surogate-hub/pkg/auth"
	"github.com/invergent-ai/surogate-hub/pkg/config"
	"github.com/invergent-ai/surogate-hub/pkg/kv/kvtest"
	"github.com/invergent-ai/surogate-hub/pkg/logging"
	"github.com/spf13/viper"
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

func TestXETGCCommandHasDryRunFlag(t *testing.T) {
	command, _, err := cmd.GetRoot().Find([]string{"gc", "xet"})
	if err != nil {
		t.Fatal(err)
	}
	flag := command.Flags().Lookup("dry-run")
	if command.Use != "xet" || flag == nil || flag.DefValue != "false" {
		t.Fatalf("expected gc xet with a --dry-run flag defaulting to false, got %#v", command)
	}
}

func TestXETGCConfigDefaults(t *testing.T) {
	t.Cleanup(viper.Reset)
	viper.Set(config.BlockstoreTypeKey, "mem")
	cfg, err := config.NewConfig("", &config.BaseConfig{})
	if err != nil {
		t.Fatal(err)
	}
	gc := cfg.GetBaseConfig().XET.GC
	if gc.MinAge != 24*time.Hour || gc.XorbMinAge != 168*time.Hour || gc.ScanBatchSize != 1000 {
		t.Fatalf("unexpected xet gc defaults: %+v", gc)
	}
}
