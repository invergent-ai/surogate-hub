package cmd_test

import (
	"context"
	"testing"

	"github.com/invergent-ai/surogate-hub/cmd/sghub/cmd"
	"github.com/invergent-ai/surogate-hub/pkg/auth"
	"github.com/invergent-ai/surogate-hub/pkg/config"
	"github.com/invergent-ai/surogate-hub/pkg/kv/kvtest"
	"github.com/invergent-ai/surogate-hub/pkg/logging"
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

func TestXETGCCommandRegistered(t *testing.T) {
	command, _, err := cmd.GetRoot().Find([]string{"gc", "xet", "--dry-run"})
	if err != nil {
		t.Fatal(err)
	}
	if command == nil || command.Use != "xet" {
		t.Fatalf("expected gc xet command, got %#v", command)
	}
}
