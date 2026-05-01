package permissions_test

import (
	"testing"

	"github.com/invergent-ai/surogate-hub/pkg/permissions"
	"golang.org/x/exp/slices"
)

func TestAllActions(t *testing.T) {
	actions := permissions.Actions

	if !slices.Contains(actions, permissions.ReadUserAction) {
		t.Errorf("Expected actions %v to include %s", actions, permissions.ReadUserAction)
	}

	if !slices.Contains(actions, permissions.ReadActionsAction) {
		t.Errorf("Expected actions %v to include %s", actions, permissions.ReadActionsAction)
	}

	if slices.Contains(actions, "IsValidAction") {
		t.Errorf("Expected actions %v not to include IsValidAction", actions)
	}
}

func TestNamespacedRepositoryResourceARNs(t *testing.T) {
	tests := map[string]string{
		"repo":   permissions.RepoArn("alice/model"),
		"object": permissions.ObjectArn("alice/model", "data/file.txt"),
		"branch": permissions.BranchArn("alice/model", "main"),
		"tag":    permissions.TagArn("alice/model", "v1"),
	}

	expected := map[string]string{
		"repo":   "arn:sghub:fs:::repository/alice/model",
		"object": "arn:sghub:fs:::repository/alice/model/object/data/file.txt",
		"branch": "arn:sghub:fs:::repository/alice/model/branch/main",
		"tag":    "arn:sghub:fs:::repository/alice/model/tag/v1",
	}

	for name, got := range tests {
		if got != expected[name] {
			t.Fatalf("%s ARN = %q, expected %q", name, got, expected[name])
		}
	}
}
