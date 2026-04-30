package testutil

import (
	"context"
	"testing"

	"github.com/invergent-ai/surogate-hub/pkg/auth/acl"
	"github.com/invergent-ai/surogate-hub/pkg/auth/crypt"
	authparams "github.com/invergent-ai/surogate-hub/pkg/auth/params"
	"github.com/invergent-ai/surogate-hub/pkg/kv"
	"github.com/invergent-ai/surogate-hub/pkg/kv/kvtest"
)

func SetupService(t *testing.T, ctx context.Context, secret []byte) (*acl.AuthService, kv.Store) {
	t.Helper()
	kvStore := kvtest.GetStore(ctx, t)
	return acl.NewAuthService(kvStore, crypt.NewSecretStore(secret), authparams.ServiceCache{
		Enabled: false,
	}), kvStore
}
