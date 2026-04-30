package lua

import (
	"context"

	"github.com/Shopify/go-lua"

	"github.com/invergent-ai/surogate-hub/pkg/actions/lua/crypto/aes"
	"github.com/invergent-ai/surogate-hub/pkg/actions/lua/crypto/hmac"
	"github.com/invergent-ai/surogate-hub/pkg/actions/lua/crypto/sha256"
	"github.com/invergent-ai/surogate-hub/pkg/actions/lua/databricks"
	"github.com/invergent-ai/surogate-hub/pkg/actions/lua/encoding/base64"
	"github.com/invergent-ai/surogate-hub/pkg/actions/lua/encoding/hex"
	"github.com/invergent-ai/surogate-hub/pkg/actions/lua/encoding/json"
	"github.com/invergent-ai/surogate-hub/pkg/actions/lua/encoding/parquet"
	"github.com/invergent-ai/surogate-hub/pkg/actions/lua/encoding/yaml"
	"github.com/invergent-ai/surogate-hub/pkg/actions/lua/formats"
	"github.com/invergent-ai/surogate-hub/pkg/actions/lua/hook"
	"github.com/invergent-ai/surogate-hub/pkg/actions/lua/net/http"
	"github.com/invergent-ai/surogate-hub/pkg/actions/lua/net/url"
	"github.com/invergent-ai/surogate-hub/pkg/actions/lua/path"
	"github.com/invergent-ai/surogate-hub/pkg/actions/lua/regexp"
	"github.com/invergent-ai/surogate-hub/pkg/actions/lua/storage/aws"
	"github.com/invergent-ai/surogate-hub/pkg/actions/lua/storage/azure"
	"github.com/invergent-ai/surogate-hub/pkg/actions/lua/storage/gcloud"
	"github.com/invergent-ai/surogate-hub/pkg/actions/lua/strings"
	"github.com/invergent-ai/surogate-hub/pkg/actions/lua/time"
	"github.com/invergent-ai/surogate-hub/pkg/actions/lua/util"
	"github.com/invergent-ai/surogate-hub/pkg/actions/lua/uuid"
)

// most classes here are taken from: https://github.com/Shopify/goluago
// See the original MIT license with copyright at ./LICENSE.md

func Open(l *lua.State, ctx context.Context, cfg OpenSafeConfig) {
	regexp.Open(l)
	strings.Open(l)
	util.Open(l)
	json.Open(l)
	yaml.Open(l)
	time.Open(l)
	hmac.Open(l)
	base64.Open(l)
	uuid.Open(l)
	hex.Open(l)
	sha256.Open(l)
	aes.Open(l)
	parquet.Open(l)
	path.Open(l)
	hook.Open(l)
	aws.Open(l, ctx)
	gcloud.Open(l, ctx)
	azure.Open(l, ctx)
	url.Open(l)
	formats.Open(l, ctx, cfg.LakeFSAddr)
	databricks.Open(l, ctx)
	if cfg.NetHTTPEnabled {
		http.Open(l)
	}
}
