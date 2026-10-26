package operations

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/invergent-ai/surogate-hub/pkg/block"
	"github.com/invergent-ai/surogate-hub/pkg/catalog"
	"github.com/invergent-ai/surogate-hub/pkg/config"
	"github.com/invergent-ai/surogate-hub/pkg/kv/kvtest"
	"github.com/invergent-ai/surogate-hub/pkg/upload"
	xetstore "github.com/invergent-ai/surogate-hub/pkg/xet/store"
	"github.com/invergent-ai/surogate-hub/pkg/xet/xettest"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

func TestGetObjectXETPhysicalAddressRange(t *testing.T) {
	ctx := context.Background()
	viper.Set(config.BlockstoreTypeKey, block.BlockstoreTypeMem)
	cfg := &config.BaseConfig{}
	cfg, err := config.NewConfig("", cfg)
	require.NoError(t, err)
	c, err := catalog.New(ctx, catalog.Config{
		Config:       cfg,
		KVStore:      kvtest.GetStore(ctx, t),
		PathProvider: upload.DefaultPathProvider,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	repo := "test-user/repo-a"
	const branch = "main"
	const path = "models/checkpoint.bin"
	_, err = c.CreateRepository(ctx, repo, "", "mem://test-user/repo-a", branch, false)
	require.NoError(t, err)
	chunk := []byte("hello world!")
	fileHash, _, _ := xettest.SeedFile(t, ctx, xetstore.NewRegistry(c.KVStore), c.BlockAdapter, "mem://_hub_xet", chunk)
	err = c.CreateEntry(ctx, repo, branch, catalog.DBEntry{
		Path:            path,
		PhysicalAddress: "xet://" + fileHash,
		AddressType:     catalog.AddressTypeFull,
		CreationDate:    time.Now(),
		Size:            int64(len(chunk)),
		Checksum:        "checksum-a",
	})
	require.NoError(t, err)
	_, err = c.GetEntry(ctx, repo, branch, path, catalog.GetEntryParams{})
	require.NoError(t, err)
	repository, err := c.GetRepository(ctx, repo)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodGet, "/"+repo+"/"+path, nil)
	req.Header.Set("Range", "bytes=3-8")
	rec := httptest.NewRecorder()
	(&GetObject{}).Handle(rec, req, &PathOperation{
		RefOperation: &RefOperation{
			RepoOperation: &RepoOperation{
				AuthorizedOperation: &AuthorizedOperation{
					Operation: &Operation{
						XETNamespace: "mem://_hub_xet",
						Region:       "us-east-1",
						Catalog:      c,
						BlockStore:   c.BlockAdapter,
						Incr:         func(action, userID, repository, ref string) {},
					},
					Principal: "user-a",
				},
				Repository: repository,
			},
			Reference: branch,
		},
		Path: path,
	})

	require.Equal(t, http.StatusPartialContent, rec.Code)
	require.Equal(t, []byte("lo wor"), rec.Body.Bytes())
}
