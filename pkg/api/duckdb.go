package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strings"

	"github.com/invergent-ai/surogate-hub/pkg/block"
	"github.com/invergent-ai/surogate-hub/pkg/block/local"
	"github.com/invergent-ai/surogate-hub/pkg/catalog"
	"github.com/invergent-ai/surogate-hub/pkg/permissions"
	"github.com/invergent-ai/surogate-hub/pkg/uri"
)

var hubRegex = regexp.MustCompile(`'sg://[^']+'`)

func transformSqlQuery(query string, ctx context.Context, c *Controller, w http.ResponseWriter, r *http.Request) (string, error) {
	hubUri := hubRegex.FindString(query)
	if hubUri == "" {
		return query, nil
	}

	if strings.Index(hubUri, "'") == 0 {
		hubUri = hubUri[1:]
	}
	if strings.LastIndex(hubUri, "'") == len(hubUri)-1 {
		hubUri = hubUri[0 : len(hubUri)-1]
	}
	parsed, err := uri.Parse(hubUri)
	if err != nil {
		writeError(w, r, http.StatusBadRequest, "invalid Surogate Hub URI")
		return "", err
	}
	if parsed.Path == nil {
		writeError(w, r, http.StatusBadRequest, "invalid Surogate Hub URI, missing path")
		return "", err
	}
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.ReadObjectAction,
			Resource: permissions.ObjectArn(parsed.Repository, *parsed.Path),
		},
	}) {
		return "", errors.New("permission denied")
	}

	repo, err := c.Catalog.GetRepository(ctx, parsed.Repository)
	if c.handleAPIError(ctx, w, r, err) {
		return "", err
	}

	entry, err := c.Catalog.GetEntry(ctx, parsed.Repository, parsed.Ref, *parsed.Path, catalog.GetEntryParams{})
	if c.handleAPIError(ctx, w, r, err) {
		return "", err
	}

	qk, err := c.BlockAdapter.ResolveNamespace(repo.StorageID, repo.StorageNamespace, entry.PhysicalAddress, entry.AddressType.ToIdentifierType())
	if c.handleAPIError(ctx, w, r, err) {
		return "", err
	}

	if qk.GetStorageType() == block.StorageTypeLocal {
		localKey := qk.(local.QualifiedKey)
		query = strings.Replace(query, hubUri, fmt.Sprintf("file:%s/%s/%s", localKey.GetPath(), parsed.Repository, localKey.GetKey()), 1)
	} else if qk.GetStorageType() == block.StorageTypeS3 {
		//s3Key := qk.(block.QualifiedKey)
		writeError(w, r, http.StatusBadRequest, "S3 namespace not supported for SQL queries")
	} else {
		writeError(w, r, http.StatusBadRequest, "storage namespace not supported for SQL queries")
	}

	return query, nil
}
