package esti

import (
	"bytes"
	"context"
	"net/http"
	"testing"
	"text/template"
	"time"

	"github.com/google/uuid"

	"github.com/invergent-ai/surogate-hub/pkg/api/apigen"
	"github.com/stretchr/testify/require"
)

var actionPreCommitTmpl = template.Must(template.New("action-pre-commit").Parse(
	`
name: Test Commit
description: set of checks to verify that branch is good
on:
  pre-commit:
    branches:
      - feature-*
hooks:
  - id: test_webhook
    type: webhook
    properties:
      url: "{{.URL}}/{{.Path}}"
      timeout : {{.Timeout}}
`))

var actionPreCreateBranchTmpl = template.Must(template.New("action-pre-create-branch").Parse(
	`
name: Test Create Branch
description: set of checks to verify that branch is good
on:
  pre-create-branch:
    branches:
hooks:
  - id: test_webhook
    type: webhook
    properties:
      url: "{{.URL}}/{{.Path}}"
      timeout : {{.Timeout}}
`))

const hooksTimeout = 2 * time.Second

func TestHooksTimeout(t *testing.T) {
	hookFailToCommit(t, "timeout")
}

func TestHooksFail(t *testing.T) {
	t.Run("commit", func(t *testing.T) {
		hookFailToCommit(t, "fail")
	})
	t.Run("create_branch", func(t *testing.T) {
		hookFailToCreateBranch(t, "fail")
	})
}

func createAction(t *testing.T, ctx context.Context, repo, branch, path string, tmp *template.Template) {
	t.Helper()

	// render actions based on templates
	docData := struct {
		URL     string
		Path    string
		Timeout string
	}{
		URL:     server.BaseURL(),
		Path:    path,
		Timeout: hooksTimeout.String(),
	}

	var doc bytes.Buffer
	doc.Reset()
	err := tmp.Execute(&doc, docData)
	require.NoError(t, err)
	content := doc.String()
	uploadResp, err := uploadContent(ctx, repo, branch, "_hub_actions/"+uuid.NewString(), content)
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, uploadResp.StatusCode())
	logger.WithField("branch", branch).Info("Commit initial content")
}

func hookFailToCreateBranch(t *testing.T, path string) {
	ctx, logger, repo := setupTest(t)
	defer tearDownTest(repo)
	const branch = "feature-1"

	logger.WithField("branch", branch).Info("Create branch")
	resp, err := client.CreateBranchWithResponse(ctx, apigen.RepositoryOwner(repo), apigen.RepositoryName(repo), apigen.CreateBranchJSONRequestBody{
		Name:   branch,
		Source: mainBranch,
	})
	require.NoError(t, err, "failed to create branch")
	require.Equal(t, http.StatusCreated, resp.StatusCode())

	createAction(t, ctx, repo, branch, path, actionPreCreateBranchTmpl)

	logger.WithField("branch", "test_branch").Info("Create branch - expect failure")
	resp, err = client.CreateBranchWithResponse(ctx, apigen.RepositoryOwner(repo), apigen.RepositoryName(repo), apigen.CreateBranchJSONRequestBody{
		Name:   "test_branch",
		Source: branch,
	})
	require.NoError(t, err)
	require.Equal(t, http.StatusPreconditionFailed, resp.StatusCode())
}

func hookFailToCommit(t *testing.T, path string) {
	ctx, logger, repo := setupTest(t)
	defer tearDownTest(repo)
	const branch = "feature-1"

	logger.WithField("branch", branch).Info("Create branch")
	resp, err := client.CreateBranchWithResponse(ctx, apigen.RepositoryOwner(repo), apigen.RepositoryName(repo), apigen.CreateBranchJSONRequestBody{
		Name:   branch,
		Source: mainBranch,
	})
	require.NoError(t, err, "failed to create branch")
	require.Equal(t, http.StatusCreated, resp.StatusCode())
	ref := string(resp.Body)
	logger.WithField("branchRef", ref).Info("Branch created")
	logger.WithField("branch", branch).Info("Upload initial content")

	createAction(t, ctx, repo, branch, path, actionPreCommitTmpl)

	commitResp, err := client.CommitWithResponse(ctx, apigen.RepositoryOwner(repo), apigen.RepositoryName(repo), branch, &apigen.CommitParams{}, apigen.CommitJSONRequestBody{
		Message: "Initial content",
	})
	require.NoError(t, err)
	require.Equal(t, http.StatusPreconditionFailed, commitResp.StatusCode())
	require.Nil(t, commitResp.JSON201)
}
