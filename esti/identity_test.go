package esti

import (
	"testing"

	"github.com/invergent-ai/surogate-hub/pkg/api/apigen"
	"github.com/stretchr/testify/require"
)

func TestIdentity(t *testing.T) {
	ctx, _, repo := setupTest(t)
	defer tearDownTest(repo)
	objPath := "1.txt"
	branch1 := "feature-1"
	branch2 := "feature-2"

	_, err := client.CreateBranchWithResponse(ctx, apigen.RepositoryOwner(repo), apigen.RepositoryName(repo), apigen.CreateBranchJSONRequestBody{
		Name:   branch1,
		Source: mainBranch,
	})
	require.NoError(t, err, "failed creating branch1")

	checksum, objContent, err := uploadFileRandomDataAndReport(ctx, repo, branch1, objPath, false)
	require.NoError(t, err, "failed uploading file")
	commitResp, err := client.CommitWithResponse(ctx, apigen.RepositoryOwner(repo), apigen.RepositoryName(repo), branch1, &apigen.CommitParams{}, apigen.CommitJSONRequestBody{
		Message: "commit on branch1",
	})
	require.NoError(t, err, "failed to commit changes")
	require.NoErrorf(t, verifyResponse(commitResp.HTTPResponse, commitResp.Body),
		"failed to commit changes repo %s branch %s", repo, mainBranch)

	// upload the same content again to a different branch
	_, err = client.CreateBranchWithResponse(ctx, apigen.RepositoryOwner(repo), apigen.RepositoryName(repo), apigen.CreateBranchJSONRequestBody{
		Name:   branch2,
		Source: mainBranch,
	})
	require.NoError(t, err, "failed creating branch2")

	checksumNew, err := uploadFileAndReport(ctx, repo, branch2, objPath, objContent, false)
	require.NoError(t, err)
	require.Equal(t, checksum, checksumNew, "Same file uploaded to committed branch, expected no checksum difference")

	_, err = client.CommitWithResponse(ctx, apigen.RepositoryOwner(repo), apigen.RepositoryName(repo), branch2, &apigen.CommitParams{}, apigen.CommitJSONRequestBody{
		Message: "commit on branch2",
	})
	require.NoError(t, err, "failed to commit changes")

	diff, err := client.DiffRefsWithResponse(ctx, apigen.RepositoryOwner(repo), apigen.RepositoryName(repo), branch1, branch2, &apigen.DiffRefsParams{})
	require.NoError(t, err, "Diff refs failed")
	require.Empty(t, diff.JSON200.Results, "Expected no diff files")

	resp, err := client.MergeIntoBranchWithResponse(ctx, apigen.RepositoryOwner(repo), apigen.RepositoryName(repo), branch1, branch2, apigen.MergeIntoBranchJSONRequestBody{})
	require.NoError(t, err, "error during merge")
	require.NotEmpty(t, resp.JSON200, "allow merge with no changes between the branches")
}
