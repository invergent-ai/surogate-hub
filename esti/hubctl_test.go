package esti

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/invergent-ai/surogate-hub/pkg/api/apigen"
	"github.com/invergent-ai/surogate-hub/pkg/graveler"
	"github.com/stretchr/testify/require"
)

var emptyVars = make(map[string]string)

const branchProtectTimeout = graveler.BranchUpdateMaxInterval + time.Second

func TestHubctlHelp(t *testing.T) {
	RunCmdAndVerifySuccessWithFile(t, Hubctl(), false, "hubctl_help", emptyVars)
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" --help", false, "hubctl_help", emptyVars)
	RunCmdAndVerifySuccessWithFile(t, Hubctl(), true, "hubctl_help", emptyVars)
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" --help", true, "hubctl_help", emptyVars)
}

func TestHubctlBasicRepoActions(t *testing.T) {
	// RunCmdAndVerifySuccess(t, Hubctl()+" repo list", false, "\n", emptyVars)

	// Fails due to the usage of repos for isolation - esti creates repos in parallel and
	// the output of 'repo list' command cannot be well-defined
	// hubctl repo list with no repo created. Verifying terminal and piped formats
	// RunCmdAndVerifySuccess(t, Hubctl()+" repo list --no-color", true, "\n", emptyVars)
	// RunCmdAndVerifySuccessWithFile(t, Hubctl()+" repo list", true, "hubctl_repo_list_empty.term", emptyVars)

	// Create repo using hubctl repo create and verifying the output
	// A variable mapping is used to pass random generated names for verification
	repoName := generateUniqueRepositoryName()
	storage := generateUniqueStorageNamespace(repoName)
	vars := map[string]string{
		"REPO":    repoName,
		"STORAGE": storage,
		"BRANCH":  mainBranch,
	}
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" repo create sg://"+repoName+" "+storage, false, "hubctl_repo_create", vars)

	// hubctl repo list is expected to show the created repo

	// Fails due to the usage of repos for isolation - esti creates repos in parallel and
	// the output of 'repo list' command cannot be well-defined
	// RunCmdAndVerifySuccessWithFile(t, Hubctl()+" repo list", false, "hubctl_repo_list_1", vars)
	// RunCmdAndVerifySuccessWithFile(t, Hubctl()+" repo list --no-color", true, "hubctl_repo_list_1", vars)
	// RunCmdAndVerifySuccessWithFile(t, Hubctl()+" repo list", true, "hubctl_repo_list_1.term", vars)

	// Create a second repo. Vars for the first repo are being saved in a new map, in order to be used
	// for a follow-up verification with 'repo list'
	// listVars := map[string]string{
	// 	"REPO1":    repoName,
	// 	"STORAGE1": storage,
	// 	"BRANCH1":  mainBranch,
	// }

	// Trying to create the same repo again fails and does not change the list
	newStorage := storage + "/new-storage/"
	RunCmdAndVerifyFailureWithFile(t, Hubctl()+" repo create sg://"+repoName+" "+newStorage, false, "hubctl_repo_create_not_unique", vars)

	// Fails due to the usage of repos for isolation - esti creates repos in parallel and
	// the output of 'repo list' command cannot be well-defined
	// RunCmdAndVerifySuccessWithFile(t, Hubctl()+" repo list", false, "hubctl_repo_list_1", vars)

	// Create another repo with non-default branch
	repoName2 := generateUniqueRepositoryName()
	storage2 := generateUniqueStorageNamespace(repoName2)
	notDefaultBranchName := "branch-123"
	vars["REPO"] = repoName2
	vars["STORAGE"] = storage2
	vars["BRANCH"] = notDefaultBranchName
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" repo create sg://"+repoName2+" "+storage2+" -d "+notDefaultBranchName, true, "hubctl_repo_create", vars)

	// The generated names are also added to the verification vars map

	// Fails due to the usage of repos for isolation - esti creates repos in parallel and
	// the output of 'repo list' command cannot be well-defined
	// listVars["REPO2"] = repoName2
	// listVars["STORAGE2"] = storage2
	// listVars["BRANCH2"] = notDefaultBranchName
	// RunCmdAndVerifySuccessWithFile(t, Hubctl()+" repo list", false, "hubctl_repo_list_2", listVars)
	// RunCmdAndVerifySuccessWithFile(t, Hubctl()+" repo list --no-color", true, "hubctl_repo_list_2", listVars)
	// RunCmdAndVerifySuccessWithFile(t, Hubctl()+" repo list", true, "hubctl_repo_list_2.term", listVars)

	// RunCmdAndVerifySuccessWithFile(t, Hubctl()+" repo list --after "+repoName, false, "hubctl_repo_list_1", vars)

	// Trying to delete a repo using malformed_uri
	RunCmdAndVerifyFailureWithFile(t, Hubctl()+" repo delete "+repoName2+" -y", false, "hubctl_repo_delete_malformed_uri", vars)

	// Trying to delete a repo using malformed_uri, using terminal
	RunCmdAndVerifyFailureWithFile(t, Hubctl()+" repo delete "+repoName2+" -y", true, "hubctl_repo_delete_malformed_uri.term", vars)

	// Deleting a repo
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" repo delete sg://"+repoName2+" -y", false, "hubctl_repo_delete", vars)

	// Trying to delete again
	RunCmdAndVerifyFailureWithFile(t, Hubctl()+" repo delete sg://"+repoName2+" -y", false, "hubctl_repo_delete_not_found", vars)

	// Create repository with sample data
	repoName3 := generateUniqueRepositoryName()
	storage3 := generateUniqueStorageNamespace(repoName3)
	vars = map[string]string{
		"REPO":    repoName3,
		"STORAGE": storage3,
		"BRANCH":  mainBranch,
	}
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" repo create sg://"+repoName3+" "+storage3+" --sample-data", false, "hubctl_repo_create_sample", vars)
}

func TestHubctlRepoCreateWithStorageID(t *testing.T) {
	// Validate the --storage-id flag (currently only allowed to be empty)
	repoName := generateUniqueRepositoryName()
	storage := generateUniqueStorageNamespace(repoName)
	vars := map[string]string{
		"REPO":    repoName,
		"STORAGE": storage,
		"BRANCH":  mainBranch,
	}
	RunCmdAndVerifyFailureWithFile(t, Hubctl()+" repo create sg://"+repoName+" "+storage+" --storage-id storage1", false, "hubctl_repo_create_with_storage_id", vars)
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" repo create sg://"+repoName+" "+storage+" --storage-id \"\"", false, "hubctl_repo_create", vars)
}

func TestHubctlPreSignUpload(t *testing.T) {
	repoName := generateUniqueRepositoryName()
	storage := generateUniqueStorageNamespace(repoName)
	vars := map[string]string{
		"REPO":    repoName,
		"STORAGE": storage,
		"BRANCH":  mainBranch,
	}
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" repo create sg://"+repoName+" "+storage, false, "hubctl_repo_create", vars)
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" log sg://"+repoName+"/"+mainBranch, false, "hubctl_log_initial", vars)

	filePath := "ro_1k.1"
	t.Run("upload from file", func(t *testing.T) {
		vars["FILE_PATH"] = filePath
		RunCmdAndVerifySuccessWithFile(t, Hubctl()+" fs upload -s files/ro_1k sg://"+repoName+"/"+mainBranch+"/"+filePath+" --pre-sign", false, "hubctl_fs_upload", vars)
	})
	t.Run("upload from stdin", func(t *testing.T) {
		RunCmdAndVerifySuccessWithFile(t, "cat files/ro_1k | "+Hubctl()+" fs upload -s - sg://"+repoName+"/"+mainBranch+"/"+filePath+" --pre-sign", false, "hubctl_fs_upload", vars)
	})
}

func TestHubctlCommit(t *testing.T) {
	repoName := generateUniqueRepositoryName()
	storage := generateUniqueStorageNamespace(repoName)
	vars := map[string]string{
		"REPO":    repoName,
		"STORAGE": storage,
		"BRANCH":  mainBranch,
	}
	RunCmdAndVerifyFailureWithFile(t, Hubctl()+" log sg://"+repoName+"/"+mainBranch, false, "hubctl_log_404", vars)
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" repo create sg://"+repoName+" "+storage, false, "hubctl_repo_create", vars)
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" log sg://"+repoName+"/"+mainBranch, false, "hubctl_log_initial", vars)

	filePath := "ro_1k.1"
	vars["FILE_PATH"] = filePath
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" fs upload -s files/ro_1k sg://"+repoName+"/"+mainBranch+"/"+filePath, false, "hubctl_fs_upload", vars)
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" log sg://"+repoName+"/"+mainBranch, false, "hubctl_log_initial", vars)
	RunCmdAndVerifyFailureWithFile(t, Hubctl()+" commit sg://"+repoName+"/"+mainBranch, false, "hubctl_commit_no_msg", vars)
	RunCmdAndVerifyFailureWithFile(t, Hubctl()+" commit sg://"+repoName+"/"+mainBranch+" -m \" \"", false, "hubctl_commit_no_msg", vars)
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" commit sg://"+repoName+"/"+mainBranch+" --allow-empty-message -m \" \"", false, "hubctl_commit_with_empty_msg_flag", vars)
	filePath = "ro_1k.2"
	vars["FILE_PATH"] = filePath
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" fs upload -s files/ro_1k sg://"+repoName+"/"+mainBranch+"/"+filePath, false, "hubctl_fs_upload", vars)
	commitMessage := "esti_hubctl:TestCommit"
	vars["MESSAGE"] = commitMessage
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" commit sg://"+repoName+"/"+mainBranch+" -m \""+commitMessage+"\"", false, "hubctl_commit", vars)
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" log sg://"+repoName+"/"+mainBranch, false, "hubctl_log_with_commit", vars)
	RunCmdAndVerifyFailureWithFile(t, Hubctl()+" commit sg://"+repoName+"/"+mainBranch+" -m \"esti_hubctl:should fail\"", false, "hubctl_commit_no_change", vars)
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" log sg://"+repoName+"/"+mainBranch, false, "hubctl_log_with_commit", vars)

	filePath = "ro_1k.3"
	vars["FILE_PATH"] = filePath
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" fs upload -s files/ro_1k sg://"+repoName+"/"+mainBranch+"/"+filePath, false, "hubctl_fs_upload", vars)
	commitMessage = "commit with a very old date"
	vars["MESSAGE"] = commitMessage
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" commit sg://"+repoName+"/"+mainBranch+` -m "`+commitMessage+`" --epoch-time-seconds 0`, false, "hubctl_commit", vars)
	vars["DATE"] = time.Unix(0, 0).String()
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" log sg://"+repoName+"/"+mainBranch+" --amount 1", false, "hubctl_log_with_commit_custom_date", vars)

	// verify the latest commit using 'show commit'
	ctx := context.Background()
	getBranchResp, err := client.GetBranchWithResponse(ctx, apigen.RepositoryOwner(repoName), apigen.RepositoryName(repoName), mainBranch)
	if err != nil {
		t.Fatal("Failed to get branch information", err)
	}
	if getBranchResp.JSON200 == nil {
		t.Fatalf("Get branch status code=%d, expected 200", getBranchResp.StatusCode())
	}
	lastCommitID := getBranchResp.JSON200.CommitId

	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" show commit sg://"+repoName+"/"+lastCommitID, false, "hubctl_show_commit", vars)
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" show commit sg://"+repoName+"/"+lastCommitID+" --show-meta-range-id", false, "hubctl_show_commit_metarange", vars)
}

func TestHubctlBranchAndTagValidation(t *testing.T) {
	repoName := generateUniqueRepositoryName()
	storage := generateUniqueStorageNamespace(repoName)
	validTagName := "my.valid.tag"

	vars := map[string]string{
		"REPO":    repoName,
		"STORAGE": storage,
		"BRANCH":  mainBranch,
		"TAG":     validTagName,
	}
	invalidBranchName := "my.invalid.branch"
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" repo create sg://"+repoName+" "+storage, false, "hubctl_repo_create", vars)
	vars["BRANCH"] = mainBranch
	vars["FILE_PATH"] = "a/b/c"
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" fs upload -s files/ro_1k sg://"+repoName+"/"+mainBranch+"/a/b/c", false, "hubctl_fs_upload", vars)
	commitMessage := "another file update on main branch"
	vars["MESSAGE"] = commitMessage
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" commit sg://"+repoName+"/"+mainBranch+" -m \""+commitMessage+"\"", false, "hubctl_commit", vars)

	RunCmdAndVerifyFailureWithFile(t, Hubctl()+" branch create sg://"+repoName+"/"+invalidBranchName+" --source sg://"+repoName+"/"+mainBranch, false, "hubctl_branch_create_invalid", vars)
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" tag create sg://"+repoName+"/"+validTagName+" sg://"+repoName+"/"+mainBranch, false, "hubctl_tag_create", vars)
	vars["TAG"] = "tag2"
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" tag create sg://"+repoName+"/"+vars["TAG"]+" sg://"+repoName+"/"+mainBranch+"~1", false, "hubctl_tag_create", vars)
	vars["TAG"] = "tag3"
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" tag create sg://"+repoName+"/"+vars["TAG"]+" sg://"+repoName+"/"+mainBranch+"^1", false, "hubctl_tag_create", vars)
	vars["TAG"] = "tag4"
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" tag create sg://"+repoName+"/"+vars["TAG"]+" sg://"+repoName+"/"+mainBranch+"~", false, "hubctl_tag_create", vars)

	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" tag show sg://"+repoName+"/"+vars["TAG"], false, "hubctl_tag_show", vars)
}

func TestHubctlMerge(t *testing.T) {
	repoName := generateUniqueRepositoryName()
	storage := generateUniqueStorageNamespace(repoName)
	vars := map[string]string{
		"REPO":    repoName,
		"STORAGE": storage,
		"BRANCH":  mainBranch,
	}
	filePath1 := "file1"

	// create repo with 'main' branch
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" repo create sg://"+repoName+" "+storage, false, "hubctl_repo_create", vars)
	// upload 'file1' and commit
	vars["FILE_PATH"] = filePath1
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" fs upload -s files/ro_1k sg://"+repoName+"/"+mainBranch+"/"+filePath1, false, "hubctl_fs_upload", vars)
	commitMessage := "first commit to main"
	vars["MESSAGE"] = commitMessage
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" commit sg://"+repoName+"/"+mainBranch+" -m \""+commitMessage+"\"", false, "hubctl_commit", vars)

	// create new feature branch
	featureBranch := "feature"
	featureBranchVars := map[string]string{
		"REPO":          repoName,
		"STORAGE":       storage,
		"SOURCE_BRANCH": mainBranch,
		"DEST_BRANCH":   featureBranch,
	}

	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" branch create sg://"+repoName+"/"+featureBranch+" --source sg://"+repoName+"/"+mainBranch, false, "hubctl_branch_create", featureBranchVars)

	// update 'file1' on feature branch and commit
	vars["FILE_PATH"] = filePath1
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" fs upload -s files/ro_1k_other sg://"+repoName+"/"+featureBranch+"/"+filePath1, false, "hubctl_fs_upload", vars)
	commitMessage = "file update on feature branch"
	vars["BRANCH"] = featureBranch
	vars["MESSAGE"] = commitMessage
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" commit sg://"+repoName+"/"+featureBranch+" -m \""+commitMessage+"\"", false, "hubctl_commit", vars)

	// update 'file2' on 'main' and commit
	vars["FILE_PATH"] = filePath2
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" fs upload -s files/ro_1k_other sg://"+repoName+"/"+featureBranch+"/"+filePath2, false, "hubctl_fs_upload", vars)
	commitMessage = "another file update on main branch"
	vars["MESSAGE"] = commitMessage
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" commit sg://"+repoName+"/"+featureBranch+" -m \""+commitMessage+"\"", false, "hubctl_commit", vars)

	cases := []struct {
		Name   string
		Squash bool
	}{
		{Name: "regular", Squash: false},
		{Name: "squash", Squash: true},
	}
	for _, tc := range cases {
		t.Run("merge with commit message and meta "+tc.Name, func(t *testing.T) {
			destBranch := "dest-" + tc.Name
			destBranchVars := map[string]string{
				"REPO":          repoName,
				"STORAGE":       storage,
				"SOURCE_BRANCH": mainBranch,
				"DEST_BRANCH":   destBranch,
			}
			// create new destBranch from main, before the additions to main.
			RunCmdAndVerifySuccessWithFile(t, Hubctl()+" branch create sg://"+repoName+"/"+destBranch+" --source sg://"+repoName+"/"+mainBranch, false, "hubctl_branch_create", destBranchVars)

			commitMessage = "merge commit"
			vars["MESSAGE"] = commitMessage
			meta := "key1=value1,key2=value2"
			squash := ""
			if tc.Squash {
				squash = "--squash"
			}
			destBranchVars["SOURCE_BRANCH"] = featureBranch
			RunCmdAndVerifySuccessWithFile(t, Hubctl()+" merge sg://"+repoName+"/"+featureBranch+" sg://"+repoName+"/"+destBranch+" -m '"+commitMessage+"' --meta "+meta+" "+squash, false, "hubctl_merge_success", destBranchVars)

			golden := "hubctl_merge_with_commit"
			if tc.Squash {
				golden = "hubctl_merge_with_squashed_commit"
			}
			RunCmdAndVerifySuccessWithFile(t, Hubctl()+" log --amount 1 sg://"+repoName+"/"+destBranch, false, golden, vars)
		})
	}
}

func TestHubctlMergeAndStrategies(t *testing.T) {
	repoName := generateUniqueRepositoryName()
	storage := generateUniqueStorageNamespace(repoName)
	vars := map[string]string{
		"REPO":    repoName,
		"STORAGE": storage,
		"BRANCH":  mainBranch,
	}

	featureBranch := "feature"
	branchVars := map[string]string{
		"REPO":          repoName,
		"STORAGE":       storage,
		"SOURCE_BRANCH": mainBranch,
		"DEST_BRANCH":   featureBranch,
	}

	filePath1 := "file1"
	filePath2 := "file2"
	lsVars := map[string]string{
		"REPO":        repoName,
		"STORAGE":     storage,
		"FILE_PATH_1": filePath1,
		"FILE_PATH_2": filePath2,
	}

	// create repo with 'main' branch
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" repo create sg://"+repoName+" "+storage, false, "hubctl_repo_create", vars)

	// upload 'file1' and commit
	vars["FILE_PATH"] = filePath1
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" fs upload -s files/ro_1k sg://"+repoName+"/"+mainBranch+"/"+filePath1, false, "hubctl_fs_upload", vars)
	commitMessage := "first commit to main"
	vars["MESSAGE"] = commitMessage
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" commit sg://"+repoName+"/"+mainBranch+" -m \""+commitMessage+"\"", false, "hubctl_commit", vars)

	// create new branch 'feature'
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" branch create sg://"+repoName+"/"+featureBranch+" --source sg://"+repoName+"/"+mainBranch, false, "hubctl_branch_create", branchVars)

	// update 'file1' on 'main' and commit
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" fs upload -s files/ro_1k_other sg://"+repoName+"/"+mainBranch+"/"+filePath1, false, "hubctl_fs_upload", vars)
	commitMessage = "file update on main branch"
	vars["MESSAGE"] = commitMessage
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" commit sg://"+repoName+"/"+mainBranch+" -m \""+commitMessage+"\"", false, "hubctl_commit", vars)

	// upload 'file2' on 'feature', delete 'file1' and commit
	vars["BRANCH"] = featureBranch
	vars["FILE_PATH"] = filePath2
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" fs upload -s files/ro_1k sg://"+repoName+"/"+featureBranch+"/"+filePath2, false, "hubctl_fs_upload", vars)
	RunCmdAndVerifySuccess(t, Hubctl()+" fs rm sg://"+repoName+"/"+featureBranch+"/"+filePath1, false, "", vars)
	commitMessage = "delete file on feature branch"
	vars["MESSAGE"] = commitMessage
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" commit sg://"+repoName+"/"+featureBranch+" -m \""+commitMessage+"\"", false, "hubctl_commit", vars)

	// try to merge - conflict
	RunCmdAndVerifyFailureWithFile(t, Hubctl()+" merge sg://"+repoName+"/"+mainBranch+" sg://"+repoName+"/"+featureBranch, false, "hubctl_merge_conflict", branchVars)
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" fs ls sg://"+repoName+"/"+featureBranch+"/", false, "hubctl_fs_ls_1_file", vars)

	// merge with strategy 'source-wins' - updated 'file1' from main is added to 'feature'
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" merge sg://"+repoName+"/"+mainBranch+" sg://"+repoName+"/"+featureBranch+" --strategy source-wins", false, "hubctl_merge_success", branchVars)
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" fs ls sg://"+repoName+"/"+featureBranch+"/", false, "hubctl_fs_ls_2_file", lsVars)

	// update 'file1' again on 'main' and commit
	vars["BRANCH"] = mainBranch
	vars["FILE_PATH"] = filePath1
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" fs upload -s files/ro_1k sg://"+repoName+"/"+mainBranch+"/"+filePath1, false, "hubctl_fs_upload", vars)
	commitMessage = "another file update on main branch"
	vars["MESSAGE"] = commitMessage
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" commit sg://"+repoName+"/"+mainBranch+" -m \""+commitMessage+"\"", false, "hubctl_commit", vars)

	// delete 'file1' on 'feature' again, and commit
	vars["BRANCH"] = featureBranch
	RunCmdAndVerifySuccess(t, Hubctl()+" fs rm sg://"+repoName+"/"+featureBranch+"/"+filePath1, false, "", vars)
	commitMessage = "delete file on feature branch again"
	vars["MESSAGE"] = commitMessage
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" commit sg://"+repoName+"/"+featureBranch+" -m \""+commitMessage+"\"", false, "hubctl_commit", vars)

	// try to merge - conflict
	vars["FILE_PATH"] = filePath2
	RunCmdAndVerifyFailureWithFile(t, Hubctl()+" merge sg://"+repoName+"/"+mainBranch+" sg://"+repoName+"/"+featureBranch, false, "hubctl_merge_conflict", branchVars)
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" fs ls sg://"+repoName+"/"+featureBranch+"/", false, "hubctl_fs_ls_1_file", vars)

	// merge with strategy 'dest-wins' - 'file1' is not added to 'feature'
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" merge sg://"+repoName+"/"+mainBranch+" sg://"+repoName+"/"+featureBranch+" --strategy dest-wins", false, "hubctl_merge_success", branchVars)
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" fs ls sg://"+repoName+"/"+featureBranch+"/", false, "hubctl_fs_ls_1_file", vars)
}

func TestHubctlLogNoMergesWithCommitsAndMerges(t *testing.T) {
	repoName := generateUniqueRepositoryName()
	storage := generateUniqueStorageNamespace(repoName)
	vars := map[string]string{
		"REPO":    repoName,
		"STORAGE": storage,
		"BRANCH":  mainBranch,
	}

	featureBranch := "feature"
	branchVars := map[string]string{
		"REPO":          repoName,
		"STORAGE":       storage,
		"SOURCE_BRANCH": mainBranch,
		"DEST_BRANCH":   featureBranch,
		"BRANCH":        featureBranch,
	}

	filePath1 := "file1"
	filePath2 := "file2"

	// create repo with 'main' branch
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" repo create sg://"+repoName+" "+storage, false, "hubctl_repo_create", vars)

	// upload 'file1' and commit
	vars["FILE_PATH"] = filePath1
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" fs upload -s files/ro_1k sg://"+repoName+"/"+mainBranch+"/"+filePath1, false, "hubctl_fs_upload", vars)
	commitMessage := "first commit to main"
	vars["MESSAGE"] = commitMessage
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" commit sg://"+repoName+"/"+mainBranch+" -m \""+commitMessage+"\"", false, "hubctl_commit", vars)

	// create new branch 'feature'
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" branch create sg://"+repoName+"/"+featureBranch+" --source sg://"+repoName+"/"+mainBranch, false, "hubctl_branch_create", branchVars)

	// upload 'file2' to feature branch and commit
	branchVars["FILE_PATH"] = filePath2
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" fs upload -s files/ro_1k sg://"+repoName+"/"+featureBranch+"/"+filePath2, false, "hubctl_fs_upload", branchVars)
	commitMessage = "second commit to feature branch"
	branchVars["MESSAGE"] = commitMessage
	vars["SECOND_MESSAGE"] = commitMessage
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" commit sg://"+repoName+"/"+featureBranch+" -m \""+commitMessage+"\"", false, "hubctl_commit", branchVars)

	// merge feature into main
	branchVars["SOURCE_BRANCH"] = featureBranch
	branchVars["DEST_BRANCH"] = mainBranch
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" merge sg://"+repoName+"/"+featureBranch+" sg://"+repoName+"/"+mainBranch, false, "hubctl_merge_success", branchVars)

	// log the commits without merges
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" log sg://"+repoName+"/"+mainBranch+" --no-merges", false, "hubctl_log_no_merges", vars)
}

func TestHubctlLogNoMergesAndAmount(t *testing.T) {
	repoName := generateUniqueRepositoryName()
	storage := generateUniqueStorageNamespace(repoName)
	vars := map[string]string{
		"REPO":    repoName,
		"STORAGE": storage,
		"BRANCH":  mainBranch,
	}

	featureBranch := "feature"
	branchVars := map[string]string{
		"REPO":          repoName,
		"STORAGE":       storage,
		"SOURCE_BRANCH": mainBranch,
		"DEST_BRANCH":   featureBranch,
		"BRANCH":        featureBranch,
	}

	filePath1 := "file1"
	filePath2 := "file2"

	// create repo with 'main' branch
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" repo create sg://"+repoName+" "+storage, false, "hubctl_repo_create", vars)

	// upload 'file1' and commit
	vars["FILE_PATH"] = filePath1
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" fs upload -s files/ro_1k sg://"+repoName+"/"+mainBranch+"/"+filePath1, false, "hubctl_fs_upload", vars)
	commitMessage := "first commit to main"
	vars["MESSAGE"] = commitMessage
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" commit sg://"+repoName+"/"+mainBranch+" -m \""+commitMessage+"\"", false, "hubctl_commit", vars)

	// create new branch 'feature'
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" branch create sg://"+repoName+"/"+featureBranch+" --source sg://"+repoName+"/"+mainBranch, false, "hubctl_branch_create", branchVars)

	// upload 'file2' to feature branch and commit
	branchVars["FILE_PATH"] = filePath2
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" fs upload -s files/ro_1k sg://"+repoName+"/"+featureBranch+"/"+filePath2, false, "hubctl_fs_upload", branchVars)
	commitMessage = "second commit to feature branch"
	branchVars["MESSAGE"] = commitMessage
	vars["SECOND_MESSAGE"] = commitMessage
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" commit sg://"+repoName+"/"+featureBranch+" -m \""+commitMessage+"\"", false, "hubctl_commit", branchVars)

	// merge feature into main
	branchVars["SOURCE_BRANCH"] = featureBranch
	branchVars["DEST_BRANCH"] = mainBranch
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" merge sg://"+repoName+"/"+featureBranch+" sg://"+repoName+"/"+mainBranch, false, "hubctl_merge_success", branchVars)

	// log the commits without merges
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" log sg://"+repoName+"/"+mainBranch+" --no-merges --amount=2", false, "hubctl_log_no_merges_amount", vars)
}

func TestHubctlAnnotate(t *testing.T) {
	repoName := generateUniqueRepositoryName()
	storage := generateUniqueStorageNamespace(repoName)
	vars := map[string]string{
		"REPO":    repoName,
		"STORAGE": storage,
		"BRANCH":  mainBranch,
	}

	// create fresh repo with 'main' branch
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" repo create sg://"+repoName+" "+storage, false, "hubctl_repo_create", vars)

	path := "aaa/bbb/ccc"
	vars["FILE_PATH"] = path
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" fs upload -s files/ro_1k sg://"+repoName+"/"+mainBranch+"/"+path, false, "hubctl_fs_upload", vars)
	path = "aaa/bbb/ddd"
	vars["FILE_PATH"] = path
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" fs upload -s files/ro_1k sg://"+repoName+"/"+mainBranch+"/"+path, false, "hubctl_fs_upload", vars)
	commitMessage := "commit #1"
	vars["MESSAGE"] = commitMessage
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" commit sg://"+repoName+"/"+mainBranch+" -m \""+commitMessage+"\"", false, "hubctl_commit", vars)
	path = "aaa/bbb/eee"
	vars["FILE_PATH"] = path
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" fs upload -s files/ro_1k sg://"+repoName+"/"+mainBranch+"/"+path, false, "hubctl_fs_upload", vars)
	commitMessage = "commit #2"
	vars["MESSAGE"] = commitMessage
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" commit sg://"+repoName+"/"+mainBranch+" -m \""+commitMessage+"\"", false, "hubctl_commit", vars)
	path = "aaa/fff/ggg"
	vars["FILE_PATH"] = path
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" fs upload -s files/ro_1k sg://"+repoName+"/"+mainBranch+"/"+path, false, "hubctl_fs_upload", vars)
	path = "aaa/fff/ggh"
	vars["FILE_PATH"] = path
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" fs upload -s files/ro_1k sg://"+repoName+"/"+mainBranch+"/"+path, false, "hubctl_fs_upload", vars)
	path = "aaa/hhh"
	vars["FILE_PATH"] = path
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" fs upload -s files/ro_1k sg://"+repoName+"/"+mainBranch+"/"+path, false, "hubctl_fs_upload", vars)
	path = "iii/jjj"
	vars["FILE_PATH"] = path
	commitMessage = "commit #3"
	vars["MESSAGE"] = commitMessage
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" commit sg://"+repoName+"/"+mainBranch+" -m \""+commitMessage+"\"", false, "hubctl_commit", vars)
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" fs upload -s files/ro_1k sg://"+repoName+"/"+mainBranch+"/"+path, false, "hubctl_fs_upload", vars)
	path = "iii/kkk/lll"
	vars["FILE_PATH"] = path
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" fs upload -s files/ro_1k sg://"+repoName+"/"+mainBranch+"/"+path, false, "hubctl_fs_upload", vars)
	path = "mmm"
	vars["FILE_PATH"] = path
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" fs upload -s files/ro_1k sg://"+repoName+"/"+mainBranch+"/"+path, false, "hubctl_fs_upload", vars)
	commitMessage = "commit #4"
	vars["MESSAGE"] = commitMessage
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" commit sg://"+repoName+"/"+mainBranch+" -m \""+commitMessage+"\"", false, "hubctl_commit", vars)

	delete(vars, "FILE_PATH")
	delete(vars, "MESSAGE")

	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" annotate sg://"+repoName+"/"+mainBranch+"/", false, "hubctl_annotate_top", vars)
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" annotate sg://"+repoName+"/"+mainBranch+"/ --recursive", false, "hubctl_annotate_top_recursive", vars)

	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" annotate sg://"+repoName+"/"+mainBranch+"/a", false, "hubctl_annotate_a", vars)
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" annotate sg://"+repoName+"/"+mainBranch+"/a --recursive", false, "hubctl_annotate_a_recursive", vars)
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" annotate sg://"+repoName+"/"+mainBranch+"/aa", false, "hubctl_annotate_a", vars)
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" annotate sg://"+repoName+"/"+mainBranch+"/aaa", false, "hubctl_annotate_a", vars)
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" annotate sg://"+repoName+"/"+mainBranch+"/aaa/ --recursive", false, "hubctl_annotate_a_recursive", vars)

	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" annotate sg://"+repoName+"/"+mainBranch+"/iii/kkk/l", false, "hubctl_annotate_iiikkklll", vars)
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" annotate sg://"+repoName+"/"+mainBranch+"/iii/kkk/l --recursive", false, "hubctl_annotate_iiikkklll", vars)
}

func TestHubctlAuthUsers(t *testing.T) {
	userName := "test_user"
	vars := map[string]string{
		"ID": userName,
	}
	isSupported := !isBasicAuth()

	// Not Found
	RunCmdAndVerifyFailure(t, Hubctl()+" auth users delete --id "+userName, false, "user not found\n404 Not Found\n", vars)

	// Check unique
	if isSupported {
		RunCmdAndVerifySuccessWithFile(t, Hubctl()+" auth users create --id "+userName, false, "hubctl_auth_users_create_success", vars)
	}
	RunCmdAndVerifyFailure(t, Hubctl()+" auth users create --id "+userName, false, "Already exists\n409 Conflict\n", vars)

	// Cleanup
	expected := "user not found\n404 Not Found\n"
	if isSupported {
		expected = "User deleted successfully\n"
	}
	runCmdAndVerifyResult(t, Hubctl()+" auth users delete --id "+userName, !isSupported, false, expected, vars)
}

// testing without user email for now, since it is a pain to config esti with a mail
func TestHubctlIdentity(t *testing.T) {
	userId := "mike"
	vars := map[string]string{
		"ID": userId,
	}
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" identity", false, "hubctl_identity", vars)
}

func TestHubctlFsDownload(t *testing.T) {
	repoName := generateUniqueRepositoryName()
	storage := generateUniqueStorageNamespace(repoName)
	vars := map[string]string{
		"REPO":    repoName,
		"STORAGE": storage,
		"BRANCH":  mainBranch,
	}
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" repo create sg://"+repoName+" "+storage, false, "hubctl_repo_create", vars)

	// upload some data
	const totalObjects = 5
	for i := 0; i < totalObjects; i++ {
		vars["FILE_PATH"] = fmt.Sprintf("data/ro/ro_1k.%d", i)
		RunCmdAndVerifySuccessWithFile(t, Hubctl()+" fs upload -s files/ro_1k sg://"+repoName+"/"+mainBranch+"/"+vars["FILE_PATH"], false, "hubctl_fs_upload", vars)
	}
	t.Run("single", func(t *testing.T) {
		src := "sg://" + repoName + "/" + mainBranch + "/data/ro/ro_1k.0"
		sanitizedResult := runCmd(t, Hubctl()+" fs download "+src, false, false, map[string]string{})
		require.Contains(t, sanitizedResult, "download: "+src)
	})

	t.Run("single_with_dest", func(t *testing.T) {
		src := "sg://" + repoName + "/" + mainBranch + "/data/ro/ro_1k.0"
		dest := t.TempDir()
		sanitizedResult := runCmd(t, Hubctl()+" fs download "+src+" "+dest, false, false, map[string]string{})
		require.Contains(t, sanitizedResult, "download: "+src)
		require.Contains(t, sanitizedResult, dest+"/"+"ro_1k.0")
	})

	t.Run("single_with_rel_dest", func(t *testing.T) {
		dest := t.TempDir()

		// Change directory
		currDir, err := os.Getwd()
		require.NoError(t, err)
		require.NoError(t, os.Chdir(dest))
		defer func() {
			require.NoError(t, os.Chdir(currDir))
		}()

		src := "sg://" + repoName + "/" + mainBranch + "/data/ro/ro_1k.0"
		sanitizedResult := runCmd(t, Hubctl()+" fs download "+src+" ./", false, false, map[string]string{})
		require.Contains(t, sanitizedResult, "download: "+src)
		require.Contains(t, sanitizedResult, dest+"/ro_1k.0")
	})

	t.Run("single_with_recursive_flag", func(t *testing.T) {
		dest := t.TempDir()
		RunCmdAndVerifyFailure(t, Hubctl()+" fs download sg://"+repoName+"/"+mainBranch+"/data/ro/ro_1k.0 "+dest+" --recursive", false, "No objects in path: sg://${REPO}/${BRANCH}/data/ro/ro_1k.0/\nError executing command.\n", vars)
	})

	t.Run("directory", func(t *testing.T) {
		sanitizedResult := runCmd(t, Hubctl()+" fs download --parallelism 1 sg://"+repoName+"/"+mainBranch+"/data --recursive", false, false, map[string]string{})
		require.Contains(t, sanitizedResult, "download ro/ro_1k.0")
		require.Contains(t, sanitizedResult, "download ro/ro_1k.1")
		require.Contains(t, sanitizedResult, "download ro/ro_1k.2")
		require.Contains(t, sanitizedResult, "download ro/ro_1k.3")
		require.Contains(t, sanitizedResult, "download ro/ro_1k.4")
		require.Contains(t, sanitizedResult, "Download Summary:")
		require.Contains(t, sanitizedResult, "Downloaded: 5")
		require.Contains(t, sanitizedResult, "Uploaded: 0")
		require.Contains(t, sanitizedResult, "Removed: 0")
	})

	t.Run("directory_with_dest", func(t *testing.T) {
		dest := t.TempDir()
		sanitizedResult := runCmd(t, Hubctl()+" fs download --parallelism 1 sg://"+repoName+"/"+mainBranch+"/data "+dest+" --recursive", false, false, map[string]string{})
		require.Contains(t, sanitizedResult, "download ro/ro_1k.0")
		require.Contains(t, sanitizedResult, "download ro/ro_1k.1")
		require.Contains(t, sanitizedResult, "download ro/ro_1k.2")
		require.Contains(t, sanitizedResult, "download ro/ro_1k.3")
		require.Contains(t, sanitizedResult, "download ro/ro_1k.4")
		require.Contains(t, sanitizedResult, "Download Summary:")
		require.Contains(t, sanitizedResult, "Downloaded: 5")
		require.Contains(t, sanitizedResult, "Uploaded: 0")
		require.Contains(t, sanitizedResult, "Removed: 0")
	})

	t.Run("directory_without_recursive", func(t *testing.T) {
		RunCmdAndVerifyFailure(t, Hubctl()+" fs download --parallelism 1 sg://"+repoName+"/"+mainBranch+"/data", false, "download failed: request failed: 404 Not Found\nError executing command.\n", map[string]string{})
	})
}

func TestHubctlFsUpload(t *testing.T) {
	repoName := generateUniqueRepositoryName()
	storage := generateUniqueStorageNamespace(repoName)
	vars := map[string]string{
		"REPO":    repoName,
		"STORAGE": storage,
		"BRANCH":  mainBranch,
	}
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" repo create sg://"+repoName+" "+storage, false, "hubctl_repo_create", vars)

	t.Run("single_file", func(t *testing.T) {
		vars["FILE_PATH"] = "data/ro/ro_1k.0"
		RunCmdAndVerifySuccessWithFile(t, Hubctl()+" fs upload sg://"+repoName+"/"+mainBranch+"/"+vars["FILE_PATH"]+" -s files/ro_1k", false, "hubctl_fs_upload", vars)
	})
	t.Run("single_file_with_separator", func(t *testing.T) {
		vars["FILE_PATH"] = "data/ro/ro_1k.0_sep/"
		RunCmdAndVerifyFailure(t, Hubctl()+" fs upload sg://"+repoName+"/"+mainBranch+"/"+vars["FILE_PATH"]+" -s files/ro_1k", false, "target path is not a valid URI\nError executing command.\n", vars)
	})
	t.Run("single_file_with_recursive", func(t *testing.T) {
		vars["FILE_PATH"] = "data/ro/ro_1k.0"
		RunCmdAndVerifySuccessWithFile(t, Hubctl()+" fs upload --recursive -s files/ro_1k sg://"+repoName+"/"+mainBranch+"/"+vars["FILE_PATH"]+" -s files/ro_1k", false, "hubctl_fs_upload", vars)
	})
	t.Run("dir", func(t *testing.T) {
		vars["FILE_PATH"] = "data/ro/"
		sanitizedResult := runCmd(t, Hubctl()+" fs upload --recursive -s files/ sg://"+repoName+"/"+mainBranch+"/"+vars["FILE_PATH"], false, false, vars)

		require.Contains(t, sanitizedResult, "diff 'local://files/' <--> 'sg://"+repoName+"/"+mainBranch+"/"+vars["FILE_PATH"]+"'...")
		require.Contains(t, sanitizedResult, "upload ro_1k")
		require.Contains(t, sanitizedResult, "upload ro_1k_other")
		require.Contains(t, sanitizedResult, "upload upload_file.txt")
		require.Contains(t, sanitizedResult, "Upload Summary:")
		require.Contains(t, sanitizedResult, "Downloaded: 0")
		require.Contains(t, sanitizedResult, "Uploaded: 3")
		require.Contains(t, sanitizedResult, "Removed: 0")
	})
	t.Run("exist_dir", func(t *testing.T) {
		vars["FILE_PATH"] = "data/ro/"
		sanitizedResult := runCmd(t, Hubctl()+" fs upload --recursive -s files/ sg://"+repoName+"/"+mainBranch+"/"+vars["FILE_PATH"], false, false, vars)
		require.Contains(t, sanitizedResult, "diff 'local://files/' <--> 'sg://"+repoName+"/"+mainBranch+"/"+vars["FILE_PATH"]+"'...")
		require.Contains(t, sanitizedResult, "Upload Summary:")
		require.Contains(t, sanitizedResult, "No changes")
	})
	t.Run("dir_without_recursive", func(t *testing.T) {
		vars["FILE_PATH"] = "data/ro/"
		RunCmdAndVerifyFailure(t, Hubctl()+" fs upload -s files/ sg://"+repoName+"/"+mainBranch+"/"+vars["FILE_PATH"], false, "target path is not a valid URI\nError executing command.\n", vars)
	})
	t.Run("dir_without_recursive_to_file", func(t *testing.T) {
		vars["FILE_PATH"] = "data/ro/1.txt"
		RunCmdAndVerifyFailureContainsText(t, Hubctl()+" fs upload -s files/ sg://"+repoName+"/"+mainBranch+"/"+vars["FILE_PATH"], false, "read files/: is a directory", vars)
	})
}

func getStorageConfig(t *testing.T) *apigen.StorageConfig {
	storageResp, err := client.GetStorageConfigWithResponse(context.Background())
	if err != nil {
		t.Fatalf("GetStorageConfig failed: %s", err)
	}
	if storageResp.JSON200 == nil {
		t.Fatalf("GetStorageConfig failed with stats: %s", storageResp.Status())
	}
	return storageResp.JSON200
}

func TestHubctlFsUpload_protectedBranch(t *testing.T) {
	repoName := generateUniqueRepositoryName()
	storage := generateUniqueStorageNamespace(repoName)
	vars := map[string]string{
		"REPO":    repoName,
		"STORAGE": storage,
		"BRANCH":  mainBranch,
	}
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" repo create sg://"+vars["REPO"]+" "+vars["STORAGE"], false, "hubctl_repo_create", vars)
	runCmd(t, Hubctl()+" branch-protect add sg://"+vars["REPO"]+"/  '*'", false, false, vars)
	RunCmdAndVerifyContainsText(t, Hubctl()+" branch-protect list sg://"+vars["REPO"]+"/ ", false, "*", vars)
	// BranchUpdateMaxInterval - sleep in order to overcome branch update caching
	time.Sleep(branchProtectTimeout)
	vars["FILE_PATH"] = "ro_1k.0"
	RunCmdAndVerifyFailure(t, Hubctl()+" fs upload sg://"+vars["REPO"]+"/"+vars["BRANCH"]+"/"+vars["FILE_PATH"]+" -s files/ro_1k", false, "cannot write to protected branch\n403 Forbidden\n", vars)
}

func TestHubctlFsRm_protectedBranch(t *testing.T) {
	repoName := generateUniqueRepositoryName()
	storage := generateUniqueStorageNamespace(repoName)
	vars := map[string]string{
		"REPO":    repoName,
		"STORAGE": storage,
		"BRANCH":  mainBranch,
	}

	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" repo create sg://"+vars["REPO"]+" "+vars["STORAGE"], false, "hubctl_repo_create", vars)
	vars["FILE_PATH"] = "ro_1k.0"
	runCmd(t, Hubctl()+" fs upload sg://"+vars["REPO"]+"/"+vars["BRANCH"]+"/"+vars["FILE_PATH"]+" -s files/ro_1k", false, false, vars)
	runCmd(t, Hubctl()+" commit sg://"+vars["REPO"]+"/"+vars["BRANCH"]+" --allow-empty-message -m \" \"", false, false, vars)
	runCmd(t, Hubctl()+" branch-protect add sg://"+vars["REPO"]+"/  '*'", false, false, vars)
	// BranchUpdateMaxInterval - sleep in order to overcome branch update caching
	time.Sleep(branchProtectTimeout)
	RunCmdAndVerifyContainsText(t, Hubctl()+" branch-protect list sg://"+vars["REPO"]+"/ ", false, "*", vars)
	RunCmdAndVerifyFailure(t, Hubctl()+" fs rm sg://"+vars["REPO"]+"/"+vars["BRANCH"]+"/"+vars["FILE_PATH"], false, "cannot write to protected branch\n403 Forbidden\n", vars)
}

func TestHubctlFsPresign(t *testing.T) {
	config := getStorageConfig(t)
	if !config.PreSignSupport {
		t.Skip()
	}
	repoName := generateUniqueRepositoryName()
	storage := generateUniqueStorageNamespace(repoName)
	vars := map[string]string{
		"REPO":    repoName,
		"STORAGE": storage,
		"BRANCH":  mainBranch,
	}
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" repo create sg://"+repoName+" "+storage, false, "hubctl_repo_create", vars)

	// upload some data
	const totalObjects = 2
	for i := 0; i < totalObjects; i++ {
		vars["FILE_PATH"] = fmt.Sprintf("data/ro/ro_1k.%d", i)
		RunCmdAndVerifySuccessWithFile(t, Hubctl()+" fs upload -s files/ro_1k sg://"+repoName+"/"+mainBranch+"/"+vars["FILE_PATH"], false, "hubctl_fs_upload", vars)
	}

	goldenFile := "hubctl_fs_presign"
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" fs presign sg://"+repoName+"/"+mainBranch+"/data/ro/ro_1k.0", false, goldenFile, map[string]string{
		"REPO":    repoName,
		"STORAGE": storage,
		"BRANCH":  mainBranch,
		"PATH":    "data/ro",
		"FILE":    "ro_1k.0",
	})
}

func TestHubctlFsStat(t *testing.T) {
	repoName := generateUniqueRepositoryName()
	storage := generateUniqueStorageNamespace(repoName)
	vars := map[string]string{
		"REPO":    repoName,
		"STORAGE": storage,
		"BRANCH":  mainBranch,
	}
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" repo create sg://"+repoName+" "+storage, false, "hubctl_repo_create", vars)

	// upload some data
	const totalObjects = 2
	for i := 0; i < totalObjects; i++ {
		vars["FILE_PATH"] = fmt.Sprintf("data/ro/ro_1k.%d", i)
		RunCmdAndVerifySuccessWithFile(t, Hubctl()+" fs upload -s files/ro_1k sg://"+repoName+"/"+mainBranch+"/"+vars["FILE_PATH"], false, "hubctl_fs_upload", vars)
	}

	t.Run("default", func(t *testing.T) {
		config := getStorageConfig(t)
		goldenFile := "hubctl_stat_default"
		if config.PreSignSupport {
			goldenFile = "hubctl_stat_pre_sign"
			if config.BlockstoreType == "s3" {
				goldenFile = "hubctl_stat_pre_sign_with_expiry"
			}
		}
		RunCmdAndVerifySuccessWithFile(t, Hubctl()+" fs stat sg://"+repoName+"/"+mainBranch+"/data/ro/ro_1k.0", false, goldenFile, map[string]string{
			"REPO":    repoName,
			"STORAGE": storage,
			"BRANCH":  mainBranch,
			"PATH":    "data/ro",
			"FILE":    "ro_1k.0",
		})
	})

	t.Run("no_presign", func(t *testing.T) {
		RunCmdAndVerifySuccessWithFile(t, Hubctl()+" fs stat --pre-sign=false sg://"+repoName+"/"+mainBranch+"/data/ro/ro_1k.0", false, "hubctl_stat_default", map[string]string{
			"REPO":    repoName,
			"STORAGE": storage,
			"BRANCH":  mainBranch,
			"PATH":    "data/ro",
			"FILE":    "ro_1k.0",
		})
	})

	t.Run("pre-sign", func(t *testing.T) {
		config := getStorageConfig(t)
		if !config.PreSignSupport {
			t.Skip("No pre-sign support for this storage")
		}
		goldenFile := "hubctl_stat_pre_sign"
		if config.BlockstoreType == "s3" {
			goldenFile = "hubctl_stat_pre_sign_with_expiry"
		}
		RunCmdAndVerifySuccessWithFile(t, Hubctl()+" fs stat --pre-sign sg://"+repoName+"/"+mainBranch+"/data/ro/ro_1k.1", false, goldenFile, map[string]string{
			"REPO":    repoName,
			"STORAGE": storage,
			"BRANCH":  mainBranch,
			"PATH":    "data/ro",
			"FILE":    "ro_1k.1",
		})
	})
}

func TestHubctlImport(t *testing.T) {
	// TODO(barak): generalize test to work all supported object stores
	const IngestTestBucketPath = "s3://esti-system-testing-data/ingest-test-data/"
	skipOnSchemaMismatch(t, IngestTestBucketPath)

	repoName := generateUniqueRepositoryName()
	storage := generateUniqueStorageNamespace(repoName)
	vars := map[string]string{
		"REPO":    repoName,
		"STORAGE": storage,
		"BRANCH":  mainBranch,
		"OBJECTS": "10",
	}

	const from = "s3://hubctl-ingest-test-data"
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" repo create sg://"+repoName+" "+storage, false, "hubctl_repo_create", vars)
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" import --no-progress --from "+from+" --to sg://"+repoName+"/"+mainBranch+"/to/", false, "hubctl_import", vars)
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" import --no-progress --from "+from+" --to sg://"+repoName+"/"+mainBranch+"/too/ --message \"import too\"", false, "hubctl_import_with_message", vars)
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" import --no-progress --from "+from+" --to sg://"+repoName+"/"+mainBranch+"/another/import/ --merge", false, "hubctl_import_and_merge", vars)
}

func TestHubctlCherryPick(t *testing.T) {
	repoName := generateUniqueRepositoryName()
	storage := generateUniqueStorageNamespace(repoName)
	vars := map[string]string{
		"REPO":    repoName,
		"STORAGE": storage,
		"BRANCH":  mainBranch,
	}
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" repo create sg://"+repoName+" "+storage, false, "hubctl_repo_create", vars)

	branch1 := "branch1"
	branch2 := "branch2"
	branchVars := map[string]string{
		"REPO":          repoName,
		"SOURCE_BRANCH": mainBranch,
		"DEST_BRANCH":   "branch1",
	}
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" branch create sg://"+repoName+"/"+branch1+" --source sg://"+repoName+"/"+mainBranch, false, "hubctl_branch_create", branchVars)
	branchVars["DEST_BRANCH"] = "branch2"
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" branch create sg://"+repoName+"/"+branch2+" --source sg://"+repoName+"/"+mainBranch, false, "hubctl_branch_create", branchVars)

	// upload some data
	vars["BRANCH"] = branch1
	for i := 1; i <= 3; i++ {
		vars["FILE_PATH"] = fmt.Sprintf("data/%d", i)
		commitMessage := fmt.Sprintf("commit %d", i)
		RunCmdAndVerifySuccessWithFile(t, Hubctl()+" fs upload -s files/ro_1k sg://"+repoName+"/"+branch1+"/"+vars["FILE_PATH"], false, "hubctl_fs_upload", vars)

		commitVars := map[string]string{
			"REPO":    repoName,
			"BRANCH":  branch1,
			"MESSAGE": commitMessage,
		}
		RunCmdAndVerifySuccessWithFile(t, Hubctl()+" commit sg://"+repoName+"/"+branch1+` -m "`+commitMessage+`" --epoch-time-seconds 0`, false, "hubctl_commit", commitVars)
	}

	vars["BRANCH"] = branch2
	for i := 3; i <= 5; i++ {
		vars["FILE_PATH"] = fmt.Sprintf("data/%d", i)
		commitMessage := fmt.Sprintf("commit %d", i)
		RunCmdAndVerifySuccessWithFile(t, Hubctl()+" fs upload -s files/ro_1k_other sg://"+repoName+"/"+branch2+"/"+vars["FILE_PATH"], false, "hubctl_fs_upload", vars)

		commitVars := map[string]string{
			"REPO":    repoName,
			"BRANCH":  branch2,
			"MESSAGE": commitMessage,
		}
		RunCmdAndVerifySuccessWithFile(t, Hubctl()+" commit sg://"+repoName+"/"+branch2+` -m "`+commitMessage+`" --epoch-time-seconds 0`, false, "hubctl_commit", commitVars)
	}

	t.Run("success", func(t *testing.T) {
		RunCmdAndVerifySuccessWithFile(t, Hubctl()+" cherry-pick sg://"+repoName+"/"+branch1+" sg://"+repoName+"/"+mainBranch, false, "hubctl_cherry_pick", map[string]string{
			"REPO":    repoName,
			"BRANCH":  mainBranch,
			"MESSAGE": "commit 3",
		})

		RunCmdAndVerifySuccessWithFile(t, Hubctl()+" cherry-pick sg://"+repoName+"/"+branch2+"~1"+" sg://"+repoName+"/"+mainBranch, false, "hubctl_cherry_pick", map[string]string{
			"REPO":    repoName,
			"BRANCH":  mainBranch,
			"MESSAGE": "commit 4",
		})
	})

	t.Run("conflict", func(t *testing.T) {
		RunCmdAndVerifyFailure(t, Hubctl()+" cherry-pick sg://"+repoName+"/"+branch1+" sg://"+repoName+"/"+branch2, false,
			fmt.Sprintf("Branch: sg://%s/%s\nupdate branch: conflict found\n409 Conflict\n", repoName, branch2), nil)
	})
}

func TestHubctlBisect(t *testing.T) {
	repoName := generateUniqueRepositoryName()
	storage := generateUniqueStorageNamespace(repoName)
	vars := map[string]string{
		"REPO":    repoName,
		"STORAGE": storage,
		"BRANCH":  mainBranch,
	}

	r := strings.NewReplacer("{hubctl}", Hubctl(), "{repo}", repoName, "{storage}", storage, "{branch}", "main")
	runCmd(t, r.Replace("{hubctl} repo create sg://{repo} {storage}"), false, false, nil)

	// generate to test data
	for i := 0; i < 5; i++ {
		obj := fmt.Sprintf("file%d", i)
		runCmd(t, r.Replace("{hubctl} fs upload -s files/ro_1k sg://{repo}/{branch}/")+obj, false, false, nil)
		commit := fmt.Sprintf("commit%d", i)
		runCmd(t, r.Replace("{hubctl} commit sg://{repo}/{branch} -m ")+commit, false, false, nil)
	}
	RunCmdAndVerifyFailureWithFile(t, r.Replace("{hubctl} bisect good"), false,
		"hubctl_bisect_good_invalid", vars)
	RunCmdAndVerifyFailureWithFile(t, r.Replace("{hubctl} bisect bad"), false,
		"hubctl_bisect_bad_invalid", vars)
	RunCmdAndVerifySuccessWithFile(t, r.Replace("{hubctl} bisect start sg://{repo}/{branch} sg://{repo}/{branch}~5"), false,
		"hubctl_bisect_start", vars)
	RunCmdAndVerifySuccessWithFile(t, r.Replace("{hubctl} bisect view"), false,
		"hubctl_bisect_view1", vars)
	RunCmdAndVerifySuccessWithFile(t, r.Replace("{hubctl} bisect good"), false,
		"hubctl_bisect_good1", vars)
	RunCmdAndVerifySuccessWithFile(t, r.Replace("{hubctl} bisect view"), false,
		"hubctl_bisect_view2", vars)
	RunCmdAndVerifySuccessWithFile(t, r.Replace("{hubctl} bisect log"), false,
		"hubctl_bisect_log1", vars)
	RunCmdAndVerifySuccessWithFile(t, r.Replace("{hubctl} bisect bad"), false,
		"hubctl_bisect_bad1", vars)
	RunCmdAndVerifySuccessWithFile(t, r.Replace("{hubctl} bisect log"), false,
		"hubctl_bisect_log2", vars)
	RunCmdAndVerifySuccessWithFile(t, r.Replace("{hubctl} bisect reset"), false,
		"hubctl_bisect_reset", vars)
}

func TestHubctlUsage(t *testing.T) {
	repoName := generateUniqueRepositoryName()
	storage := generateUniqueStorageNamespace(repoName)
	vars := map[string]string{
		"REPO":    repoName,
		"STORAGE": storage,
		"BRANCH":  mainBranch,
	}

	r := strings.NewReplacer("{hubctl}", Hubctl(), "{repo}", repoName, "{storage}", storage, "{branch}", "main")
	runCmd(t, r.Replace("{hubctl} repo create sg://{repo} {storage}"), false, false, nil)
	runCmd(t, r.Replace("{hubctl} repo list"), false, false, nil)
	RunCmdAndVerifyFailureWithFile(t, r.Replace("{hubctl} usage summary"), false, "hubctl_usage_summary", vars)
}

func TestHubctlBranchProtection(t *testing.T) {
	repoName := generateUniqueRepositoryName()
	storage := generateUniqueStorageNamespace(repoName)
	vars := map[string]string{
		"REPO":    repoName,
		"STORAGE": storage,
		"BRANCH":  mainBranch,
	}
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" repo create sg://"+repoName+" "+storage, false, "hubctl_repo_create", vars)

	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" branch-protect add sg://"+repoName+" "+mainBranch, false, "hubctl_empty", vars)
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" branch-protect list sg://"+repoName, false, "hubctl_branch_protection_list.term", vars)
}

// TestHubctlAbuse runs a series of abuse commands to test the functionality of hubctl abuse (not in order to test how Surogate Hub handles abuse)
func TestHubctlAbuse(t *testing.T) {
	repoName := generateUniqueRepositoryName()
	storage := generateUniqueStorageNamespace(repoName)
	vars := map[string]string{
		"REPO":    repoName,
		"STORAGE": storage,
		"BRANCH":  mainBranch,
	}
	RunCmdAndVerifySuccessWithFile(t, Hubctl()+" repo create sg://"+repoName+" "+storage, false, "hubctl_repo_create", vars)

	fromFile := ""
	const totalObjects = 5
	for i := 0; i < totalObjects; i++ {
		vars["FILE_PATH"] = fmt.Sprintf("data/ro/ro_1k.%d", i)
		fromFile = fromFile + vars["FILE_PATH"] + "\n"
		runCmd(t, Hubctl()+" fs upload -s files/ro_1k sg://"+repoName+"/"+mainBranch+"/"+vars["FILE_PATH"], false, false, vars)
	}
	f, err := os.CreateTemp("", "abuse-read")
	require.NoError(t, err)
	_, err = f.WriteString(fromFile)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	const (
		abuseAmount      = 50
		abuseParallelism = 3
	)
	tests := []struct {
		Cmd            string
		Amount         int
		AdditionalArgs string
	}{
		{
			Cmd:    "commit",
			Amount: 10,
		},
		{
			Cmd:            "create-branches",
			Amount:         abuseAmount,
			AdditionalArgs: fmt.Sprintf("--parallelism %d", abuseParallelism),
		},
		{
			Cmd:            "link-same-object",
			Amount:         abuseAmount,
			AdditionalArgs: fmt.Sprintf("--parallelism %d", abuseParallelism),
		},
		{
			Cmd:            "list",
			Amount:         abuseAmount,
			AdditionalArgs: fmt.Sprintf("--parallelism %d", abuseParallelism),
		},
		{
			Cmd:            "random-read",
			Amount:         abuseAmount,
			AdditionalArgs: fmt.Sprintf("--parallelism %d --from-file %s", abuseParallelism, f.Name()),
		},
		{
			Cmd:            "random-delete",
			Amount:         abuseAmount,
			AdditionalArgs: fmt.Sprintf("--parallelism %d --from-file %s", abuseParallelism, f.Name()),
		},
		{
			Cmd:            "random-write",
			Amount:         abuseAmount,
			AdditionalArgs: fmt.Sprintf("--parallelism %d", abuseParallelism),
		},
	}
	for _, tt := range tests {
		t.Run(tt.Cmd, func(t *testing.T) {
			hubURI := "sg://" + repoName + "/" + mainBranch
			RunCmdAndVerifyContainsText(t, fmt.Sprintf("%s abuse %s %s --amount %d %s", Hubctl(), tt.Cmd, hubURI, tt.Amount, tt.AdditionalArgs), false, "errors: 0", map[string]string{})
		})
	}
}
