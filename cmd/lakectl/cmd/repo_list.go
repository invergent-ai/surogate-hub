package cmd

import (
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/api/apiutil"
)

var repoListCmd = &cobra.Command{
	Use:   "list",
	Short: "List repositories",
	Args:  cobra.NoArgs,
	ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		return nil, cobra.ShellCompDirectiveNoFileComp
	},
	Run: func(cmd *cobra.Command, args []string) {
		amount := Must(cmd.Flags().GetInt("amount"))
		after := Must(cmd.Flags().GetString("after"))
		clt := getClient()

		resp, err := clt.ListRepositoriesWithResponse(cmd.Context(), &apigen.ListRepositoriesParams{
			After:  apiutil.Ptr(apigen.PaginationAfter(after)),
			Amount: apiutil.Ptr(apigen.PaginationAmount(amount)),
		})
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusOK)
		if resp.JSON200 == nil {
			Die("Bad response from server", 1)
		}
		repos := resp.JSON200.Results
		rows := make([][]interface{}, len(repos))
		for i, repo := range repos {
			ts := time.Unix(repo.CreationDate, 0).String()
			rows[i] = []interface{}{repo.Id, ts, repo.DefaultBranch, repo.StorageNamespace, mapToString(repo.Metadata)}
		}
		pagination := resp.JSON200.Pagination
		PrintTable(rows, []interface{}{"Repository", "Creation Date", "Default Ref Name", "Storage Namespace", "Metadata"}, &pagination, amount)
	},
}

func mapToString(metadata *apigen.RepositoryMetadata) string {
	if metadata == nil {
		return "(empty)"
	}

	if len(metadata.AdditionalProperties) == 0 {
		return "(empty)"
	}
	keys := make([]string, 0, len(metadata.AdditionalProperties))
	for k := range metadata.AdditionalProperties {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var result []string
	for _, k := range keys {
		result = append(result, fmt.Sprintf("%s=%s", k, metadata.AdditionalProperties[k]))
	}

	return strings.Join(result, ",")
}

//nolint:gochecknoinits
func init() {
	repoListCmd.Flags().Int("amount", defaultAmountArgumentValue, "number of results to return")
	repoListCmd.Flags().String("after", "", "show results after this value (used for pagination)")

	repoCmd.AddCommand(repoListCmd)
}
