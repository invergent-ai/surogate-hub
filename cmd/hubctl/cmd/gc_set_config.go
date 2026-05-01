package cmd

import (
	"encoding/json"
	"io"
	"net/http"
	"os"

	"github.com/invergent-ai/surogate-hub/pkg/api/apigen"
	"github.com/spf13/cobra"
)

const (
	gcSetConfigCmdArgs = 1
	filenameFlagName   = "filename"
)

var gcSetConfigCmd = &cobra.Command{
	Use:   "set-config <repository URI>",
	Short: "Set garbage collection policy JSON",
	Long: `Sets the garbage collection policy JSON.
Example configuration file:
{
  "default_retention_days": 21,
  "branches": [
    {
      "branch_id": "main",
      "retention_days": 28
    },
    {
      "branch_id": "dev",
      "retention_days": 14
    }
  ]
}`,
	Example: "hubctl gc set-config " + myRepoExample + " -f config.json",
	Args:    cobra.ExactArgs(gcSetConfigCmdArgs),
	Run: func(cmd *cobra.Command, args []string) {
		u := MustParseRepoURI("repository URI", args[0])
		filename := Must(cmd.Flags().GetString(filenameFlagName))
		var reader io.ReadCloser
		var err error
		if filename == "-" {
			reader = os.Stdin
		} else {
			reader, err = os.Open(filename)
			if err != nil {
				DieErr(err)
			}
			defer func() {
				_ = reader.Close()
			}()
		}
		var body apigen.SetGCRulesJSONRequestBody
		err = json.NewDecoder(reader).Decode(&body)
		if err != nil {
			DieErr(err)
		}
		client := getClient()
		resp, err := client.SetGCRules(cmd.Context(), apigen.RepositoryOwner(u.Repository), apigen.RepositoryName(u.Repository), body)
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusNoContent)
	},
}

//nolint:gochecknoinits
func init() {
	gcSetConfigCmd.Flags().StringP(filenameFlagName, "f", "", "file containing the GC policy as JSON")
	_ = gcSetConfigCmd.MarkFlagRequired(filenameFlagName)

	gcCmd.AddCommand(gcSetConfigCmd)
}
