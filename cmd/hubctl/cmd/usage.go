package cmd

import (
	"github.com/spf13/cobra"
)

var usageCmd = &cobra.Command{
	Use:    "usage <sub command>",
	Short:  "Usage reports from Surogate Hub",
	Hidden: true,
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(usageCmd)
}
