package cmd

import "github.com/spf13/cobra"

var gcCmd = &cobra.Command{
	Use:   "gc",
	Short: "Run garbage collection tasks",
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(gcCmd)
}
