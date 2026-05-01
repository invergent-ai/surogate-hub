package cmd

import (
	"os"

	"github.com/spf13/cobra"
)

// completionCmd represents the completion command
var completionCmd = &cobra.Command{
	Use:   "completion <bash|zsh|fish>",
	Short: "Generate completion script",
	Long: `To load completions:

Bash:

` + "```" + `sh
$ source <(hubctl completion bash)
` + "```" + `

To load completions for each session, execute once:
Linux:

` + "```" + `sh
$ hubctl completion bash > /etc/bash_completion.d/hubctl
` + "```" + `

MacOS:

` + "```" + `sh
$ hubctl completion bash > /usr/local/etc/bash_completion.d/hubctl
` + "```" + `

Zsh:

If shell completion is not already enabled in your environment you will need
to enable it.  You can execute the following once:

` + "```" + `sh
$ echo "autoload -U compinit; compinit" >> ~/.zshrc
` + "```" + `

To load completions for each session, execute once:
` + "```" + `sh
$ hubctl completion zsh > "${fpath[1]}/_hubctl"
` + "```" + `

You will need to start a new shell for this setup to take effect.

Fish:

` + "```" + `sh
$ hubctl completion fish | source
` + "```" + `

To load completions for each session, execute once:

` + "```" + `sh
$ hubctl completion fish > ~/.config/fish/completions/hubctl.fish
` + "```" + `

`,
	DisableFlagsInUseLine: true,
	ValidArgs:             []string{"bash", "zsh", "fish"},
	Args:                  cobra.MatchAll(cobra.ExactArgs(1), cobra.OnlyValidArgs),
	Run: func(cmd *cobra.Command, args []string) {
		switch args[0] {
		case "bash":
			_ = cmd.Root().GenBashCompletion(os.Stdout)
		case "zsh":
			_ = cmd.Root().GenZshCompletion(os.Stdout)
		case "fish":
			_ = cmd.Root().GenFishCompletion(os.Stdout, true)
		}
	},
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(completionCmd)
}
