package cmd

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/invergent-ai/surogate-hub/pkg/auth"
	"github.com/invergent-ai/surogate-hub/pkg/auth/acl"
	"github.com/invergent-ai/surogate-hub/pkg/auth/crypt"
	"github.com/invergent-ai/surogate-hub/pkg/auth/model"
	authparams "github.com/invergent-ai/surogate-hub/pkg/auth/params"
	"github.com/invergent-ai/surogate-hub/pkg/auth/setup"
	"github.com/invergent-ai/surogate-hub/pkg/kv"
	"github.com/invergent-ai/surogate-hub/pkg/kv/kvparams"
	"github.com/invergent-ai/surogate-hub/pkg/logging"
	"github.com/invergent-ai/surogate-hub/pkg/stats"
	"github.com/invergent-ai/surogate-hub/pkg/version"
	"github.com/spf13/cobra"
)

// superuserCmd represents the init command
var superuserCmd = &cobra.Command{
	Use:   "superuser",
	Short: "Create additional user with admin credentials",
	Long: `Create additional user with admin credentials.
This command can be used to import an admin user when moving from Surogate Hub version 
with previously configured users to a Surogate Hub with basic auth version.
To do that provide the user name as well as the access key ID to import. 
If the wrong user or credentials were chosen it is possible to delete the user and perform the action again.
`,
	Run: func(cmd *cobra.Command, args []string) {
		cfg := loadConfig().GetBaseConfig()

		userName, err := cmd.Flags().GetString("user-name")
		if err != nil {
			fmt.Printf("user-name: %s\n", err)
			os.Exit(1)
		}
		accessKeyID, err := cmd.Flags().GetString("access-key-id")
		if err != nil {
			fmt.Printf("access-key-id: %s\n", err)
			os.Exit(1)
		}
		secretAccessKey, err := cmd.Flags().GetString("secret-access-key")
		if err != nil {
			fmt.Printf("secret-access-key: %s\n", err)
			os.Exit(1)
		}

		logger := logging.ContextUnavailable()
		ctx := cmd.Context()
		kvParams, err := kvparams.NewConfig(&cfg.Database)
		if err != nil {
			fmt.Printf("KV params: %s\n", err)
			os.Exit(1)
		}
		kvStore, err := kv.Open(ctx, kvParams)
		if err != nil {
			fmt.Printf("Failed to open KV store: %s\n", err)
			os.Exit(1)
		}

		secretStore := crypt.NewSecretStore([]byte(cfg.Auth.Encrypt.SecretKey))
		var authService auth.Service = acl.NewAuthService(kvStore, secretStore, authparams.ServiceCache(cfg.Auth.Cache))
		addToAdmins := true

		authMetadataManager := auth.NewKVMetadataManager(version.Version, cfg.Installation.FixedID, cfg.Database.Type, kvStore)

		metadataProvider := stats.BuildMetadataProvider(logger, cfg)
		metadata := stats.NewMetadata(ctx, logger, cfg.Blockstore.Type, authMetadataManager, metadataProvider)
		credentials, err := setup.AddAdminUser(ctx, authService, &model.SuperuserConfiguration{
			User: model.User{
				CreatedAt: time.Now(),
				Username:  userName,
			},
			AccessKeyID:     accessKeyID,
			SecretAccessKey: secretAccessKey,
		}, addToAdmins)
		if err != nil {
			fmt.Printf("Failed to setup admin user: %s\n", err)
			os.Exit(1)
		}

		ctx, cancelFn := context.WithCancel(ctx)
		collector := stats.NewBufferedCollector(metadata.InstallationID, stats.Config(cfg.Stats),
			stats.WithLogger(logger.WithField("service", "stats_collector")))
		collector.Start(ctx)
		defer collector.Close()

		collector.CollectMetadata(metadata)
		collector.CollectEvent(stats.Event{Class: "global", Name: "superuser"})

		fmt.Printf("credentials:\n  access_key_id: %s\n  secret_access_key: %s\n",
			credentials.AccessKeyID, credentials.SecretAccessKey)

		cancelFn()
	},
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(superuserCmd)
	f := superuserCmd.Flags()
	f.String("user-name", "", "an identifier for the user (e.g. \"jane.doe\")")
	f.String("access-key-id", "", "create this access key ID for the user (for ease of integration)")
	f.String("secret-access-key", "", "use this access key secret (potentially insecure, use carefully for ease of integration)")

	_ = superuserCmd.MarkFlagRequired("user-name")
}
