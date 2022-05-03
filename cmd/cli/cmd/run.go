package cmd

import (
	"fmt"

	openapi "github.com/Eventual-Inc/Daft/codegen/openapi/gen-openapi-web-client"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func init() {
	rootCmd.AddCommand(runCmd)
}

var runCmd = &cobra.Command{
	Use:   "run",
	Short: "Runs a Data Function",
	Long:  `Runs a Data Function a single time, useful for debugging and for adhoc runs`,
	Run: func(cmd *cobra.Command, args []string) {
		ctx := cmd.Context()
		authToken := viper.GetString(configKeyBearerAuthToken)
		cfg := openapi.NewConfiguration()
		cfg.AddDefaultHeader("Authorization", fmt.Sprintf("Bearer %s", authToken))
		client := openapi.NewAPIClient(cfg)
		request := client.DefaultApi.PostInvocations(ctx)
		response, httpResponse, err := client.DefaultApi.PostInvocationsExecute(request)
		fmt.Println(response, httpResponse, err)
	},
}
