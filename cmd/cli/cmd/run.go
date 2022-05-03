package cmd

import (
	"fmt"

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
		authToken := viper.GetString(configKeyBearerAuthToken)
		fmt.Println(authToken)
	},
}
