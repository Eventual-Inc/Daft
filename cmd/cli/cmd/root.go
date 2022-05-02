/*
Copyright © 2022 Jay Chia jay@eventualcomputing.com

*/
package cmd

import (
	"os"

	"github.com/spf13/cobra"
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "daft",
	Short: "Data Functions as a Service",
	Long: `Daft is a platform for developing and executing Data Functions.
	
You can use Daft to quickly build and test functions developed locally on your machine on data that
resides locally or in cloud storage. When you are ready, deploy these functions to run on massive amounts
of data in the cloud and let Daft do the heavy infrastructure lifting for you.
`,
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func init() {}


