package cmd

import (
	"fmt"
  
	"github.com/spf13/cobra"
  )
  
  func init() {
	rootCmd.AddCommand(loginCmd)
  }
  
  var loginCmd = &cobra.Command{
	Use:   "login",
	Short: "Logs in to Daft",
	Long:  `Authenticates and logs in to your Daft deployment`,
	Run: func(cmd *cobra.Command, args []string) {
	  fmt.Println("This function is a WIP - contact your Daft administrators for the appropriate steps to authenticate with Daft")
	},
  }
