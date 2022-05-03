package cmd

import (
	"fmt"
	"log"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/Eventual-Inc/Daft/codegen/go/DaftWeb"
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
		fmt.Println("Mock auth token:", authToken)
		addr := viper.GetString(configKeyDaftWebURL)
		fmt.Println("Using address:", addr)
		conn, err := grpc.Dial(
			addr,
			// We can use this to hardcode tokens for authentication using Interceptors:
			// https://github.com/grpc/grpc-go/issues/106#issuecomment-246978683
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithDefaultCallOptions(grpc.ForceCodec(flatbuffers.FlatbuffersCodec{})),
		)
		if err != nil {
			log.Fatalf("Failed to connect: %v", err)
		}
		defer conn.Close()

		client := DaftWeb.NewWebClient(conn)

		builder := flatbuffers.NewBuilder(0)
		functionId := builder.CreateString("foobar")
		DaftWeb.InvocationRequestStart(builder)
		DaftWeb.InvocationRequestAddFunctionId(builder, functionId)
		req := DaftWeb.InvocationRequestEnd(builder)
		builder.Finish(req)

		response, err := client.Invoke(cmd.Context(), builder)
		if err != nil {
			log.Fatalf("Error when invoking: %v", err)
		}
		fmt.Println("Function ID:", string(response.FunctionId()))
	},
}
