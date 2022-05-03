package main

import (
	"net"

	"github.com/Eventual-Inc/Daft/codegen/go/DaftWeb"
	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding"
)

const addr = "0.0.0.0:8080"

func main() {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		logrus.Fatalf("Failed to listen: %v", err)
	}

	ser := grpc.NewServer()
	encoding.RegisterCodec(flatbuffers.FlatbuffersCodec{})
	DaftWeb.RegisterWebServer(ser, &webServer{})

	if err := ser.Serve(lis); err != nil {
		logrus.Fatalf("Failed to serve: %v", err)
	}
}
