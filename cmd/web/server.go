package main

import (
	"context"

	"github.com/Eventual-Inc/Daft/codegen/go/DaftWeb"
	flatbuffers "github.com/google/flatbuffers/go"
)

type webServer struct {
	DaftWeb.UnimplementedWebServer
}

func (s *webServer) Invoke(ctx context.Context, req *DaftWeb.InvocationRequest) (*flatbuffers.Builder, error) {
	builder := flatbuffers.NewBuilder(0)
	functionId := builder.CreateByteString(req.FunctionId())
	DaftWeb.InvocationResponseStart(builder)
	DaftWeb.InvocationResponseAddFunctionId(builder, functionId)
	responseEnd := DaftWeb.InvocationResponseEnd(builder)
	builder.Finish(responseEnd)
	return builder, nil
}
