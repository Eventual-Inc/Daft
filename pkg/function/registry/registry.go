package registry

import (
	"context"

	"github.com/Eventual-Inc/Daft/pkg/function"
	"github.com/Eventual-Inc/Daft/pkg/object"
)

type FunctionRegistry interface {
	RegisterFunction(ctx context.Context, function function.FunctionDefinition) (function.FunctionID, error)
	GetFunctionDefinition(ctx context.Context, fid function.FunctionID) (*function.FunctionDefinition, error)
	InvokeFunction(ctx context.Context, fid function.FunctionID, args ...function.Operand) ([]object.ObjectRef, error)
}

func NewFunctionRegistry() FunctionRegistry {
	return newLocalFunctionRegistry()
}
