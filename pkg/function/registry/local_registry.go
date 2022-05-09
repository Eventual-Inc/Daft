package registry

import (
	"context"

	"github.com/google/uuid"

	"github.com/Eventual-Inc/Daft/pkg/function"
	"github.com/Eventual-Inc/Daft/pkg/object"
)

type LocalFunctionRegistry struct {
	functionDefMap map[function.FunctionID]function.FunctionDefinition
}

func (fr *LocalFunctionRegistry) RegisterFunction(ctx context.Context, functionDef function.FunctionDefinition) function.FunctionID {
	fid := uuid.New().String()
	fr.functionDefMap[fid] = functionDef
	return fid
}

func (fr *LocalFunctionRegistry) InvokeFunction(ctx context.Context, fid function.FunctionID, args ...function.Operand) []object.ObjectRef {
	return make([]object.ObjectRef, 0)
}
