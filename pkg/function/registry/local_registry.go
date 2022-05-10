package registry

import (
	"context"
	"errors"

	"github.com/google/uuid"

	"github.com/Eventual-Inc/Daft/pkg/function"
	"github.com/Eventual-Inc/Daft/pkg/object"
)

type LocalFunctionRegistry struct {
	functionDefMap map[function.FunctionID]function.FunctionDefinition
}

func newLocalFunctionRegistry() *LocalFunctionRegistry {
	lfr := new(LocalFunctionRegistry)
	lfr.functionDefMap = make(map[function.FunctionID]function.FunctionDefinition)
	return lfr
}

func (fr *LocalFunctionRegistry) RegisterFunction(ctx context.Context, functionDef function.FunctionDefinition) (function.FunctionID, error) {
	fid := uuid.New().String()
	fr.functionDefMap[fid] = functionDef
	return fid, nil
}

func (fr *LocalFunctionRegistry) GetFunctionDefinition(ctx context.Context, fid function.FunctionID) (*function.FunctionDefinition, error) {
	funcDef, ok := fr.functionDefMap[fid]
	if ok {
		return &funcDef, nil
	}
	return nil, errors.New("Function ID not found")

}

func (fr *LocalFunctionRegistry) InvokeFunction(ctx context.Context, fid function.FunctionID, args ...function.Operand) ([]object.ObjectRef, error) {
	return make([]object.ObjectRef, 0), nil
}
