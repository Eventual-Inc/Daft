package function

import (
	"github.com/Eventual-Inc/Daft/pkg/object"
)

type FunctionID string

type FunctionDefinition struct{}

type Operand struct {
	object.ObjectRef
}
