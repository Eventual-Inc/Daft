package function

import (
	"github.com/Eventual-Inc/Daft/pkg/container_runtime"
	"github.com/Eventual-Inc/Daft/pkg/object"
)

type FunctionID = string

type FunctionDefinition struct {
	image container_runtime.ContainerImageURI
	args  []string
}

type Operand struct {
	object.ObjectRef
}
