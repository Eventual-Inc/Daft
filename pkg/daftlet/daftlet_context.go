package daftlet

import (
	cr "github.com/Eventual-Inc/Daft/pkg/container_runtime"
	"github.com/Eventual-Inc/Daft/pkg/function/registry"
)

type DaftletContext struct {
	func_registry registry.FunctionRegistry
	c_runtime     cr.ContainerRuntimeContext
}

func NewDaftletContext() *DaftletContext {
	dc := new(DaftletContext)
	dc.func_registry = registry.NewFunctionRegistry()
	dc.c_runtime = cr.NewContainerRuntimeContext("/run/containerd/containerd.sock", "/run/eventual/")
	return dc
}
