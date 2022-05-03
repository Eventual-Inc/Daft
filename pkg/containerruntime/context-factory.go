package containerruntime

import (
	"context"
)

type ContainerStatus uint

const (
	pending = iota
	running
	completed
	dead
)

type ContainerRuntimeContext interface {
	PullImage(context.Context, string) (string, error)
	ContainsImage(ctx context.Context, uri string) bool
	CreateContainer(ctx context.Context, uri string) (string, error)
	Close()
	// containerStatus(string) ContainerStatus
}


func NewContainerRuntimeContext() ContainerRuntimeContext {
	return NewContainerdContext("test")
}