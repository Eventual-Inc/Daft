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
	PullImage(ctx context.Context, uri string) (string, error)
	EvictImage(ctx context.Context, uri string) (string, error)
	ContainsImage(ctx context.Context, uri string) bool
	CreateContainer(ctx context.Context, uri string) (string, error)
	StartContainer(ctx context.Context, containerName string) (string, error)
	StopContainer(ctx context.Context, containerName string) (string, error)
	DeleteContainer(ctx context.Context, containerName string) (string, error)
	Close()
	// containerStatus(string) ContainerStatus
}

func NewContainerRuntimeContext(socketPath string, hostPathPrefix string) ContainerRuntimeContext {
	return NewContainerdContext(socketPath, hostPathPrefix)
}
