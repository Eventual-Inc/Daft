package container_runtime

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

type ContainerImageURI = string

type ContainerID = string

type ContainerRuntimeContext interface {
	PullImage(ctx context.Context, uri ContainerImageURI) (bool, error)
	EvictImage(ctx context.Context, uri ContainerImageURI) (bool, error)
	ContainsImage(ctx context.Context, uri ContainerImageURI) bool
	CreateContainer(ctx context.Context, uri ContainerImageURI) (ContainerID, error)
	StartContainer(ctx context.Context, containerID ContainerID) (bool, error)
	StopContainer(ctx context.Context, containerID ContainerID) (bool, error)
	DeleteContainer(ctx context.Context, containerID ContainerID) (bool, error)
	Close()
	// containerStatus(string) ContainerStatus
}

func NewContainerRuntimeContext(socketPath string, hostPathPrefix string) ContainerRuntimeContext {
	return newContainerdContext(socketPath, hostPathPrefix)
}
