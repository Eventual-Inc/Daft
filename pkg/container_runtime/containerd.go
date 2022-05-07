package container_runtime

import (
	"context"
	"errors"
	"fmt"
	"os"
	"syscall"

	"github.com/containerd/containerd"
	"github.com/containerd/containerd/cio"
	"github.com/containerd/containerd/oci"
	"github.com/google/uuid"
	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/sirupsen/logrus"

	"github.com/Eventual-Inc/Daft/pkg/image"
	"github.com/Eventual-Inc/Daft/pkg/logging/timing"
)

type containerData struct {
	containerId string
	imageName   string
	container   containerd.Container
	task        containerd.Task
}

type ContainerdContext struct {
	client         *containerd.Client
	hostPathPrefix string
	imagePool      map[ContainerImageURI]containerd.Image
	containerPool  map[string]containerData
}

func NewContainerdContext(socketPath string, hostPathPrefix string) *ContainerdContext {
	defer timing.Timeit("NewContainerdContext", socketPath)()

	cdc := new(ContainerdContext)
	client, err := containerd.New("/run/containerd/containerd.sock")
	cdc.client = client
	if err != nil {
		logrus.Fatal(err)
	}
	cdc.hostPathPrefix = hostPathPrefix
	cdc.imagePool = make(map[ContainerImageURI]containerd.Image)
	cdc.containerPool = make(map[string]containerData)
	return cdc
}

func (c *ContainerdContext) PullImage(ctx context.Context, uri ContainerImageURI) (bool, error) {
	defer timing.Timeit("PullImage", uri)()
	if c.ContainsImage(ctx, uri) {
		return true, nil
	}

	resolver, err := image.ResolverFactory(ctx, uri)
	if err != nil {
		logrus.Error(err)
		return false, err
	}

	image, err := c.client.Pull(
		ctx,
		uri,
		containerd.WithResolver(resolver),
		containerd.WithPullUnpack,
	)
	unpacked, err := image.IsUnpacked(ctx, "overlayfs")

	if !unpacked {
		err = image.Unpack(ctx, "overlayfs")
		if err != nil {
			logrus.Error(err)
			return false, err
		}
	}

	c.imagePool[uri] = image

	return true, err
}

func (c *ContainerdContext) EvictImage(ctx context.Context, uri ContainerImageURI) (bool, error) {
	if !c.ContainsImage(ctx, uri) {
		return false, nil
	}

	delete(c.imagePool, uri)
	return true, nil
}

func (c *ContainerdContext) ContainsImage(ctx context.Context, uri ContainerImageURI) bool {
	_, ok := c.imagePool[uri]
	return ok
}

func (c *ContainerdContext) CreateContainer(ctx context.Context, uri ContainerImageURI) (ContainerID, error) {
	containerId := uuid.New().String()

	defer timing.Timeit("CreateContainer", containerId)()
	if !c.ContainsImage(ctx, uri) {
		return "", errors.New("image does not exist in container context")
	} else {
		logrus.Debugf("image in cache for: %s", uri)
	}

	container_image, _ := c.imagePool[uri]
	mntSourceDir := fmt.Sprintf("%s-%s", c.hostPathPrefix, containerId)
	os.MkdirAll(mntSourceDir, os.ModePerm) // should probably set permissions correctly

	logrus.Debugf("building container for: %s with name: %s", uri, containerId)

	container, err := c.client.NewContainer(
		ctx,
		containerId,
		containerd.WithImage(container_image),
		containerd.WithNewSnapshot(fmt.Sprintf("%s-snapshot", containerId), container_image),
		containerd.WithNewSpec(
			oci.WithImageConfig(container_image),
			oci.WithMounts([]specs.Mount{
				{
					Destination: "/run/eventual",
					Type:        "bind",
					Source:      mntSourceDir,
					Options:     []string{"rbind"},
				},
			}),
		),
	)

	if err != nil {
		logrus.Error(err)
		return "", err
	}

	logrus.Debugf("done building container for: %s -> %s", uri, container.ID())

	logrus.Debugf("building task for: %s", container.ID())

	task, err := container.NewTask(ctx, cio.NewCreator(cio.WithStdio))

	if err != nil {
		logrus.Error(err)
		return "", err
	}

	logrus.Debugf("created task %s as pid: %d", task.ID(), task.Pid())

	c.containerPool[containerId] = containerData{
		containerId: containerId,
		imageName:   uri,
		container:   container,
		task:        task,
	}

	return containerId, nil

}

func (c *ContainerdContext) StartContainer(ctx context.Context, containerId ContainerID) (bool, error) {
	defer timing.Timeit("StartContainer", containerId)()
	cd, ok := c.containerPool[containerId]
	if !ok {
		return false, errors.New("container does not exist in container context")
	}

	if err := cd.task.Start(ctx); err != nil {
		logrus.Error(err)
		return false, err
	}

	return true, nil
}

func (c *ContainerdContext) StopContainer(ctx context.Context, containerId ContainerID) (bool, error) {
	defer timing.Timeit("StopContainer", containerId)()
	cd, ok := c.containerPool[containerId]
	if !ok {
		return false, errors.New("container does not exist in container context")
	}

	if err := cd.task.Kill(ctx, syscall.SIGTERM); err != nil {
		logrus.Error(err)
		return false, err
	}
	status, err := cd.task.Wait(ctx)

	if err != nil {
		logrus.Error(err)
		return false, err
	}
	<-status
	return true, nil
}

func (c *ContainerdContext) DeleteContainer(ctx context.Context, containerId ContainerID) (bool, error) {
	defer timing.Timeit("DeleteContainer", containerId)()
	cd, ok := c.containerPool[containerId]
	if !ok {
		return false, errors.New("container does not exist in container context")
	}

	if _, err := cd.task.Delete(ctx); err != nil {
		logrus.Fatal(err)
		return false, err
	}

	if err := cd.container.Delete(ctx, containerd.WithSnapshotCleanup); err != nil {
		logrus.Fatal(err)
		return false, err
	}
	delete(c.containerPool, containerId)
	return true, nil
}

func (c *ContainerdContext) Close() {
	c.client.Close()
}
