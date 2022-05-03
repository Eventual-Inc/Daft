package containerruntime

import (
	"context"
	"errors"
	"fmt"
	"os"

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
	containerName string
	imageName     string
	container     containerd.Container
	task          containerd.Task
}

type ContainerdContext struct {
	client        *containerd.Client
	numContainers uint64
	imagePool     map[string]containerd.Image
	containerPool map[string]containerData
}

func NewContainerdContext(socketPath string) *ContainerdContext {
	defer timing.Timeit("NewContainerdContext", socketPath)()

	cdc := new(ContainerdContext)
	client, err := containerd.New("/run/containerd/containerd.sock")
	cdc.client = client
	if err != nil {
		logrus.Fatal(err)
	}
	cdc.imagePool = make(map[string]containerd.Image)
	cdc.containerPool = make(map[string]containerData)
	return cdc
}

func (c *ContainerdContext) PullImage(ctx context.Context, uri string) (string, error) {
	defer timing.Timeit("PullImage", uri)()
	if c.ContainsImage(ctx, uri) {
		return uri, nil
	}

	resolver, err := image.ResolverFactory(ctx, uri)
	if err != nil {
		logrus.Fatal(err)
		return "", err
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
			logrus.Fatal(err)
			return "", err
		}
	}

	c.imagePool[uri] = image

	return uri, err
}

func (c *ContainerdContext) ContainsImage(ctx context.Context, uri string) bool {
	_, ok := c.imagePool[uri]
	return ok
}

func (c *ContainerdContext) CreateContainer(ctx context.Context, uri string) (string, error) {
	containerName := uuid.New().String()

	defer timing.Timeit("CreateContainer", containerName)()
	if !c.ContainsImage(ctx, uri) {
		return "", errors.New("image does not exist in container context")
	} else {
		logrus.Debugf("image in cache for: %s", uri)
	}

	c.numContainers = c.numContainers + 100

	container_image, _ := c.imagePool[uri]
	mntSourceDir := fmt.Sprintf("/run/eventual/container-%d", c.numContainers)
	os.MkdirAll(mntSourceDir, os.ModePerm) // should probably set permissions correctly

	logrus.Debugf("building container for: %s with name: %s", uri, containerName)

	container, err := c.client.NewContainer(
		ctx,
		containerName,
		containerd.WithImage(container_image),
		containerd.WithNewSnapshot(fmt.Sprintf("%s-snapshot", containerName), container_image),
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
		logrus.Fatal(err)
		return "", err
	}

	logrus.Debugf("done building container for: %s -> %s", uri, container.ID())

	logrus.Debugf("building task for: %s", container.ID())

	task, err := container.NewTask(ctx, cio.NewCreator(cio.WithStdio))

	if err != nil {
		logrus.Fatal(err)
	}

	logrus.Debugf("started task %s as pid: %d", task.ID(), task.Pid())

	if err := task.Start(ctx); err != nil {
		logrus.Fatal(err)
	}

	c.containerPool[containerName] = containerData{
		containerName: containerName,
		imageName:     uri,
		container:     container,
		task:          task,
	}

	return uri, nil

}

func (c *ContainerdContext) Close() {
	c.client.Close()
}
