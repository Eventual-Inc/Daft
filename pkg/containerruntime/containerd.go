package containerruntime

import (
	"context"
	"fmt"
	"errors"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/containerd/containerd"
	"github.com/containerd/containerd/cio"
	"github.com/containerd/containerd/oci"
	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/sirupsen/logrus"


	"github.com/Eventual-Inc/Daft/pkg/image"
)

type ContainerdContext struct {
	client *containerd.Client
	numContainers uint64
	imagePool map[string]containerd.Image
}


func NewContainerdContext(socketPath string) *ContainerdContext {
	cdc := new(ContainerdContext)
	client, err := containerd.New("/run/containerd/containerd.sock")
	cdc.client = client
	if err != nil {
		logrus.Fatal(err)
	}
	cdc.imagePool = make(map[string]containerd.Image)
	return cdc
}

func (c *ContainerdContext) PullImage(ctx context.Context, uri string) (string, error) {
	if c.ContainsImage(ctx, uri) {
		return uri, nil
	}
	
	resolver, err := image.ResolverFactory(ctx, uri)
	if err != nil {
		logrus.Fatal(err)
		return "", err
	}
	logrus.Debugf("pulling image: %s", uri)

	image, err := c.client.Pull(
		ctx,
		uri,
		containerd.WithResolver(resolver),
		containerd.WithPullUnpack,
	)
	logrus.Debugf("done pulling image: %s", image.Name())
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
	if !c.ContainsImage(ctx, uri) {
		return "", errors.New("image does not exist in container context")
	} else {
		logrus.Debugf("image in cache for: %s", uri)
	}
	

	c.numContainers = c.numContainers + 100

	container_image, _ := c.imagePool[uri]
	mntSourceDir := fmt.Sprintf("/run/eventual/container-%d", c.numContainers)
	os.MkdirAll(mntSourceDir, os.ModePerm) // should probably set permissions correctly

	containerName := uuid.New().String()
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
	start := time.Now()

	task, err := container.NewTask(ctx, cio.NewCreator(cio.WithStdio))
	exitStatusC, err := task.Wait(ctx)
	
	logrus.Debugf("done building Task for: %s -> %s", container.ID(), task.ID())



	if err != nil {
		logrus.Fatal(err, exitStatusC)
	}
	logrus.Print(task.Pid())


	if err := task.Start(ctx); err != nil {
		logrus.Fatal(err)
	}

	exitStatusC, err = task.Wait(ctx)
	logrus.Debugf("Time to wait for task: %v", time.Since(start))

	logrus.Print("waited")
	// task, err = container.NewTask(ctx, cio.NewCreator(cio.WithStdio))
	// exitStatusC, err = task.Wait(ctx)
	// if err := task.Start(ctx); err != nil {
	// 	logrus.Fatal(err)
	// }
	// exitStatusC, err = task.Wait(ctx)
	// if err := task.Start(ctx); err != nil {
	// 	logrus.Fatal(err)
	// }


	return "", nil


}

func (c *ContainerdContext) Close() {
	c.client.Close()
}