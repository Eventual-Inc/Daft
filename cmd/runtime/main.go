package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/containerd/containerd"
	"github.com/containerd/containerd/cio"
	"github.com/containerd/containerd/namespaces"
	"github.com/containerd/containerd/oci"
	"github.com/gorilla/mux"
)

// Code that will launch a busybox container using the host's containerd client
func launchBusybox(id int) {
	// Create a containerd client
	client, err := containerd.New("/run/containerd/containerd.sock")
	defer client.Close()
	if err != nil {
		log.Fatal(err)
	}

	// Pull image
	ctx := namespaces.WithNamespace(context.Background(), "busybox")
	image, err := client.Pull(ctx, "docker.io/library/busybox:1.35.0", containerd.WithPullUnpack)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Successfully pulled %s image\n", image.Name())

	// Create new container
	container, err := client.NewContainer(
		ctx,
		fmt.Sprintf("busybox-%d", id),
		containerd.WithImage(image),
		containerd.WithNewSnapshot(fmt.Sprintf("busybox-%d", id), image),
		containerd.WithNewSpec(
			oci.WithImageConfigArgs(
				image,
				[]string{
					"sleep",
					"10",
				},
			),
		),
	)
	defer container.Delete(ctx, containerd.WithSnapshotCleanup)

	// create a task from the container
	task, err := container.NewTask(ctx, cio.NewCreator(cio.WithStdio))
	if err != nil {
		log.Fatal(err)
	}
	defer task.Delete(ctx)

	// make sure we wait before calling start
	exitStatusC, err := task.Wait(ctx)
	if err != nil {
		fmt.Println(err)
	}

	// call start on the task
	if err := task.Start(ctx); err != nil {
		log.Fatal(err)
	}

	// wait for the process to fully exit and print out the exit status
	status := <-exitStatusC
	code, _, err := status.Result()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("busybox exited with status: %d\n", code)
}

type IDDocument struct {
	ID uint64 `json:"id"`
}

func main() {
	r := mux.NewRouter()
	r.HandleFunc("/launch-busybox", func(w http.ResponseWriter, req *http.Request) {
		decodedReq := new(IDDocument)
		json.NewDecoder(req.Body).Decode(&decodedReq)
		launchBusybox(int(decodedReq.ID))
	}).Methods("POST")

	srv := &http.Server{
		Addr:    ":8080",
		Handler: r,
	}
	srv.ListenAndServe()
}
