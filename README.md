# DaFt
Data Functions (DaFt)

## Build
- Build all images `make`
- Build runtime image `make runtime`
- Build reader image `make reader`

## Code Generation
- Generate flatbuffer Golang code  `make gen-fbs`

## Local Development
- start tilt + kind cluster: `make local-dev`
- kill local cluster: `make stop-local-cluster`

## Develop on Kubernetes
- Tag your Docker images: `docker tag runtime:latest 941892620273.dkr.ecr.us-west-2.amazonaws.com/daft/runtime:<YOUR_LABEL>`
- Push your Docker images to the remote registry: `docker push 941892620273.dkr.ecr.us-west-2.amazonaws.com/daft/runtime:<YOUR_LABEL>`
- Update the pod YAML config image in `k8s/runtime.yaml` to your new image
- Run your runtime in Kubernetes with `kubectl apply -f k8s/runtime.yaml`
- Now, you can make HTTP requests to your pod (get its IP address with `kubectl get pods -o wide`) from any other pod in the cluster
- You can create a "debugger" pod with `kubectl apply -f k8s/debugger.yaml` which you can then shell into with `kubectl exec -it debugger /bin/bash`, and make HTTP requests to your runtime pod with `curl`.
