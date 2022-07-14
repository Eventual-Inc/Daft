# Developing on Daft

1. [Install Poetry](https://python-poetry.org/docs/#installation)
2. Init your python environment
    - `poetry install`
3. Run tests
    - `poetry run pytest`
4. Run type checking
    - `poetry run mypy`
5. Run any other script
    - `poetry run CMD`
6. Add package
    - `poetry add PACKAGE`
7. Lock env
    - `poetry lock`

## Launching Daft on a Kubernetes cluster

1. Install [`kubectl`](https://kubernetes.io/docs/tasks/tools/#kubectl) and [`helm`](https://helm.sh/docs/intro/install/#through-package-managers)
2. Setup your `kubectl` client to use the `jay_sandbox_eks_cluster` cluster:
```
aws eks update-kubeconfig --region us-west-2 --name jay_sandbox_eks_cluster
```
3. Spin up Daft deployment with `make ray-up`
4. Spin down Daft deployment: `make ray-down`

If you want your own sandbox Kubernetes cluster instead, create one by [copying and applying this terraform module](https://github.com/Eventual-Inc/engine/blob/main/cloud-ops/main.tf#L131-L134).

### Using Kubernetes

Here is a useful cheat sheet for debugging in Kubernetes using `kubectl`.

1. Get all running pods in `ray` namespace: `kubectl get pods -n ray`
2. Get all services in `ray` namespace: `kubectl get services -n ray`
3. Port-forward to Ray head node: `kubectl -n ray port-forward service/<CLUSTER_NAME>-ray-head-svc 10001:10001`
4. Port-forward to Ray dashboard UI: `kubectl -n ray port-forward service/<CLUSTER_NAME>-ray-head-svc 8265:8265`
5. Check logs for a pod: `kubectl logs -n ray <pod_name>`
