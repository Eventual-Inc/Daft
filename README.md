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
2. Setup your `kubectl` client to administrate our sandbox Kubernetes cluster: `aws eks update-kubeconfig --region us-west-2 --name jay_sandbox_eks_cluster`.
    - If you want your own sandbox Kubernetes cluster instead, feel free to create one by [copying this terraform module and applying your changes](https://github.com/Eventual-Inc/engine/blob/main/cloud-ops/main.tf#L131-L134).
3. Now to spin up a new deployment of Daft, cd to `github.com/Eventual-Inc/engine/kubernetes-ops` and run:

```
CLUSTER_NAME=MY_CLUSTER
helm install $CLUSTER_NAME ray-static-cluster \
  --set clusterName=$CLUSTER_NAME \
  --namespace ray
```

4. When you do not need your Daft deployment anymore, spin it back down with `helm uninstall -n ray $CLUSTER_NAME`.

### Using Kubernetes

Here is a useful cheat sheet for debugging in Kubernetes using `kubectl`.

1. `kubectl get pods -n ray` - get all running pods in the `ray` namespace
2. `kubectl get services -n ray` - get all services in the `ray` namespace; we use services to expose the head node of each ray cluster
3. `kubectl -n ray port-forward service/<CLUSTER_NAME>-ray-head-svc 10001:10001` - this forwards port 10001 on the kubernetes service to localhost which enables you to connect to the ray cluster via a Ray client
4. `kubectl logs -n ray <pod_name>` - check logs for a given pod
5. `kubectl -n ray port-forward service/<CLUSTER_NAME>-ray-head-svc 8265:8265` - this forwards port 8265 on the kubernetes service to localhost which is the Ray UI dashboard for your cluster
