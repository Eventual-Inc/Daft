# Provisioning resources on Kubernetes

We use Terraform to provision our EKS cluster, node pool resources and permissioning. However, for more dynamic provisioning of applications on the cluster itself, we rely on Helm charts and Kubernetes YAML resource definitions.

## Cluster Autoscaler

By default, our clusters will not autoscale themselves. We need to deploy a Kubernetes autoscaler to the cluster which will monitor for pending pods and trigger scale-up and down of the number of nodes in the cluster.

We use Helm and a [Helm chart provided by Kubernetes](https://github.com/kubernetes/autoscaler/tree/master/charts/cluster-autoscaler) to install the cluster autoscaler. To do so, simply run:

```
helm install cluster-autoscaler cluster-autoscaler
```

Some values in `values.yaml` have been modified for our cluster to ensure that our autoscaler runs on a dedicated On-Demand node pool

1. `clusterName: jay_sandbox_eks_cluster` is hardcoded to our sandbox cluster
2. `nodeSelector: { dedicated: clusterAutoscaler }` adds a node selector which is configured on our nodes Terraform to be On-Demand nodes
3. `tolerations: [{ key: "dedicated", operator: "Equal", value: "clusterAutoscaler", effect: "NoSchedule" }]` adds a toleration for those nodes

## Ray

Deploying Ray on Kubernetes is not very straightforward and rather manual. There are three main ways:

1. Use the existing Helm charts provided by Ray to deploy an autoscaling Ray cluster (recommended by Ray team)
2. Use a simpler collection of Kubernetes deployments and services to deploy a non-autoscaling Ray cluster
3. Use the KubeRay project (marked experimental by the Ray team)

Here, we go with option 2 as it is the simplest option for now and does not autoscale - we will figure out autoscaling later on.

See: main Daft README for instructions on how to deploy a Ray cluster for Daft.
