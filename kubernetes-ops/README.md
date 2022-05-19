# Provisioning resources on Kubernetes

We use Terraform to provision our EKS cluster, node pool resources and permissioning. However, for more dynamic provisioning of applications on the cluster itself, we rely on Helm charts and Kubernetes YAML resource definitions.

## Permissioning

For providing our Daft pods with the appropriate permissions, we rely on [IAM roles for service accounts (IRSA)](https://docs.aws.amazon.com/eks/latest/userguide/iam-roles-for-service-accounts.html).

1. For every EKS cluster, we create a `CLUSTER_NAME_DaftRole` AWS IAM Role through terraform and assign this role with the appropriate policies for user data access (e.g. S3 access)
2. In Terraform we also say that certain Kubernetes Service Accounts are allowed to assume this role - see: [application-iams.tf](https://github.com/Eventual-Inc/engine/blob/main/cloud-ops/modules/sandbox_eks_cluster/application-iams.tf#L26-L31)
3. Now in Kubernetes, we create Service Accounts with the appropriate annotation (`eks.amazonaws.com/role-arn: <CLUSTER_NAME_DaftRole.arn>`)
4. Now for every pod that we want to grant Daft application AWS permissions to, simply add `serviceAccountName: ...` to the pod spec

## Helm Releases

### Cluster Autoscaler

By default, our clusters will not autoscale themselves. We need to deploy a Kubernetes autoscaler to the cluster which will monitor for pending pods and trigger scale-up and down of the number of nodes in the cluster.

We use Helm and a [Helm chart provided by Kubernetes](https://github.com/kubernetes/autoscaler/tree/master/charts/cluster-autoscaler) to install the cluster autoscaler. To do so, simply run:

```
helm install cluster-autoscaler cluster-autoscaler
```

Some values in `values.yaml` have been modified for our cluster to ensure that our autoscaler runs on a dedicated On-Demand node pool

1. `clusterName: jay_sandbox_eks_cluster` is hardcoded to our sandbox cluster
2. `nodeSelector: { dedicated: clusterAutoscaler }` adds a node selector which is configured on our nodes Terraform to be On-Demand nodes
3. `tolerations: [{ key: "dedicated", operator: "Equal", value: "clusterAutoscaler", effect: "NoSchedule" }]` adds a toleration for those nodes

### Ray

> As this is a fairly frequent operation, installing Ray helm releases has has been incorporated into the Daft Makefile as a simple `make ray-up/down` command. See the main Daft README for easy instructions for bringing Ray clusters up and down.

Deploying Ray on Kubernetes is not very straightforward and rather manual. There are three main ways:

1. Use the existing Helm charts provided by Ray to deploy an autoscaling Ray cluster (recommended by Ray team)
2. Use a simpler collection of Kubernetes deployments and services to deploy a non-autoscaling Ray cluster
3. Use the KubeRay project (marked experimental by the Ray team)

Here, we go with option 2 as it is the simplest option for now and does not autoscale - we will figure out autoscaling later on.

The Helm chart for deploying option 2 is found in `ray-static-cluster/`, and can be installed with:

```
CLUSTER_NAME=my-cluster

helm install ${CLUSTER_NAME} \
  ray-static-cluster \
  --set clusterName=${CLUSTER_NAME} \
  --namespace ray \
  --create-namespace
```

### JupyterHub

We deploy JupyterHub using an open-source [Helm chart](https://github.com/jupyterhub/helm-chart). This is added as a subchart under our `jupyterhub/` chart.

```
helm upgrade --cleanup-on-fail \
  --install jupyterhub jupyterhub \
  --namespace jupyterhub \
  --values jupyterhub/values.yaml \
  --set jupyterhub.hub.config.Auth0OAuthenticator.client_secret=<Auth0 Client Secret>
```

The Auth0 Client Secret can be retrieved from the [Auth0 application page](https://manage.auth0.com/dashboard/us/dev-kn2voyk3/applications/zwLsZdOmbKRat6i5Ccm7pq8vfNSNZNvR/settings).
