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

### Vault

We deploy Vault using the default releases provided by Vault.

```
helm install vault vault --values vault/values.yaml
```

This installs a single-instance non-HA installation of Vault in the cluster. We will move this out into a Hashicorp Vault cluster deployment instead in the future.

> If this is the first time Vault is being installed, we need to initialize and unseal it.
> 1. Initialize Vault with: `kubectl exec -ti vault-0 -- vault operator init`
> 2. Unseal Vault with  3 different seal keys: `kubectl exec -ti vault-0 -- vault operator unseal <key_obtained_at_initialization>`

To use a local Vault client:

```
kubectl port-forward vault-0 8200:8200
export VAULT_ADDR=http://127.0.0.1:8200
export VAULT_TOKEN=<vault_root_token_obtained_at_initialization>
```

Now we need to enable the AWS auth method to allow us to authenticate with Vault using AWS IAM entities (see: [AWS Auth Method](https://www.vaultproject.io/docs/auth/aws))

```
vault auth enable aws
```

We also need to enable AWS secrets to allow us to store and assume AWS roles from Vault:

```
vault secrets enable aws
```

We create a Vault policy that allows for reading all `aws/sts/userrole-*` secrets - any Vault role that has this policy attached can now *read* these secrets and retrieve credentials for roles stored in these secrets.

```
vault policy write read-all-user-roles - <<EOF
path "aws/sts/userrole-*" {
  capabilities = ["read", "update"]
}
EOF
```

Now we create a Vault role and link the aforementioned policy to this Vault role. This Vault role can be authenticated against by entities that have assumed the `jay_sandbox_eks_clusterDaftServiceRole` role.

```
vault write auth/aws/role/daft-service-role \
  auth_type=iam \
  policies=read-all-user-roles \
  bound_iam_principal_arn=arn:aws:iam::941892620273:role/jay_sandbox_eks_clusterDaftServiceRole
```

**Linking User Roles**

Let's say Jay is trying to create an Eventual account. Jay will create a new AWS role in his AWS account, and allow Eventual's Vault role to assume it by adding Eventual's Vault role as a Principal in his role's Trust Policy.

On our end, all we need to do is to write a new Vault AWS secret:

```
vault write aws/roles/userrole-jay@eventualcomputing.com \
  role_arns=<JAYS_ROLE_ARN> \
  credential_type=assumed_role
```
