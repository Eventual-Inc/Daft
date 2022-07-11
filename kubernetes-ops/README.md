# Provisioning resources on Kubernetes

We use Terraform to provision our EKS cluster, node pool resources and permissioning. However, for more dynamic provisioning of applications on the cluster itself, we rely on the Helm and Kustomize configurations we store here in `kubernetes-ops`.

## Kubernetes Permissioning

For providing our Daft pods with the appropriate permissions, we rely on [IAM roles for service accounts (IRSA)](https://docs.aws.amazon.com/eks/latest/userguide/iam-roles-for-service-accounts.html).

1. For every EKS cluster, we create a `CLUSTER_NAME_DaftRole` AWS IAM Role through terraform and assign this role with the appropriate policies for user data access (e.g. S3 access)
2. In Terraform we also say that certain Kubernetes Service Accounts are allowed to assume this role - see: [application-iams.tf](https://github.com/Eventual-Inc/engine/blob/main/cloud-ops/modules/sandbox_eks_cluster/application-iams.tf#L26-L31)
3. Now in Kubernetes, we create Service Accounts with the appropriate annotation (`eks.amazonaws.com/role-arn: <CLUSTER_NAME_DaftRole.arn>`)
4. Now for every pod that we want to grant Daft application AWS permissions to, simply add `serviceAccountName: ...` to the pod spec

## Deploying applications on the cluster

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

### Eventual Hub

The Eventual hub refers to our deployment of a suite of services that constitute the Eventual platform:

1. Eventual frontend React application
2. Eventual web backend
3. Jupyterhub
4. Users' notebook servers
5. Traefik proxy as an ingress to the backend/notebook servers

All configurations are stored in `./eventual-hub` as Kustomize templates. This lets us perform templating to have the same configurations be re-used across environments such as our local development, development cluster and production cluster.

1. `./eventual-hub/_pre`: configs that need to be installed first (e.g. CustomResourceDefinitions)
2. `./eventual-hub/installs/local_tilt_dev`: configs installed by Tilt when developing locally
3. `./eventual-hub/installs/cluster_dev`: configs installed in our Dev cluster

To install the Eventual hub in a cluster, run the following:

```
# NOTE: this is made available as a convenience target in our Makefile as `make deploy-eventual-hub` already
kubectl apply -k kubernetes-ops/eventual-hub/_pre
kubectl apply -k kubernetes-ops/eventual-hub/installs/cluster_dev
```

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
