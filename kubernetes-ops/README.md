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
helm install cluster-autoscaler cluster-autoscaler -f cluster-autoscaler/values.yaml --set autoDiscovery.clusterName=<CLUSTER_NAME>
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
# NOTE: this is made available as a convenience target in our Makefile as `make deploy-dev-eventual-hub` already
kubectl apply -k kubernetes-ops/eventual-hub/_pre
kubectl apply -k kubernetes-ops/eventual-hub/installs/cluster_dev
```

### Vault

We deploy Vault on Hashicorp Cloud Platform as a managed service that is VPC-peered into our cluster, and integrate with it via the Vault Kubernetes Agent Injector. The injector is part of the `eventual-hub` Kustomize package.

Setting up Vault:

#### Enable AWS Authentication

Now we need to enable the AWS auth method to allow us to authenticate with Vault using AWS IAM entities (see: [AWS Auth Method](https://www.vaultproject.io/docs/auth/aws) and [Create STS Role](https://www.vaultproject.io/api-docs/auth/aws#create-sts-role)).

```
# Enable AWS authentication to allow us to authenticate with Vault via AWS roles
vault auth enable aws

# Enable AWS secrets to allow us to store and assume AWS roles from Vault
vault secrets enable aws

# Configures Vault to use the provided IAMUser's credentials when assuming a role in a `GET aws/roles/*` call
vault write aws/config/root \
    access_key=<IAM USER ACCESS KEY> \
    secret_key=<IAM USER ACCESS SECRET> \
    region=us-west-2

# Configures Vault to use the provided IAMUser's credentials when making API calls to AWS for configuring AWS authentication
vault write /auth/aws/config/client access_key=<IAM USER ACCESS KEY> secret_key=<IAM USER ACCESS SECRET>
```

We create a Vault policy that allows for reading all `aws/sts/userrole-*` secrets - any Vault role that has this policy attached can now *read* these secrets and retrieve credentials for roles stored in these secrets.

```
vault policy write read-all-user-roles - <<EOF
path "aws/sts/userrole-*" {
  capabilities = ["read", "update"]
}
EOF
```

Now we create a Vault role for the cluster and link the aforementioned policy to this Vault role. This Vault role can be authenticated against by entities that have assumed the `jay_sandbox_eks_cluster-vault-bound-role` role.

```
vault write auth/aws/role/daft-service-role \
  auth_type=iam \
  policies=read-all-user-roles \
  bound_iam_principal_arn=arn:aws:iam::941892620273:role/jay_sandbox_eks_cluster-vault-bound-role
```

**Linking User Roles**

Let's say Jay is trying to create an Eventual account. Jay will create a new AWS role in his AWS account, and allow Eventual's Vault role to assume it by adding Eventual's Vault role as a Principal in his role's Trust Policy.

On our end, all we need to do is to write a new Vault AWS secret:

```
vault write aws/roles/userrole-jay@eventualcomputing.com \
  role_arns=<JAYS_ROLE_ARN> \
  credential_type=assumed_role
```

Now any entity that has assumed the `arn:aws:iam::941892620273:role/jay_sandbox_eks-vault-bound-role` role can authenticate to Vault as `daft-service-role` and retrieve credentials for the AWS role stored in the secret at `aws/roles/userrole-jay@eventualcomputing.com`.

**Using Vault from Kubernetes**

To have the Vault injector inject IAM credentials into any pod, add these annotations to the pod spec:

```
# Enable the Vault Agent to inject secrets
vault.hashicorp.com/agent-inject: "true"
# Authenticate as the daft-service-role in Vault, which has a policy that allows it to
# read secrets at aws/sts/userrole-*
vault.hashicorp.com/namespace: admin
vault.hashicorp.com/role: daft-service-role
vault.hashicorp.com/auth-type: aws
vault.hashicorp.com/auth-path: auth/aws
vault.hashicorp.com/auth-config-type: iam
# Only run an init_container and not the sidecar to pre-populate the file
vault.hashicorp.com/agent-pre-populate-only: "true"
# Retrieve the secrets for some user at aws/sts/userrole-<user-hash> and mount it as a file
# in /vault/secrets/aws-credentials in the application container
# The application container can then source from this file to have the proper credentials loaded as
# environment variables.
vault.hashicorp.com/agent-inject-secret-aws-credentials: aws/sts/userrole-jay@eventualcomputing.com
vault.hashicorp.com/agent-inject-template-aws-credentials: |
  {{ with secret "aws/sts/userrole-jay@eventualcomputing.com" "ttl=1h" -}}
  export AWS_ACCESS_KEY_ID="{{ .Data.access_key }}"
  export AWS_SECRET_ACCESS_KEY="{{ .Data.secret_key }}"
  export AWS_SESSION_TOKEN="{{ .Data.security_token }}"
  {{- end }}
```

Make sure also that the pod uses the correct Kubernetes service account `serviceAccountName: vault-role-sa` that is linked to our IAM `<CLUSTER>-vault-bound-role`.
