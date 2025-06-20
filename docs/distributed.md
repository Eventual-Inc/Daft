# Distributed Computing

By default, Daft runs using your local machine's resources and your operations are thus limited by the CPUs, memory and GPUs available to you in your single local development machine. Daft's native support for [Ray](https://docs.ray.io/en/latest/ray-overview/index.html), an open-source framework for distributed computing, enables you to run distributed DataFrame workloads at scale across a cluster of machines.

## Setting Up Ray with Daft

You can run Daft on Ray in multiple ways:

### Simple Local Setup

If you want to start a single node ray cluster on your local machine, you can do the following:

```bash
pip install ray[default]
ray start --head
```

This should output something like:

```
Usage stats collection is enabled. To disable this, add `--disable-usage-stats` to the command that starts the cluster, or run the following command: `ray disable-usage-stats` before starting the cluster. See https://docs.ray.io/en/master/cluster/usage-stats.html for more details.

Local node IP: 127.0.0.1

--------------------
Ray runtime started.
--------------------

...
```

You can take the IP address and port and pass it to Daft with [`set_runner_ray`][daft.context.set_runner_ray]:

```python
>>> import daft
>>> daft.context.set_runner_ray("ray://127.0.0.1:10001")
DaftContext(_daft_execution_config=<daft.daft.PyDaftExecutionConfig object at 0x100fbd1f0>, _daft_planning_config=<daft.daft.PyDaftPlanningConfig object at 0x100fbd270>, _runner_config=_RayRunnerConfig(address='127.0.0.1:10001', max_task_backlog=None), _disallow_set_runner=True, _runner=None)

>>> df = daft.from_pydict({
...   'text': ['hello', 'world']
... })
2024-07-29 15:49:26,610 INFO worker.py:1567 -- Connecting to existing Ray cluster at address: 127.0.0.1:10001...
2024-07-29 15:49:26,622 INFO worker.py:1752 -- Connected to Ray cluster.

>>> print(df)
╭───────╮
│ text  │
│ ---   │
│ Utf8  │
╞═══════╡
│ hello │
├╌╌╌╌╌╌╌┤
│ world │
╰───────╯

(Showing first 2 of 2 rows)
```

By default, if no address is specified, Daft will spin up a Ray cluster locally on your machine. If you are running Daft on a powerful machine (such as an AWS P3 machine which is equipped with multiple GPUs) this is already very useful because Daft can parallelize its execution of computation across your CPUs and GPUs.

### Connecting to Remote Ray Clusters

If you already have your own Ray cluster running remotely, you can connect Daft to it by supplying an address with [`set_runner_ray`][daft.context.set_runner_ray]:

=== "🐍 Python"

    ```python
    daft.context.set_runner_ray(address="ray://url-to-mycluster")
    ```

For more information about the `address` keyword argument, please see the [Ray documentation on initialization](https://docs.ray.io/en/latest/ray-core/api/doc/ray.init.html).

### Using Ray Client

The Ray client is a quick way to get started with running tasks and retrieving their results on Ray using Python.

!!! warning "Warning"

    To run tasks using the Ray client, the version of Daft and the minor version (eg. 3.9, 3.10) of Python must match between client and server.

```python
import daft
import ray

# Refer to the note under "Ray Job" for details on "runtime_env"
ray.init("ray://<head_node_host>:10001", runtime_env={"pip": ["daft"]})

# Starts the Ray client and tells Daft to use Ray to execute queries
# If ray.init() has already been called, it uses the existing client
daft.context.set_runner_ray("ray://<head_node_host>:10001")

df = daft.from_pydict({
    "a": [3, 2, 5, 6, 1, 4],
    "b": [True, False, False, True, True, False]
})
df = df.where(df["b"]).sort(df["a"])

# Daft executes the query remotely and returns a preview to the client
df.collect()
```

```{title="Output"}
╭───────┬─────────╮
│ a     ┆ b       │
│ ---   ┆ ---     │
│ Int64 ┆ Boolean │
╞═══════╪═════════╡
│ 1     ┆ true    │
├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
│ 3     ┆ true    │
├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
│ 6     ┆ true    │
╰───────┴─────────╯

(Showing first 3 of 3 rows)
```

### Using Ray Jobs

Ray jobs allow for more control and observability over using the Ray client. In addition, your entire code runs on Ray, which means it is not constrained by the compute, network, library versions, or availability of your local machine.

```python
# wd/job.py

import daft

def main():
    # call without any arguments to connect to Ray from the head node
    daft.context.set_runner_ray()

    # ... Run Daft commands here ...

if __name__ == "__main__":
    main()
```

To submit this script as a job, use the Ray CLI, which can be installed with `pip install "ray[default]"`.

```bash
ray job submit \
    --working-dir wd \
    --address "http://<head_node_host>:8265" \
    --runtime-env-json '{"pip": ["daft"]}' \
    -- python job.py
```

!!! note "Note"

    The runtime env parameter specifies that Daft should be installed on the Ray workers. Alternative methods of including Daft in the worker dependencies can be found [here](https://docs.ray.io/en/latest/ray-core/handling-dependencies.html).

For more information about Ray jobs, see [Ray docs -> Ray Jobs Overview](https://docs.ray.io/en/latest/cluster/running-applications/job-submission/index.html).

## Daft CLI Overview

Daft CLI is a convenient command-line tool that simplifies running Daft in distributed environments. It provides two modes of operation to suit different needs:

1. **Provisioned Mode**: Automatically provisions and manages Ray clusters in AWS. This is perfect for teams who want a turnkey solution with minimal setup.

2. **BYOC (Bring Your Own Cluster) Mode**: Connects to existing Kubernetes clusters and handles Ray/Daft setup for you. This is ideal for organizations with existing infrastructure or specific compliance requirements.

### When to Choose Each Mode

| Choose **Provisioned Mode** if you: | Choose **BYOC Mode** if you: |
| ------------------------------------| -----------------------------|
| • Want a fully managed solution with minimal setup | • Have existing Kubernetes infrastructure |
| • Are using AWS (GCP and Azure support coming soon) | • Need multi-cloud support |
| • Need quick deployment without existing infrastructure | • Have specific security or compliance requirements |
| | • Want to use local development clusters |
| | • Want more control over your cluster configuration |

## Prerequisites

The following should be installed on your machine:

A python package manager. We recommend using `uv` to manage everything (i.e., dependencies, as well as the python version itself)

Additional mode-specific requirements:

| **For Provisioned Mode:** | **For BYOC Mode:** |
| ------------------------- | ------------------ |
| • The [AWS CLI](https://aws.amazon.com/cli) tool | • Running Kubernetes cluster (local, cloud-managed, or on-premise) |
| • AWS account with appropriate IAM permissions | • `kubectl` configured with correct context |
| • SSH key pair for cluster access | • Appropriate namespace permissions |

## Installation

Run the following commands in your terminal to initialize your project:

```bash
# Create a project directory
mkdir my-project
cd my-project

# Initialize the project
uv init --python 3.12
uv venv
source .venv/bin/activate

# Install Daft CLI
uv pip install "daft-cli"
```

In your virtual environment, you should have Daft CLI installed — you can verify this by running `daft --version`.

## Mode-Specific Setup

### Provisioned Mode Setup

1. Configure AWS credentials:
```bash
# Configure your SSO
aws configure sso

# Login to your SSO
aws sso login
```

2. Generate and configure SSH keys:
```bash
# Generate key pair
ssh-keygen -t rsa -b 2048 -f ~/.ssh/daft-key

# Import to AWS
aws ec2 import-key-pair \
  --key-name "daft-key" \
  --public-key-material fileb://~/.ssh/daft-key.pub

# Set permissions
chmod 600 ~/.ssh/daft-key
```

### BYOC Mode Setup

Ensure your Kubernetes context is properly configured:
```bash
# Verify your kubernetes connection
kubectl cluster-info

# Set the correct context if needed
kubectl config use-context my-context
```

## Configuration

Initialize a configuration file based on your chosen mode:

```bash
# For Provisioned Mode
daft config init --provider provisioned

# For BYOC Mode
daft config init --provider byoc
```

### Example Configurations

**Provisioned Mode (.daft.toml)**:
```toml
[setup]
name = "my-daft-cluster"
python-version = "3.11"
ray-version = "2.40.0"
provider = "provisioned"

[setup.provisioned]
region = "us-west-2"
number-of-workers = 4
ssh-user = "ubuntu"
ssh-private-key = "~/.ssh/daft-key"
instance-type = "i3.2xlarge"
image-id = "ami-04dd23e62ed049936"

[[job]]
name = "example-job"
command = "python my_script.py"
working-dir = "~/my_project"
```

**BYOC Mode (.daft.toml)**:
```toml
[setup]
name = "my-daft-cluster"
python-version = "3.11"
ray-version = "2.40.0"
provider = "byoc"

[setup.byoc]
namespace = "default"

[[job]]
name = "example-job"
command = "python my_script.py"
working-dir = "~/my_project"
```

## Cluster Operations

### Provisioned Mode

```bash
# Spin up a cluster
daft provisioned up

# List clusters and their status
daft provisioned list

# Connect to Ray dashboard
daft provisioned connect

# SSH into head node
daft provisioned ssh

# Gracefully shutdown cluster
daft provisioned down

# Force terminate cluster
daft provisioned kill
```

### BYOC Mode

```bash
# Initialize Ray/Daft on your cluster
daft byoc init

# Connect to your cluster
daft byoc connect

# Clean up Ray/Daft resources
daft byoc cleanup
```

## Job Management

Jobs can be submitted and managed similarly in both modes:

```bash
# Submit a job
daft job submit example-job

# Check job status (provisioned mode only)
daft job status example-job

# View job logs (provisioned mode only)
daft job logs example-job
```

#### Example Daft Script

```python
import daft

# Ray context is automatically set by Daft CLI
df = daft.from_pydict({"nums": [1,2,3]})
df.agg(daft.col("nums").mean()).show()
```

## SQL Query Support

Daft supports running SQL queries against your data using the postgres dialect:

```bash
# Run a SQL query
daft sql -- "\"SELECT * FROM my_table\""
```

## Ray Dashboard Access

The Ray dashboard provides insights into your cluster's performance and job status:

```bash
# For Provisioned Mode
daft provisioned connect

# For BYOC Mode
daft byoc connect
```

!!! note "Note"
    For Provisioned Mode, you'll need your SSH key to access the dashboard. BYOC Mode uses your Kubernetes credentials.

### Monitoring Cluster State

For Provisioned Mode, `daft provisioned list` shows cluster status:
```
Running:
  - daft-demo, head, i-053f9d4856d92ea3d, 35.94.91.91
  - daft-demo, worker, i-00c340dc39d54772d, 44.234.112.173
  - daft-demo, worker, i-042a96ce1413c1dd6, 35.94.206.130
```

For BYOC Mode, use standard Kubernetes tools:
```bash
kubectl get pods -n your-namespace
```
