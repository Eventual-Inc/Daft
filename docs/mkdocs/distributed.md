# Distributed Computing

<!-- todo(docs - jay): need to add jessie's k8s instructions -->

By default, Daft runs using your local machine's resources and your operations are thus limited by the CPUs, memory and GPUs available to you in your single local development machine.

However, Daft has strong integrations with [Ray](https://www.ray.io) which is a distributed computing framework for distributing computations across a cluster of machines. Here is a snippet showing how you can connect Daft to a Ray cluster:

=== "🐍 Python"

    ```python
    import daft

    daft.context.set_runner_ray()
    ```

By default, if no address is specified Daft will spin up a Ray cluster locally on your machine. If you are running Daft on a powerful machine (such as an AWS P3 machine which is equipped with multiple GPUs) this is already very useful because Daft can parallelize its execution of computation across your CPUs and GPUs. However, if instead you already have your own Ray cluster running remotely, you can connect Daft to it by supplying an address:

=== "🐍 Python"

    ```python
    daft.context.set_runner_ray(address="ray://url-to-mycluster")
    ```

For more information about the `address` keyword argument, please see the [Ray documentation on initialization](https://docs.ray.io/en/latest/ray-core/api/doc/ray.init.html).


If you want to start a single node ray cluster on your local machine, you can do the following:

```bash
> pip install ray[default]
> ray start --head --port=6379
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

You can take the IP address and port and pass it to Daft:

=== "🐍 Python"

    ```python
    >>> import daft
    >>> daft.context.set_runner_ray("127.0.0.1:6379")
    DaftContext(_daft_execution_config=<daft.daft.PyDaftExecutionConfig object at 0x100fbd1f0>, _daft_planning_config=<daft.daft.PyDaftPlanningConfig object at 0x100fbd270>, _runner_config=_RayRunnerConfig(address='127.0.0.1:6379', max_task_backlog=None), _disallow_set_runner=True, _runner=None)
    >>> df = daft.from_pydict({
    ...   'text': ['hello', 'world']
    ... })
    2024-07-29 15:49:26,610	INFO worker.py:1567 -- Connecting to existing Ray cluster at address: 127.0.0.1:6379...
    2024-07-29 15:49:26,622	INFO worker.py:1752 -- Connected to Ray cluster.
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

## Daft Launcher

Daft Launcher is a convenient command-line tool that provides simple abstractions over Ray, enabling a quick uptime for users to leverage Daft for distributed computations. Rather than worrying about the complexities of managing Ray, users can simply run a few CLI commands to spin up a cluster, submit a job, observe the status of jobs and clusters, and spin down a cluster.

### Prerequisites

The following should be installed on your machine:

- The [AWS CLI](https://aws.amazon.com/cli) tool. (Assuming you're using AWS as your cloud provider)

- A python package manager. We recommend using `uv` to manage everything (i.e., dependencies, as well as the python version itself). It's much cleaner and faster than `pip`.

### Install Daft Launcher

Run the following commands in your terminal to initialize your project:

```bash
# Create a project directory
cd some/working/directory
mkdir launch-test
cd launch-test

# Initialize the project
uv init --python 3.12
uv venv
source .venv/bin/activate

# Install Daft Launcher
uv pip install "daft-launcher"
```

In your virtual environment, you should have Daft launcher installed — you can verify this by running `daft --version` which will return the latest version of Daft launcher available. You should also have a basic working directly that may look something like this:

```bash
/
|- .venv/
|- hello.py
|- pyproject.toml
|- README.md
|- .python-version
```

### Configure AWS Credentials

Establish an SSO connection to configure your AWS credentials:

```bash
# Configure your SSO
aws configure sso

# Login to your SSO
aws sso login
```

These commands should open your browsers. Accept the prompted requests and then return to your terminal, you should see a success message from the AWS CLI tool. At this point, your AWS CLI tool has been configured and your environment is fully setup.

### Initialize Configuration File

Initialize a default configuration file to store default values that you can later tune, and they are denoted as required and optional respectively.

```python
# Initialize the default .daft.toml configuration file
daft init-config

# Optionally you can also specify a custom name for your file
daft init-config my-custom-config.toml
```

Fill out the required values in your `.daft.toml` file. Optional configurations will have a default value pre-defined.

```toml
[setup]

# (required)
# The name of the cluster.
name = ...

# (required)
# The cloud provider that this cluster will be created in.
# Has to be one of the following:
# - "aws"
# - "gcp"
# - "azure"
provider = ...

# (optional; default = None)
# The IAM instance profile ARN which will provide this cluster with the necessary permissions to perform whatever actions.
# Please note that if you don't specify this field, Ray will create an automatic instance profile for you.
# That instance profile will be minimal and may restrict some of the feature of Daft.
iam_instance_profile_arn = ...

# (required)
# The AWS region in which to place this cluster.
region = ...

# (optional; default = "ec2-user")
# The ssh user name when connecting to the cluster.
ssh_user = ...

# (optional; default = 2)
# The number of worker nodes to create in the cluster.
number_of_workers = ...

# (optional; default = "m7g.medium")
# The instance type to use for the head and worker nodes.
instance_type = ...

# (optional; default = "ami-01c3c55948a949a52")
# The AMI ID to use for the head and worker nodes.
image_id = ...

# (optional; default = [])
# A list of dependencies to install on the head and worker nodes.
# These will be installed using UV (https://docs.astral.sh/uv/).
dependencies = [...]

[run]

# (optional; default = ['echo "Hello, World!"'])
# Any post-setup commands that you want to invoke manually.
# This is a good location to install any custom dependencies or run some arbitrary script.
setup_commands = [...]

```

### Spin Up a Cluster

`daft up` will spin up a cluster given the configuration file you initialized earlier. The configuration file contains all required information necessary for Daft launcher to know how to spin up a cluster.

```python
# Spin up a cluster using the default .daft.toml configuration file created earlier
daft up

# Alternatively spin up a cluster using a custom configuration file created earlier
daft up -c my-custom-config.toml
```

This command will do a couple of things:

1. First, it will reach into your cloud provider and spin up the necessary resources. This includes things such as the worker nodes, security groups, permissions, etc.

2. When the nodes are spun up, the ray and daft dependencies will be downloaded into a python virtual environment.

3. Next, any other custom dependencies that you've specified in the configuration file will then be downloaded.

4. Finally, the setup commands that you've specified in the configuration file will be run on the head node.

!!! note "Note"

    `daft up` will only return successfully when the head node is fully set up. Even though the command will request the worker nodes to also spin up, it will not wait for them to be spun up before returning. Therefore, when the command completes and you type `daft list`, the worker nodes may be in a “pending” state immediately after. Give it a few seconds and they should be fully running.

### Submit a Job

`daft submit` enables you to submit a working directory and command or a “job” to the remote cluster to be run.

```python
# Submit a job using the default .daft.toml configuration file
daft submit -i my-keypair.pem -w my-working-director

# Alternatively submit a job using a custom configuration file
daft submit -c my-custom-config.toml -i my-keypair.pem -w my-working-director
```

### Run a SQL Query

Daft supports SQL API so you can use `daft sql` to run raw SQL queries against your data. The SQL dialect is the postgres standard.

```python
# Run a sql query using the default .daft.toml configuration file
daft sql -- "\"SELECT * FROM my_table\""

# Alternatively you can run a sql query using a custom configuration file
daft sql -c my-custom-config.toml -- "\"SELECT * FROM my_table\""
```

### View Ray Dashboard

You can view the Ray dashboard of your running cluster with `daft connect` which establishes a port-forward over SSH from your local machine to the head node of the cluster (connecting `localhost:8265` to the remote head's `8265`).

```python
# Establish the port-forward using the default .daft.toml configuration file
daft connect -i my-keypair.pem

# Alternatively establish the port-forward using a custom configuration file
daft connect -c my-custom-config.toml -i my-keypair.pem
```

!!! note "Note"

    `daft connect` will require you to have the appropriate SSH keypair to authenticate against the remote head’s public SSH keypair. Make sure to pass this SSH keypair as an argument to the command.

### Spin Down a Cluster

`daft down` will spin down all instances of the cluster specified in the configuration file, not just the head node.

```python
# Spin down a cluster using the default .daft.toml configuration file
daft down

# Alternatively spin down a cluster using a custom configuration file
daft down -c my-custom-config.toml
```

### List Running and Terminated Clusters

`daft list` allows you to view the current state of all clusters, running and terminated, and includes each instance name and their given IPs (assuming the cluster is running). Here’s an example output after running `daft list`:

```python
Running:
  - daft-demo, head, i-053f9d4856d92ea3d, 35.94.91.91
  - daft-demo, worker, i-00c340dc39d54772d
  - daft-demo, worker, i-042a96ce1413c1dd6
```

Say we spun up another cluster `new-cluster` and then terminated it, here’s what the output of `daft list` would look like immediately after:

```python
Running:
  - daft-demo, head, i-053f9d4856d92ea3d, 35.94.91.91
  - daft-demo, worker, i-00c340dc39d54772d, 44.234.112.173
  - daft-demo, worker, i-042a96ce1413c1dd6, 35.94.206.130
Shutting-down:
  - new-cluster, head, i-0be0db9803bd06652, 35.86.200.101
  - new-cluster, worker, i-056f46bd69e1dd3f1, 44.242.166.108
  - new-cluster, worker, i-09ff0e1d8e67b8451, 35.87.221.180
```

In a few seconds later, the state of `new-cluster` will be finalized to “Terminated”.
