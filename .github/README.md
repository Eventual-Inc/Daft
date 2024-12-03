# GitHub Actions Workflows

## Run-cluster

In order to run a cluster, visit the [run-cluster](https://github.com/Eventual-Inc/Daft/actions/workflows/run-cluster.yaml) workflow UI on the Daft GitHub repo.
Click on the `Run workflow` button and fill in the required inputs.

For a basic run, try inputting `0.3.14` into the `daft_version` input.
Next, enter `python simple.py` into the `command` input.
Keep the other fields (e.g., `working-dir`) alone for now.
Finally, click the green `Run workflow` button to submit the job to the cluster.

This will spawn up a `medium-x86` cluster that will download `daft` version `0.3.14`.
It will then submit the `.github/working-dir` directory to the cluster and run the `python simple.py` command on the head node.

## Custom python scripts

If you want to run a custom script, then you can just create a new python script inside of `.github/working-dir` and invoke that script in the command field.
For example, create a new python file called `.github/working-dir/my_custom_python_script.py`.
Then, when submitting the workflow, enter `python my_custom_python_script.py` into the command field.

That will submit your custom python script to the cluster and run it on the head node.

## Cluster profiles

Cluster profiles give the end-user a discrete set of options of cluster configurations to choose from.
The current available ones are:
- medium-x86 (spins up 1 head node and 2 worker nodes, each of instance type `i3.2xlarge`)
- debug_xs-x86 (spins up 1 head node and 0 worker nodes, each of instance type `t3.large`)
