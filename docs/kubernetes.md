# Running on Kubernetes

Daft can run on Kubernetes using the Daft quickstart Helm chart. This allows you to test drive both simple single-node jobs and distributed Ray-based workloads on your Kubernetes cluster without any additional dependencies.

## Prerequisites

- Kubernetes 1.19+
- Helm 3.x
- kubectl configured to access your cluster

## Simple Mode

Run a quick Daft job on Kubernetes using the native runner:

=== "🐍 Python"

    ```python
    # Create a script file: my_script.py
    # /// script
    # dependencies = ["daft"]
    # ///
    import daft

    df = daft.from_pydict({
        "a": [3, 2, 5, 6, 1, 4],
        "b": [True, False, False, True, True, False]
    })

    df = df.where(df["b"]).sort(df["a"])
    print(df.collect())
    ```

Deploy to Kubernetes:

```bash
# Install the job with your script
helm install my-job oci://ghcr.io/eventual-inc/daft/quickstart \
  --set-file job.script=my_script.py

# View logs
kubectl logs -f job/my-job-quickstart-job

# Cleanup
helm uninstall my-job
```

## Distributed Mode

Run the same Daft job on a Ray cluster with multiple workers:

=== "🐍 Python"

    ```python
    # Create a script file: distributed_script.py
    # /// script
    # dependencies = ["daft", "ray[client]==2.46.0"]
    # ///
    import daft
    import ray

    ray.init(runtime_env={"pip": ["daft"]})

    df = daft.from_pydict({
        "a": [3, 2, 5, 6, 1, 4],
        "b": [True, False, False, True, True, False]
    })

    df = df.where(df["b"]).sort(df["a"])
    print(df.collect())
    ```

Deploy with Ray cluster:

```bash
# Install with Ray cluster (3 workers)
helm install distributed-job oci://ghcr.io/eventual-inc/daft/quickstart \
  --set distributed=true \
  --set worker.replicas=3 \
  --set-file job.script=distributed_script.py

# Access Ray Dashboard
kubectl port-forward service/distributed-job-quickstart-head 8265:8265
# Open http://localhost:8265 in your browser

# View job logs
kubectl logs -f job/distributed-job-quickstart-job

# Cleanup
helm uninstall distributed-job
```

## What's Next?

Visit the [Helm chart documentation](https://github.com/Eventual-Inc/Daft/tree/main/k8s/charts/quickstart) to try out your own Daft workloads on Kubernetes.
