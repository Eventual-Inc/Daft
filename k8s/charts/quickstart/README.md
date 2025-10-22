# Daft Quickstart Helm Chart

A Helm chart for running Daft data processing workloads on Kubernetes, with support for both simple single-node execution and distributed Ray-based processing.

## Features

- **Two deployment modes**: Simple mode for quick jobs, distributed mode for large-scale processing
- **Two execution methods**: Script injection for rapid prototyping, custom images for managing complex dependencies
- **No operator required**: Uses native Kubernetes resources only

## Prerequisites

- Kubernetes 1.19+
- Helm 3.x
- kubectl configured to access your cluster

## Quick Start

### Simple Mode (Default)

Run a quick Daft job using the native runner:

```bash
# Create a simple Python script
cat > my_script.py <<'EOF'
# /// script
# dependencies = ["daft"]
# ///
import daft

df = daft.from_pydict({
    "name": ["Alice", "Bob", "Charlie"],
    "age": [25, 30, 35]
})

df.filter(df["age"] > 25).show()
EOF

# Deploy
helm install my-job oci://ghcr.io/eventual-inc/daft/quickstart \
  --set-file job.script=my_script.py

# View logs
kubectl logs -f job/my-job-quickstart-job

# Cleanup
helm uninstall my-job
```

### Distributed Mode

Run a Daft job on a Ray cluster with multiple workers:

```bash
# Create a simple Python script with Ray
cat > my_script.py <<'EOF'
# /// script
# dependencies = ["daft", "ray[client]==2.46.0"]
# ///

import daft
import ray

ray.init(runtime_env={"pip": ["daft"]})

df = daft.from_pydict({
    "name": ["Alice", "Bob", "Charlie"],
    "age": [25, 30, 35]
})

df.filter(df["age"] > 25).show()
EOF

# Deploy with Ray cluster
helm install distributed-job oci://ghcr.io/eventual-inc/daft/quickstart \
  --set distributed=true \
  --set worker.replicas=3 \
  --set-file job.script=my_script.py

# Access Ray Dashboard
kubectl port-forward service/distributed-job-quickstart-head 8265:8265
# Open http://localhost:8265

# Cleanup
helm uninstall distributed-job
```

### Using a custom image

This chart also supports running Daft jobs from custom images. This is useful in scenarios where more complex dependencies are required.

```bash
helm install custom-image-job oci://ghcr.io/eventual-inc/daft/quickstart \
  --set image=my-image:tag \
  --set "job.command={python,my_script.py}"
```

## Installation

See [values.yaml](values.yaml) for detailed descriptions of all supported parameters.

## Monitoring

### View Job Status

```bash
# Get job status
kubectl get jobs -l app.kubernetes.io/instance=my-release

# View job logs
kubectl logs -f job/my-release-quickstart-job

# Check pod status
kubectl get pods -l app.kubernetes.io/instance=my-release
```

### Access Ray Dashboard (Distributed Mode)

```bash
# Port forward to access both Ray Dashboard and Grafana
kubectl port-forward service/my-release-quickstart-head 8265:8265 3000:3000

# Open http://localhost:8265 in browser
# Grafana panels will be embedded directly in the Ray Dashboard

# Optionally open http://localhost:3000 to access Grafana directly
# Default credentials: admin/admin
```

### Check Ray Cluster Status (Distributed Mode)

```bash
# Get cluster status
kubectl exec deployment/my-release-quickstart-head -- ray status

# View cluster resources
kubectl exec deployment/my-release-quickstart-head -- python -c "import ray; ray.init(); print(ray.cluster_resources())"
```

## Scaling

### Manual Scaling (Distributed Mode)

```bash
# Scale workers using kubectl
kubectl scale deployment my-release-quickstart-worker --replicas=10
```

## Troubleshooting

### Job Not Starting

```bash
# Check job events
kubectl describe job my-release-quickstart-job

# Check pod events
kubectl describe pod -l app.kubernetes.io/component=job

# View pod logs
kubectl logs -l app.kubernetes.io/component=job
```

### Workers Not Connecting (Distributed Mode)

```bash
# Check head service
kubectl get service -l ray.io/node-type=head

# Check head logs
kubectl logs deployment/my-release-quickstart-head

# Check worker logs
kubectl logs deployment/my-release-quickstart-worker
```

### Out of Memory Errors

Increase memory limits in values:

```yaml
job:
  resources:
    limits:
      memory: "16Gi"  # Increase as needed

head:
  resources:
    limits:
      memory: "32Gi"  # Increase as needed

worker:
  resources:
    limits:
      memory: "32Gi"  # Increase as needed
```

### Script Not Found Error

Ensure you're using `--set-file` (not `--set`):

```bash
# Correct
helm install my-job oci://ghcr.io/eventual-inc/daft/quickstart --set-file job.script=script.py

# Incorrect
helm install my-job oci://ghcr.io/eventual-inc/daft/quickstart --set job.script=script.py
```

### Ray Module Not Found (Distributed Mode)

Your custom image must include `ray[default]`:

```txt
# requirements.txt
daft
ray[default]
```

## Uninstalling

```bash
helm uninstall my-release
```

This removes all Kubernetes resources created by the chart.

## License

Apache 2.0

## Contributing

Contributions are welcome! Please see the [Daft repository](https://github.com/Eventual-Inc/Daft) for contribution guidelines.

## Support

- **Issues**: [GitHub Issues](https://github.com/Eventual-Inc/Daft/issues)
- **Documentation**: [docs.daft.ai](https://docs.daft.ai)
- **Community**: [Daft Discussions](https://github.com/Eventual-Inc/Daft/discussions)
