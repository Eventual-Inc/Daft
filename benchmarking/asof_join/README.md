## Summary

Adds a self-contained benchmarking suite for Daft's `join_asof` operation:

- **`data_generation.py`** — generates reproducible left/right parquet datasets at three scales (`small`, `medium`, `large`) with clustered timestamps and Zipf-skewed entity distribution, written to `benchmarking/data/asof_join/`
- **`benchmark.py`** — runs a single asof-join using Daft's native or Ray runner, wrapped in a memray memory tracker, and prints a JSON result with wall time and memray output path
- **`deployment.yaml`** — Ray cluster config for AWS (1 `m7i.large` head + 4 `r7i.4xlarge` workers) for distributed runs against S3

## How to run

From inside `benchmarking/asof_join/`:

**1. Generate data (one-time)**
```bash
python data_generation.py --scale small
# or --scale medium / --scale large / --all
```

**2. Run locally (native runner)**
```bash
python benchmark.py --scale small
# Output: asof_join_memray.bin + JSON result on stdout
```

**3. Inspect memory profile**
```bash
memray flamegraph asof_join_memray.bin
```

**Run on a Ray cluster**

> Before running: update `DATA_ROOT` to your S3 bucket and uncomment `daft.set_runner_ray()` in `benchmark.py`. Also update the S3 bucket and IAM settings in `deployment.yaml`.

Spin up the cluster:
```bash
ray up benchmarking/asof_join/deployment.yaml
```

Forward the dashboard in one terminal:
```bash
ray dashboard benchmarking/asof_join/deployment.yaml
```

Submit the job in another (after updating `DATA_ROOT` and `daft.set_runner_ray()` in `benchmark.py`):
```bash
ray job submit --address "http://localhost:8265" --working-dir benchmarking/asof_join -- python benchmark.py --scale small
```

Tear down when done:
```bash
ray down benchmarking/asof_join/deployment.yaml
```
