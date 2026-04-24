# Asof Join Benchmarks

## Entry points

### 1. Generate data — `data_generation.py`

One-time step to upload benchmark datasets to S3.

```bash
python -m benchmarking.asof_join.data_generation --scale <small|medium|large>
python -m benchmarking.asof_join.data_generation --all
```

---

### 2. Single-node benchmark — `single_node/infra/run.sh`

Provisions an EC2 instance via Terraform, rsync's the benchmark code, and runs it remotely.

```bash
cd benchmarking/asof_join/single_node/infra
./run.sh [--scale <small|medium|large>] [--daft-index-url <url>] [benchmark flags...]
```

| Flag | Default | Description |
|---|---|---|
| `--scale` | required | Dataset scale: `small`, `medium`, or `large` |
| `--daft-index-url` | — | Install Daft from a custom build index URL |
| `--systems` | `pandas,polars,daft_native` | Comma-separated list of systems to benchmark |
| `--n_runs` | `3` | Number of timed runs per system |
| `--skip_warmup` | false | Skip the warmup run |

---

### 3. Distributed benchmark — `distributed/run.py`

Spins up a Ray cluster via `ray submit` and runs the distributed Daft benchmark.

```bash
python benchmarking/asof_join/distributed/run.py [--scale <small|medium|large>] [...]
```

| Flag | Default | Description |
|---|---|---|
| `--scale` | `small` | Dataset scale: `small`, `medium`, or `large` |
| `--n_runs` | `3` | Number of timed runs |
| `--workers` | `2` | Number of Ray worker nodes to launch |
| `--daft-index-url` | — | Install Daft from a custom build index URL |
