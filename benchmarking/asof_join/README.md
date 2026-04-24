# Asof Join Benchmarks

## Entry points

### 1. Generate data — `data_generation.py`

One-time step to upload benchmark datasets to S3.

```bash
python -m benchmarking.asof_join.data_generation --scale small
python -m benchmarking.asof_join.data_generation --all
```

---

### 2. Single-node benchmark — `single_node/infra/run.sh`

Provisions an EC2 instance via Terraform, rsync's the benchmark code, and runs it remotely.

```bash
cd benchmarking/asof_join/single_node/infra
./run.sh --scale small
./run.sh --scale large --systems polars,daft_native
# Use a custom Daft build:
./run.sh --scale small --daft-index-url https://ds0gqyebztuyf.cloudfront.net/builds/dev/<commit>
```

---

### 3. Distributed benchmark — `distributed/run.py`

Spins up a Ray cluster via `ray submit` and runs the distributed Daft benchmark.

```bash
python benchmarking/asof_join/distributed/run.py --scale small
python benchmarking/asof_join/distributed/run.py --scale large --workers 4
# Use a custom Daft build:
python benchmarking/asof_join/distributed/run.py --scale small --daft-index-url https://ds0gqyebztuyf.cloudfront.net/builds/dev/<commit>
```
