#!/usr/bin/env bash
# Usage:
#   ./run.sh --scale small
#   ./run.sh --scale small --daft-index-url https://ds0gqyebztuyf.cloudfront.net/builds/dev/<commit>
#
# Run from this directory (benchmarking/asof_join/distributed/).
# Brings up the cluster, installs deps, runs the benchmark.
set -euo pipefail

DAFT_INDEX_URL=""
SCALE="small"
N_RUNS="3"

while [[ $# -gt 0 ]]; do
  case $1 in
    --daft-index-url) DAFT_INDEX_URL="$2"; shift 2;;
    --scale)          SCALE="$2"; shift 2;;
    --n_runs)         N_RUNS="$2"; shift 2;;
    *)                echo "Unknown arg: $1"; exit 1;;
  esac
done

YAML="deployment.yaml"

# If a custom daft index is provided, resolve the wheel URL and inject
# a direct install into setup_commands (same approach as single_node/setup.sh).
if [[ -n "$DAFT_INDEX_URL" ]]; then
  WHEEL_PATH=$(curl -sf "${DAFT_INDEX_URL}/daft/index.html" \
    | sed -n 's/.*href="\([^"]*x86_64[^"]*\)".*/\1/p' | head -1)
  WHEEL_URL="https://ds0gqyebztuyf.cloudfront.net${WHEEL_PATH}"
  echo "Resolved wheel: $WHEEL_URL"

  YAML="/tmp/deployment_custom_daft.yaml"
  # Replace the daft install line with: install wheel directly + deps separately.
  sed "s|python -m pip install 'ray\[default\]' 'daft\[ray,aws\]'|python -m pip install 'ray[default]' ${WHEEL_URL} pyarrow fsspec boto3|" deployment.yaml > "$YAML"
  echo "Using custom daft wheel in deployment yaml"
fi

# Bring up the cluster (installs deps on all nodes via setup_commands).
ray up "$YAML" --yes

# Sync the script to the head node, then run in tmux so it survives SSH disconnects.
ray rsync-up "$YAML" run_benchmark.py /home/ec2-user/run_benchmark.py
ray exec "$YAML" \
  "tmux kill-session -t bench 2>/dev/null; tmux new-session -d -s bench 'python -u ~/run_benchmark.py --scale $SCALE --n_runs $N_RUNS 2>&1 | tee ~/benchmark.log'"

echo ""
echo "Benchmark running in tmux session 'bench' on the head node."
echo "To check progress:  ray exec $YAML 'tail -f ~/benchmark.log'"
echo "To attach:          ray attach $YAML --tmux"
