#!/usr/bin/env bash
# Usage: ./run_benchmark.sh --scale <small|medium|large> [--daft-index-url <url>] [benchmark flags]
#
# Examples:
#   ./run_benchmark.sh --scale large --systems polars,daft_native
#   ./run_benchmark.sh --scale small --daft-index-url https://ds0gqyebztuyf.cloudfront.net/builds/dev/598009e5587385c1fa95bf0e55c266b3e2ec1393
#
# --daft-index-url installs daft from a custom build (--pre --extra-index-url).
# All other flags are forwarded to __main__.py.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../../.." && pwd)"

DAFT_INDEX_URL=""
PASSTHROUGH_ARGS=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        --daft-index-url)
            DAFT_INDEX_URL="$2"
            shift 2
            ;;
        *)
            PASSTHROUGH_ARGS+=("$1")
            shift
            ;;
    esac
done

cd "${SCRIPT_DIR}"
terraform apply -auto-approve

IP=$(terraform output -raw public_ip)
KEY_NAME=$(terraform output -raw ssh_command | sed -E 's/.*-i ~\/\.ssh\/([^ ]+)\.pem.*/\1/')
SSH="ssh -i ${HOME}/.ssh/${KEY_NAME}.pem -o StrictHostKeyChecking=no ec2-user@${IP}"

echo "==> Waiting for SSH..."
for i in $(seq 1 30); do
    $SSH true 2>/dev/null && break
    sleep 10
done

$SSH "bash -s" "${DAFT_INDEX_URL}" < "${SCRIPT_DIR}/setup.sh"

$SSH "mkdir -p ~/benchmarking/asof_join && touch ~/benchmarking/__init__.py ~/benchmarking/asof_join/__init__.py"
rsync -az --exclude '__pycache__' --exclude '*.pyc' \
    -e "ssh -i ${HOME}/.ssh/${KEY_NAME}.pem -o StrictHostKeyChecking=no" \
    "${REPO_ROOT}/benchmarking/asof_join/single_node/" \
    "ec2-user@${IP}:~/benchmarking/asof_join/single_node/"

$SSH "tmux new-session -d -s bench \
    'python3.11 -m benchmarking.asof_join.single_node ${PASSTHROUGH_ARGS[*]} 2>&1 | tee ~/bench.log'"

echo "--------------------------------"
echo "Logs:          $SSH 'tail -f ~/bench.log'"

echo ""
echo "Results:       $SSH 'cat ~/asof_join_results/results.csv'"

echo ""
echo "Memory traces: $SSH 'cat ~/asof_join_results/*_mem.csv'"

echo ""
echo "OOM check:     $SSH 'sudo dmesg | grep -i \"oom\|killed process\"'"
