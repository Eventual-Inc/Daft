#!/usr/bin/env bash
# Usage: ./run_benchmark.sh --scale <small|medium|large> [benchmark flags]
#
# Example:
#   ./run_benchmark.sh --scale large --systems polars,daft_native
#
# All flags are forwarded to __main__.py.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"

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

$SSH "bash -s" < "${SCRIPT_DIR}/setup.sh"

$SSH "mkdir -p ~/benchmarking && touch ~/benchmarking/__init__.py"
rsync -az --exclude '__pycache__' --exclude '*.pyc' \
    -e "ssh -i ${HOME}/.ssh/${KEY_NAME}.pem -o StrictHostKeyChecking=no" \
    "${REPO_ROOT}/benchmarking/asof_join/" \
    "ec2-user@${IP}:~/benchmarking/asof_join/"

$SSH "tmux new-session -d -s bench \
    'python3.11 -m benchmarking.asof_join $* 2>&1 | tee ~/bench.log'"

echo "--------------------------------"
echo "Logs:          $SSH 'tail -f ~/bench.log'"

echo ""
echo "Results:       $SSH 'cat ~/asof_join_results/results.csv'"

echo ""
echo "Memory traces: $SSH 'cat ~/asof_join_results/*_mem.csv'"

echo ""
echo "OOM check:     $SSH 'sudo dmesg | grep -i \"oom\|killed process\"'"
