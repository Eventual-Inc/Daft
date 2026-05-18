#!/bin/bash
# Install a local daft wheel onto a single Ray worker.
# Usage: install_daft_built.sh <worker_ip> [<wheel_path>]
# Defaults: wheel = /tmp/daft-target/wheels/daft-0.3.0.dev0-cp310-abi3-manylinux_2_34_aarch64.whl
# Run in parallel across workers:
#   cat /tmp/worker_ips.txt | xargs -P32 -I{} bash install_daft_built.sh {}
set -e
IP="$1"
WHEEL="${2:-/tmp/daft-target/wheels/daft-0.3.0.dev0-cp310-abi3-manylinux_2_34_aarch64.whl}"
KEY="${KEY:-$HOME/ray_bootstrap_key.pem}"
SSH_OPTS="-i $KEY -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=15 -o LogLevel=ERROR"
WHEEL_NAME=$(basename "$WHEEL")
scp $SSH_OPTS "$WHEEL" "ec2-user@$IP:/tmp/$WHEEL_NAME" >/dev/null 2>&1
ssh $SSH_OPTS "ec2-user@$IP" "~/.venv/bin/pip install --quiet --force-reinstall --no-deps /tmp/$WHEEL_NAME 2>&1 | tail -2; ~/.venv/bin/python -c 'import daft; print(\"$IP\", daft.__version__)'"
