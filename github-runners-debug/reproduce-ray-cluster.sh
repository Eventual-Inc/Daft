#!/bin/bash

# Script to reproduce ray cluster creation from the distributed TPC-H benchmark workflow
# Run this on a VM in the same VPC as the self-hosted GitHub runner

set -e

# Configuration - adjust these as needed
export TPCH_SCALE_FACTOR=${TPCH_SCALE_FACTOR:-1000}
export RAY_NUM_WORKERS=${RAY_NUM_WORKERS:-4}
export GITHUB_RUN_ID=${GITHUB_RUN_ID:-"debug-$(date +%s)"}
export GITHUB_RUN_ATTEMPT=${GITHUB_RUN_ATTEMPT:-1}
export AWS_REGION=${AWS_REGION:-us-west-2}
export UV_VENV_CLEAR=1

echo "Configuration:"
echo "  TPCH_SCALE_FACTOR: $TPCH_SCALE_FACTOR"
echo "  RAY_NUM_WORKERS: $RAY_NUM_WORKERS"
echo "  GITHUB_RUN_ID: $GITHUB_RUN_ID"
echo "  AWS_REGION: $AWS_REGION"
echo ""

# Setup Python environment
echo "Setting up Python environment..."
if ! command -v uv &> /dev/null; then
    echo "Installing uv..."
    curl -LsSf https://astral.sh/uv/install.sh | sh
    export PATH="$HOME/.local/bin:$PATH"
fi

# Create virtual environment
echo "Creating virtual environment..."
uv venv --python 3.9 .venv
source .venv/bin/activate

# Install dependencies
echo "Installing dependencies..."
uv pip install daft
uv pip install ray[default] boto3

# Generate ray configuration
echo "Generating ray configuration..."
envsubst < .github/assets/tpch-bench.yaml > ray.yaml
echo "Generated ray.yaml:"
cat ray.yaml
echo ""

# Download SSH key from AWS Secrets Manager
echo "Downloading SSH private key..."
mkdir -p ~/.ssh
KEY=$(aws secretsmanager get-secret-value --secret-id ci-github-actions-ray-cluster-key-3 --query SecretString --output text)
echo "$KEY" > ~/.ssh/ci-github-actions-ray-cluster-key.pem
chmod 600 ~/.ssh/ci-github-actions-ray-cluster-key.pem
echo "SSH key downloaded and configured."

# Spin up ray cluster
echo "Spinning up ray cluster..."
ray up ray.yaml -y

# Optional: Start ray dashboard in background
echo "Starting ray dashboard in background..."
ray dashboard ray.yaml &
DASHBOARD_PID=$!

echo ""
echo "Ray cluster is now running!"
echo "Dashboard PID: $DASHBOARD_PID"
echo ""
echo "To connect to the cluster, run:"
echo "  source .venv/bin/activate"
echo "  ray attach ray.yaml"
echo ""
echo "To run the TPC-H benchmark:"
echo "  PYTHONPATH=. python .github/ci-scripts/distributed_tpch.py"
echo ""
echo "To tear down the cluster when done:"
echo "  ray down ray.yaml -y"
echo "  kill $DASHBOARD_PID  # Stop dashboard"
