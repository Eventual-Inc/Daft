#!/bin/bash

# Script to tear down the ray cluster created by reproduce-ray-cluster.sh

set -e

echo "Tearing down ray cluster..."

# Activate virtual environment if it exists
if [ -f ".venv/bin/activate" ]; then
    source .venv/bin/activate
fi

# Tear down the cluster
if [ -f "ray.yaml" ]; then
    ray down ray.yaml -y
    echo "Ray cluster has been torn down."
else
    echo "ray.yaml not found. Make sure you're running this from the same directory as reproduce-ray-cluster.sh"
    exit 1
fi

# Kill any running ray dashboard processes
pkill -f "ray dashboard" || echo "No ray dashboard processes found."

echo "Cleanup complete."