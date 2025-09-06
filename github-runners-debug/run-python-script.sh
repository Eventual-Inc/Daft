#!/bin/bash

# Script to run a Python script on the ray cluster
# Usage: ./run-python-script.sh <script_name>

set -e

if [ $# -eq 0 ]; then
    echo "Usage: $0 <script_name>"
    echo "Example: $0 test-ray-cluster.py"
    exit 1
fi

SCRIPT_NAME="$1"

if [ ! -f "$SCRIPT_NAME" ]; then
    echo "Error: Script '$SCRIPT_NAME' not found"
    exit 1
fi

if [ ! -f "ray.yaml" ]; then
    echo "Error: ray.yaml not found. Make sure you've run reproduce-ray-cluster.sh first"
    exit 1
fi

# Activate virtual environment if it exists
if [ -f ".venv/bin/activate" ]; then
    source .venv/bin/activate
fi

echo "Running $SCRIPT_NAME on ray cluster..."
ray submit ray.yaml "$SCRIPT_NAME"