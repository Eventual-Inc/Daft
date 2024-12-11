#!/bin/bash

# This script should only be run in CI.
# CI should set `RUN_MODE="ci"` prior to running this script.
#
# This script should not be run locally.
if [ "$RUN_MODE" != "ci" ]; then
    echo "Looks like you're running this script locally on your laptop." >&2
    echo "This script is meant to run in CI only." >&2
    echo "Failing..." >&2
    exit 1
fi

uv pip install duckdb
DAFT_RUNNER=ray python -m benchmarking.tpcds --questions '3' --scale-factor '0.01'
