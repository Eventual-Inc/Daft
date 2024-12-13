#!/bin/bash

current_branch=$(git rev-parse --abbrev-ref HEAD)
echo "Running tpcds benchmarks on the branch: $current_branch"

gh workflow run run-script.yaml \
    --ref refactor/run-cluster \
    -f entrypoint="ci/entrypoints/tpcds.sh"
