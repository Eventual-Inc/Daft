#!/bin/bash

current_branch=$(git rev-parse --abbrev-ref HEAD)
echo "Running tpcds benchmarks on the branch: $current_branch"

gh workflow run run-script.yaml \
    --ref $current_branch \
    -f cluster_profile="debug_xs-x86" \
    -f entrypoint="ci/entrypoints/tpcds.sh"
