#!/bin/bash

current_branch=$(git rev-parse --abbrev-ref HEAD)
echo "Running a build on the branch: $current_branch"

gh workflow run build-commit.yaml \
    --ref $current_branch \
    -f arch='x86'
