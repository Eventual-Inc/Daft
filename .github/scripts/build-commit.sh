#!/bin/bash

BRANCH_NAME=$(git rev-parse --abbrev-ref HEAD)
gh workflow run build-commit.yaml \
    --ref $BRANCH_NAME \
    -f commit=$BRANCH_NAME \
    -f machine_type="buildjet-8vcpu-ubuntu-2004"
