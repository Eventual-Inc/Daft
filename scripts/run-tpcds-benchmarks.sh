#!/bin/bash

gh workflow run run-script.yaml \
    --ref refactor/run-cluster \
    -f entrypoint="ci/entrypoints/tpcds.sh"
