#!/bin/bash

# Simple health check script for Iceberg REST server
# Uses bash built-in /dev/tcp for port checking

set -e

HOST="localhost"
PORT="8181"

# Use bash built-in /dev/tcp to check if port is open
if timeout 5 bash -c "</dev/tcp/$HOST/$PORT"; then
    echo "Health check passed - port $PORT is accessible"
    exit 0
else
    echo "Health check failed - cannot connect to port $PORT"
    exit 1
fi
