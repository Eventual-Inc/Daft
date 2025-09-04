#!/bin/bash

# Script to capture pytest duration output from stdin
# Usage: pytest <args> | ./capture-durations.sh <output_file>
#
# Example: pytest tests/ --durations=0 | ./capture-durations.sh "output.txt"

set -euo pipefail

if [ $# -ne 1 ]; then
    echo "Usage: <pytest_command> | $0 <output_file>"
    echo "Example: pytest tests/ --durations=0 | $0 'output.txt'"
    exit 1
fi

OUTPUT_FILE="$1"

# Redirect output to file after "slowest durations" line
awk -v output_file="$OUTPUT_FILE" '
/slowest durations/ {
    output_to_file = 1
    print > output_file
    next
}
{
    if (output_to_file) {
        print > output_file
    } else {
        print
    }
}'
