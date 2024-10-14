#!/bin/bash
set -e

# ANSI color codes
RED='\033[0;31m'
YELLOW='\033[1;33m'
GREEN='\033[0;32m'
NC='\033[0m' # No Color

# Function to print colored messages
print_message() {
    local color=$1
    local message=$2
    echo -e "${color}${message}${NC}"
}

# Check if cargo-machete is installed
command -v cargo-machete >/dev/null 2>&1 || {
    print_message "$YELLOW" "Warning: cargo-machete could not be found"
    print_message "$YELLOW" "To install, run: cargo install cargo-machete"
    print_message "$RED" "Exiting due to missing cargo-machete"
    exit 1
}

# Run cargo-machete
cargo machete --with-metadata
