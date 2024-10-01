#!/bin/bash

# ANSI color codes
RED='\033[0;31m'
YELLOW='\033[1;33m'
GREEN='\033[0;32m'
NC='\033[0m' # No Color

# Check if cargo-machete is installed
if ! command -v cargo-machete &> /dev/null
then
    echo -e "${YELLOW}Warning: cargo-machete could not be found${NC}"
    echo -e "${YELLOW}To install, run: ${GREEN}cargo install cargo-machete${NC}"
    echo -e "${YELLOW}Skipping cargo-machete check${NC}"
else
    # Run cargo-machete with the --with-metadata flag
    echo -e "${GREEN}Running cargo-machete...${NC}"
    cargo machete --with-metadata
fi

# Always exit with 0 to not fail
exit 0
