#!/bin/sh

set -e

# Install uv
if ! command -v uv &> /dev/null
then
    curl -LsSf https://astral.sh/uv/install.sh | sh
fi

# Build Daft by going to top-level directory
cd ../

# Install necessary Python
# Check if virtualenv has been initialized
if [ ! -d ".venv" ]; then
    uv python install 3.10
    uv venv --python 3.10
    uv python pin 3.10
fi

# Activate the uv virtual environment
. .venv/bin/activate

# Build and install Daft
uv pip install -r requirements-dev.txt
uv run --with pip maturin develop --extras=all

# Delete all built data
rm -rf build/
rm -rf sphinx-build/

# Go back to /docs/ folder and build Sphinx docs
# This will output HTML to sphinx-build/html
cd docs/
uv run make html

# Copy built docs for landing page and API docs to final build directory
mkdir -p build/
cp -r landing-page-source/ build/
cp -r sphinx-build/html/ build/docs/
