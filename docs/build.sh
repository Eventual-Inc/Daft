#!/bin/sh

# Fail script on first command failure
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

# Go back to /docs/ folder and build Sphinx docs
cd docs/
rm -rf build/
uv run make html
