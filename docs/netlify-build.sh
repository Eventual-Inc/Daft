#!/bin/sh

# Install uv
curl -LsSf https://astral.sh/uv/install.sh | sh

# Build Daft by going to top-level directory
pushd ../

# Install necessary Python
uv python install 3.10
uv venv --python 3.10
uv python pin 3.10
source .venv/bin/activate

# Build and install Daft
uv pip install -r requirements-dev.txt
maturin develop --extras=all

# Go back to /docs/ folder and build docs
popd
make html
