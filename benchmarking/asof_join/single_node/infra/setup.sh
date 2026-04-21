#!/usr/bin/env bash
# Usage: bash setup.sh [daft-extra-index-url]
#   If an extra index URL is provided, daft is installed from that custom build.
#   Otherwise, the latest stable daft is installed from PyPI.
#
# Examples:
#   bash setup.sh  (installs stable daft)
#   bash setup.sh https://ds0gqyebztuyf.cloudfront.net/builds/dev/598009e5587385c1fa95bf0e55c266b3e2ec1393
set -euo pipefail

DAFT_EXTRA_INDEX_URL="${1:-}"

sudo dnf install -y python3.11 python3.11-pip tmux
python3.11 -m pip install --upgrade pip

if [[ -n "${DAFT_EXTRA_INDEX_URL}" ]]; then
    # Dev builds have local version segments (e.g. 0.7.10.dev20+g598009e55) which pip
    # can't resolve normally. Grab the x86_64 wheel URL from the index and install directly.
    WHEEL_PATH=$(curl -sf "${DAFT_EXTRA_INDEX_URL}/daft/index.html" \
        | sed -n 's/.*href="\([^"]*x86_64[^"]*\)".*/\1/p' | head -1)
    WHEEL_URL="https://ds0gqyebztuyf.cloudfront.net${WHEEL_PATH}"
    echo "Installing wheel: ${WHEEL_URL}"
    python3.11 -m pip install "${WHEEL_URL}"
    # Install daft's own deps + benchmark deps from PyPI.
    python3.11 -m pip install pyarrow fsspec tqdm packaging pandas polars psutil boto3 numpy
else
    python3.11 -m pip install daft pandas polars psutil pyarrow boto3 numpy
fi

python3.11 -c "import daft, pandas, polars, psutil; print('OK')"
