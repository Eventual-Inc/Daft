#!/usr/bin/env bash
set -euo pipefail

sudo dnf install -y python3.11 python3.11-pip tmux
python3.11 -m pip install --upgrade pip
python3.11 -m pip install daft pandas polars psutil pyarrow boto3 numpy
python3.11 -c "import daft, pandas, polars, psutil; print('OK')"
