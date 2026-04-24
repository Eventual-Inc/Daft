#!/usr/bin/env python3
"""Launch the distributed asof-join benchmark via ray submit.

Usage:
    python run.py --scale small
    python run.py --scale small --daft-index-url https://ds0gqyebztuyf.cloudfront.net/builds/dev/<commit>
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
import tempfile
import urllib.request
from pathlib import Path

DEPLOYMENT_YAML = Path(__file__).parent / "deployment.yaml"
BENCHMARK_SCRIPT = Path(__file__).parent / "benchmark.py"


def resolve_wheel_url(daft_index_url: str) -> str:
    """Fetch the index page and extract the x86_64 wheel URL."""
    index_url = f"{daft_index_url}/daft/index.html"
    with urllib.request.urlopen(index_url) as resp:
        html = resp.read().decode()
    match = re.search(r'href="([^"]*x86_64[^"]*)"', html)
    if not match:
        print(f"ERROR: No x86_64 wheel found at {index_url}", file=sys.stderr)
        sys.exit(1)
    return f"https://ds0gqyebztuyf.cloudfront.net{match.group(1)}"


def build_custom_yaml(*, wheel_url: str | None = None, workers: int | None = None) -> str:
    """Return path to a temporary deployment.yaml with optional customizations."""
    content = DEPLOYMENT_YAML.read_text()
    if wheel_url:
        content = content.replace(
            "python -m pip install 'ray[default]' 'daft[ray,aws]'",
            f"python -m pip install 'ray[default]' {wheel_url} pyarrow fsspec boto3",
        )
    if workers is not None:
        content = re.sub(r"^max_workers:\s*\d+", f"max_workers: {workers}", content, flags=re.MULTILINE)
        content = re.sub(
            r"(ray\.worker\.default:\s*\n\s*min_workers:)\s*\d+",
            rf"\g<1> {workers}",
            content,
        )
        content = re.sub(
            r"(ray\.worker\.default:\s*\n\s*min_workers:\s*\d+\s*\n\s*max_workers:)\s*\d+",
            rf"\g<1> {workers}",
            content,
        )
    tmp = tempfile.NamedTemporaryFile(
        mode="w", suffix=".yaml", prefix="deployment_custom_daft_", delete=False
    )
    tmp.write(content)
    tmp.close()
    return tmp.name


def main():
    parser = argparse.ArgumentParser(description="Run distributed asof-join benchmark")
    parser.add_argument("--scale", default="small", choices=["small", "medium", "large"])
    parser.add_argument("--n_runs", type=int, default=3)
    parser.add_argument(
        "--daft-index-url",
        default="",
        help="Custom Daft wheel index URL (e.g. https://ds0gqyebztuyf.cloudfront.net/builds/dev/<commit>)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=2,
        help="Number of Ray worker nodes to launch (default: 2)",
    )
    parser.add_argument(
        "--no-restart",
        action="store_true",
        help="Skip ray up if cluster is already running",
    )
    args = parser.parse_args()

    wheel_url = None
    if args.daft_index_url:
        wheel_url = resolve_wheel_url(args.daft_index_url)
        print(f"Resolved wheel: {wheel_url}")

    needs_custom = wheel_url or args.workers != 2
    if needs_custom:
        yaml_path = build_custom_yaml(wheel_url=wheel_url, workers=args.workers)
        print(f"Using custom deployment yaml: {yaml_path}")
    else:
        yaml_path = str(DEPLOYMENT_YAML)

    cmd = [
        "ray",
        "submit",
        yaml_path,
        str(BENCHMARK_SCRIPT),
        "--start",
    ]
    if args.no_restart:
        cmd.append("--no-restart")
    cmd += ["--", "--scale", args.scale, "--n_runs", str(args.n_runs), "--workers", str(args.workers)]

    print(f"Running: {' '.join(cmd)}")
    result = subprocess.run(cmd)
    sys.exit(result.returncode)


if __name__ == "__main__":
    main()
