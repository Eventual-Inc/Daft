#!/usr/bin/env python3

"""This script generates the files needed to implement Python's Simple repository API (https://packaging.python.org/en/latest/specifications/simple-repository-api/#simple-repository-api).

Given a S3 bucket with these files:

bucket/
- foo-1.0-none-any.whl
- foo-1.1-none-any.whl
- bar-1.0-none-any.whl

It will generate these files in `dist/indices/`:
- index.html
- foo/index.html
- bar/index.html

Then, put these files in the same bucket with a Cloudfront distribution configured to serve via HTTPS and serve index.html for directory paths.
"""

from __future__ import annotations

import os
import sys
import urllib.parse

import boto3
from packaging.utils import parse_wheel_filename


def write_file_ensure_dir(filename, s):
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, "w") as f:
        f.write(s)


def generate_root_index(prefix, pkg_names):
    links = "\n".join([f'<a href="/{prefix}/{urllib.parse.quote(name)}/">{name}</a>' for name in pkg_names])

    return f"""<!DOCTYPE html>
<html>
  <body>
    {links}
  </body>
</html>"""


def generate_pkg_index(prefix, wheel_names):
    links = "\n".join([f'<a href="/{prefix}/{urllib.parse.quote(name)}">{name}</a>' for name in wheel_names])

    return f"""<!DOCTYPE html>
<html>
  <body>
    {links}
  </body>
</html>"""


def main():
    if len(sys.argv) != 2:
        print("Usage: python generate_simple_py_index.py s3://bucket")
        sys.exit(1)

    s3_prefix = "s3://"
    s3_url = sys.argv[1]
    assert s3_url.startswith(s3_prefix)
    (bucket, _, prefix) = s3_url.removeprefix(s3_prefix).removesuffix("/").partition("/")

    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

    pkg_map = {}
    for page in pages:
        for obj in page["Contents"]:
            if obj["Key"].endswith(".whl"):
                wheel_name = obj["Key"].removeprefix(prefix + "/")

                pkg_name, _, _, _ = parse_wheel_filename(wheel_name)
                if pkg_name not in pkg_map:
                    pkg_map[pkg_name] = []
                pkg_map[pkg_name].append(wheel_name)

    root_index = generate_root_index(prefix, pkg_map.keys())
    write_file_ensure_dir("dist/indices/index.html", root_index)

    for pkg_name, wheel_names in pkg_map.items():
        pkg_index = generate_pkg_index(prefix, wheel_names)
        write_file_ensure_dir(f"dist/indices/{pkg_name}/index.html", pkg_index)


if __name__ == "__main__":
    main()
