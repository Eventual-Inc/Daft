from __future__ import annotations

import shlex
import subprocess


def calculate_version() -> str:
    match_for_tag = r"v[0-9]*\.[0-9]*\.[0-9]*"
    exclude_for_tag = "*dev*"
    git_describe_base_cmd = f"git describe --match='{match_for_tag}' --exclude '{exclude_for_tag}' --tags"
    git_tag_version_number_command = f"{git_describe_base_cmd} --abbrev=0"

    git_tag_version_output = subprocess.check_output(shlex.split(git_tag_version_number_command))
    assert git_tag_version_output is not None
    cleaned_version = list(map(int, git_tag_version_output.decode().lstrip("v").rstrip("\n").split(".")))
    MAJOR, MINOR, PATCH = cleaned_version

    git_full_tag_version_output = subprocess.check_output(shlex.split(git_describe_base_cmd))
    assert git_full_tag_version_output is not None
    _, distance, commit = git_full_tag_version_output.decode().strip().split("-")
    if int(distance) > 0:
        version = f"{MAJOR}.{MINOR}.{PATCH + 1}+dev{distance}.{commit}"
    else:
        version = f"{MAJOR}.{MINOR}.{PATCH}"
    return version


if __name__ == "__main__":
    print(calculate_version())
