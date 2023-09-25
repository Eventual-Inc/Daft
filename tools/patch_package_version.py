from __future__ import annotations

import shlex
import subprocess

import toml


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
    split_full_tag_version = git_full_tag_version_output.decode().strip().split("-")

    if len(split_full_tag_version) > 1:
        _, distance, commit = split_full_tag_version
        version = f"{MAJOR}.{MINOR}.{PATCH}+dev{int(distance):04d}.{commit[1:]}"
    else:
        version = f"{MAJOR}.{MINOR}.{PATCH}"
    return version


def patch_cargo_toml_version(version: str):
    CARGO_TOML = "Cargo.toml"
    source = toml.load(CARGO_TOML)

    # update package version
    assert "package" in source
    package = source["package"]
    assert "version" in package
    package["version"] = version

    # update workspace version
    assert "workspace" in source
    workspace = source["workspace"]
    assert "package" in workspace
    package = workspace["package"]
    assert "version" in package
    package["version"] = version

    with open("Cargo.toml", "w") as out_file:
        toml.dump(source, out_file)
    print(f"Patched {CARGO_TOML} to version: {version}")


if __name__ == "__main__":
    version = calculate_version()
    patch_cargo_toml_version(version=version)
