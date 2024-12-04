# /// script
# requires-python = ">=3.12"
# dependencies = ['pydantic']
# ///

import sys
from argparse import ArgumentParser
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import read_inline_metadata
from pydantic import BaseModel, Field

CLUSTER_NAME_PLACEHOLDER = "\\{{CLUSTER_NAME}}"
DAFT_INSTALL_PLACEHOLDER = "\\{{DAFT_INSTALL}}"
OTHER_INSTALL_PLACEHOLDER = "\\{{OTHER_INSTALLS}}"
PYTHON_VERSION_PLACEHOLDER = "\\{{PYTHON_VERSION}}"
CLUSTER_PROFILE__NODE_COUNT = "\\{{CLUSTER_PROFILE/node_count}}"
CLUSTER_PROFILE__INSTANCE_TYPE = "\\{{CLUSTER_PROFILE/instance_type}}"
CLUSTER_PROFILE__IMAGE_ID = "\\{{CLUSTER_PROFILE/image_id}}"
CLUSTER_PROFILE__SSH_USER = "\\{{CLUSTER_PROFILE/ssh_user}}"
CLUSTER_PROFILE__VOLUME_MOUNT = "\\{{CLUSTER_PROFILE/volume_mount}}"

NOOP_STEP = "echo 'noop step; skipping'"


@dataclass
class Profile:
    node_count: int
    instance_type: str
    image_id: str
    ssh_user: str
    volume_mount: Optional[str] = None


class Metadata(BaseModel, extra="allow"):
    dependencies: list[str] = Field(default_factory=list)
    env: dict[str, str] = Field(default_factory=dict)


profiles: dict[str, Optional[Profile]] = {
    "debug_xs-x86": Profile(
        instance_type="t3.large",
        image_id="ami-04dd23e62ed049936",
        node_count=1,
        ssh_user="ubuntu",
    ),
    "medium-x86": Profile(
        instance_type="i3.2xlarge",
        image_id="ami-04dd23e62ed049936",
        node_count=4,
        ssh_user="ubuntu",
        volume_mount=""" |
    findmnt /tmp 1> /dev/null
    code=$?
    if [ $code -ne 0 ]; then
        sudo mkfs.ext4 /dev/nvme0n1
        sudo mount -t ext4 /dev/nvme0n1 /tmp
        sudo chmod 777 /tmp
    fi""",
    ),
}


if __name__ == "__main__":
    content = sys.stdin.read()

    parser = ArgumentParser()
    parser.add_argument("--cluster-name", required=True)
    parser.add_argument("--daft-version")
    parser.add_argument("--python-version", required=True)
    parser.add_argument("--cluster-profile", required=True, choices=["debug_xs-x86", "medium-x86"])
    parser.add_argument("--script", required=True)
    args = parser.parse_args()

    content = content.replace(CLUSTER_NAME_PLACEHOLDER, args.cluster_name)
    content = content.replace(DAFT_INSTALL_PLACEHOLDER, f"getdaft=={args.daft_version}" if args.daft_version else "")
    content = content.replace(PYTHON_VERSION_PLACEHOLDER, args.python_version)

    profile = profiles[args.cluster_profile]
    content = content.replace(CLUSTER_PROFILE__NODE_COUNT, str(profile.node_count))
    content = content.replace(CLUSTER_PROFILE__INSTANCE_TYPE, profile.instance_type)
    content = content.replace(CLUSTER_PROFILE__IMAGE_ID, profile.image_id)
    content = content.replace(CLUSTER_PROFILE__SSH_USER, profile.ssh_user)
    content = content.replace(
        CLUSTER_PROFILE__VOLUME_MOUNT, profile.volume_mount if profile.volume_mount else NOOP_STEP
    )

    assert Path(args.script).exists()
    with open(args.script) as f:
        metadata = Metadata(**read_inline_metadata.read(f.read()))

    content = content.replace(OTHER_INSTALL_PLACEHOLDER, " ".join(metadata.dependencies))

    print(content)
