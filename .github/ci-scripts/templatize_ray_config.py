import sys
from argparse import ArgumentParser
from dataclasses import dataclass
from typing import Optional

CLUSTER_NAME_PLACEHOLDER = "\\{{CLUSTER_NAME}}"
DAFT_INSTALL_PLACEHOLDER = "\\{{DAFT_INSTALL}}"
PYTHON_VERSION_PLACEHOLDER = "\\{{PYTHON_VERSION}}"
CLUSTER_PROFILE__NODE_COUNT = "\\{{CLUSTER_PROFILE/node_count}}"
CLUSTER_PROFILE__INSTANCE_TYPE = "\\{{CLUSTER_PROFILE/instance_type}}"
CLUSTER_PROFILE__IMAGE_ID = "\\{{CLUSTER_PROFILE/image_id}}"
CLUSTER_PROFILE__SSH_USER = "\\{{CLUSTER_PROFILE/ssh_user}}"
CLUSTER_PROFILE__VOLUME_MOUNT = "\\{{CLUSTER_PROFILE/volume_mount}}"


@dataclass
class Profile:
    node_count: int
    instance_type: str
    image_id: str
    ssh_user: str
    volume_mount: Optional[str] = None


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
    parser.add_argument("--cluster-name")
    parser.add_argument("--daft-wheel-url")
    parser.add_argument("--daft-version")
    parser.add_argument("--python-version")
    parser.add_argument("--cluster-profile")
    args = parser.parse_args()

    if args.cluster_name:
        content = content.replace(CLUSTER_NAME_PLACEHOLDER, args.cluster_name)

    if args.daft_wheel_url and args.daft_version:
        raise ValueError(
            "Cannot specify both the `daft-wheel-name` and the `daft-version`; please choose one or the other"
        )
    elif args.daft_wheel_url:
        daft_install = args.daft_wheel_url
    elif args.daft_version:
        daft_install = f"getdaft=={args.daft_version}"
    else:
        daft_install = "getdaft"
    content = content.replace(DAFT_INSTALL_PLACEHOLDER, daft_install)

    if args.python_version:
        content = content.replace(PYTHON_VERSION_PLACEHOLDER, args.python_version)

    if cluster_profile := args.cluster_profile:
        cluster_profile: str
        if cluster_profile not in profiles:
            raise Exception(f'Cluster profile "{cluster_profile}" not found')

        profile = profiles[cluster_profile]
        if profile is None:
            raise Exception(f'Cluster profile "{cluster_profile}" not yet implemented')

        assert profile is not None
        content = content.replace(CLUSTER_PROFILE__NODE_COUNT, str(profile.node_count))
        content = content.replace(CLUSTER_PROFILE__INSTANCE_TYPE, profile.instance_type)
        content = content.replace(CLUSTER_PROFILE__IMAGE_ID, profile.image_id)
        content = content.replace(CLUSTER_PROFILE__SSH_USER, profile.ssh_user)
        if profile.volume_mount:
            content = content.replace(CLUSTER_PROFILE__VOLUME_MOUNT, profile.volume_mount)
        else:
            content = content.replace(CLUSTER_PROFILE__VOLUME_MOUNT, "echo 'Nothing to mount; skipping'")

    print(content)
