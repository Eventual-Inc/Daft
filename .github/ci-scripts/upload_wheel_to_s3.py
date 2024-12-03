import argparse
from pathlib import Path

import boto3
import constants
import wheellib

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--commit-hash", required=True)
    parser.add_argument("--platform-substring", required=True, choices=["x86", "aarch", "arm"])
    parser.add_argument("--path-to-wheel-dir", required=True)
    args = parser.parse_args()

    commit_hash = args.commit_hash
    platform_substring = args.platform_substring
    path_to_wheel_dir = Path(args.path_to_wheel_dir)

    assert path_to_wheel_dir.exists(), f"Path to wheel directory does not exist: {path_to_wheel_dir}"
    wheelpaths = iter(filepath for filepath in path_to_wheel_dir.iterdir() if filepath.suffix == constants.WHEEL_SUFFIX)

    def f(wheelpath: Path) -> bool:
        platform_tag = wheellib.get_platform_tag(wheelpath.name)
        return platform_substring in platform_tag

    filtered_wheelpaths: list[Path] = list(filter(f, wheelpaths))

    length = len(filtered_wheelpaths)
    if length == 0:
        raise RuntimeError(f"No wheels found that match the given platform substring: {platform_substring}; expected 1")
    elif length > 1:
        raise RuntimeError(
            f"""Multiple wheels found that match the given platform substring: {platform_substring}; expected just 1
Wheels available: {wheelpaths}"""
        )
    [wheelpath] = filtered_wheelpaths
    s3 = boto3.client("s3")
    destination = Path("builds") / commit_hash / wheelpath.name
    s3.upload_file(
        Filename=wheelpath,
        Bucket=constants.BUCKET_NAME,
        Key=str(destination),
        ExtraArgs={"ACL": "public-read"},
    )
    print(wheelpath.name)
