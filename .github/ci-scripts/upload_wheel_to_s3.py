import argparse
from pathlib import Path

import boto3
import wheellib

BUCKET_NAME = "github-actions-artifacts-bucket"
WHEEL_SUFFIX = ".whl"

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--commit-hash", type=str, required=True)
    parser.add_argument("--subdir", type=str, required=True)
    parser.add_argument("--platform-substring", type=str, required=True, choices=["x86", "aarch", "arm"])
    parser.add_argument("--path-to-wheel-dir", type=Path, required=True)
    args = parser.parse_args()

    assert args.path_to_wheel_dir.exists(), f"Path to wheel directory does not exist: {args.path_to_wheel_dir}"
    wheelpaths = iter(filepath for filepath in args.path_to_wheel_dir.iterdir() if filepath.suffix == WHEEL_SUFFIX)

    def f(wheelpath: Path) -> bool:
        platform_tag = wheellib.get_platform_tag(wheelpath.name)
        return args.platform_substring in platform_tag

    filtered_wheelpaths: list[Path] = list(filter(f, wheelpaths))

    length = len(filtered_wheelpaths)
    if length == 0:
        raise RuntimeError(
            f"No wheels found that match the given platform substring: {args.platform_substring}; expected 1"
        )
    elif length > 1:
        raise RuntimeError(
            f"""Multiple wheels found that match the given platform substring: {args.platform_substring}; expected just 1
Wheels available: {wheelpaths}"""
        )
    [wheelpath] = filtered_wheelpaths
    s3 = boto3.client("s3")
    destination = Path("builds") / args.commit_hash / args.subdir / wheelpath.name
    s3.upload_file(
        Filename=wheelpath,
        Bucket=BUCKET_NAME,
        Key=str(destination),
        ExtraArgs={"ACL": "public-read"},
    )
    print(wheelpath.name)
