import sys
from pathlib import Path

import boto3
import constants
import wheellib

if __name__ == "__main__":
    commit_hash = sys.argv[1]
    platform_substring = sys.argv[2]
    path_to_wheel_dir = Path(sys.argv[3])

    assert path_to_wheel_dir.exists(), f"Path to wheel directory does not exist: {path_to_wheel_dir}"
    wheelpaths = iter(filepath for filepath in path_to_wheel_dir.iterdir() if filepath.suffix == constants.WHEEL_SUFFIX)

    def f(wheelpath: Path) -> bool:
        platform_tag = wheellib.get_platform_tag(wheelpath.name)
        return platform_substring in platform_tag

    filtered_wheelpaths: list[Path] = list(filter(f, wheelpaths))

    length = len(filtered_wheelpaths)
    if length == 0:
        raise Exception(f"No wheels found that match the given platform substring: {platform_substring}; expected 1")
    elif length > 1:
        raise Exception(
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
