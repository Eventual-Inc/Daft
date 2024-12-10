"""Given a commit hash and a "platform substring", prints the wheelname of the wheel (if one exists) to stdout.

# Example

```bash
COMMIT_HASH="abcdef0123456789"
PLATFORM_SUBSTRING="x86"
WHEELNAME=$(python get_wheel_name_from_s3.py $COMMIT_HASH $PLATFORM_SUBSTRING)

echo $WHEELNAME
# Will echo the wheelname if a wheel exists that matches the platform substring.
# Otherwise, will echo nothing.
```
"""

import argparse
from pathlib import Path

import boto3
import wheellib

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--commit-hash", required=True)
    parser.add_argument("--platform-substring", required=True, choices=["x86", "aarch", "arm"])
    args = parser.parse_args()

    commit_hash = args.commit_hash
    platform_substring = args.platform_substring

    s3 = boto3.client("s3")
    response = s3.list_objects_v2(Bucket="github-actions-artifacts-bucket", Prefix=f"builds/{commit_hash}/")
    matches = []
    for content in response.get("Contents", []):
        wheelname = Path(content["Key"]).name
        platform_tag = wheellib.get_platform_tag(wheelname)
        if platform_substring in platform_tag:
            matches.append(wheelname)

    if len(matches) > 1:
        raise RuntimeError(
            f"Multiple wheels found that match the given platform substring: {platform_substring}; expected just 1"
        )

    print(matches[0]) if matches else None
