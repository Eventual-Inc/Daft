import shutil
import sys
import sysconfig
from pathlib import Path

wheel = Path(sys.sys.argv[1])
dest_dir = Path(sys.sys.argv[2])

wheel_target = dest_dir.joinpath(wheel.name)

platform = sysconfig.get_platform()

if platform.startswith("macosx") and platform.endswith("arm64") and wheel.endswith("x86_64.whl"):
    wheel_name = wheel.name
    prefix = wheel_name.rsplit("-", 1)[0]
    new_wheel_name = f"{prefix}-{platform}.whl"
    wheel_target = dest_dir.joinpath(new_wheel_name)

shutil.copyfile(wheel, wheel_target)
