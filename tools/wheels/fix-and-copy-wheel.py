import shutil
import sys
import sysconfig
from pathlib import Path

from wheeltools import InWheelCtx, add_platforms

wheel = sys.argv[1]
dest_dir = sys.argv[2]

platform = sysconfig.get_platform()
wheel_name = Path(wheel).name
wheel_target = Path(dest_dir).joinpath(wheel_name).as_posix()

if platform.startswith("macosx") and platform.endswith("arm64") and wheel.endswith("x86_64.whl"):
    prefix = wheel_name.rsplit("-", 1)[0]
    new_wheel_name = f"{prefix}-{platform}.whl"
    wheel_target = Path(dest_dir).joinpath(new_wheel_name).as_posix()
    with InWheelCtx(wheel, wheel_target) as ctx:
        curr_platform = wheel.rsplit("-", 1)[1].strip(".whl")
        new_name = add_platforms(ctx, [platform], remove_platforms=[curr_platform])
else:
    shutil.copyfile(wheel, wheel_target)
