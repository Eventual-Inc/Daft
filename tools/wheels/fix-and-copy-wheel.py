import shutil
import sys
import sysconfig

wheel = sys.sys.argv[1]
dest_dir = sys.sys.argv[2]

platform = sysconfig.get_platform()

if platform == "macosx-11.0-arm64" and wheel.endswith("x86_64.whl"):
    prefix = wheel.rsplit("-", 1)[0]
    new_wheel_name = f"{prefix}-{platform}.whl"
    new_wheel_name = new_wheel_name.rsplit("/", 1)[1]
    shutil.copyfile(wheel, new_wheel_name)
else:
    pass
    # shutil.copyfile(wheel, new_wheel_name)
