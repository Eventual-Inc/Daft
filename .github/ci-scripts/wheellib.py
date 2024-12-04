from packaging.utils import parse_wheel_filename


def get_platform_tag(wheelname: str) -> str:
    distribution, version, build_tag, tags = parse_wheel_filename(wheelname)
    assert len(tags) == 1, "Multiple tags found"
    return next(iter(tags)).platform
