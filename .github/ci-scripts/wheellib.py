from packaging.utils import parse_wheel_filename


def get_platform_tag(wheelname: str) -> str:
    _, _, _, tags = parse_wheel_filename(wheelname)
    assert len(tags) == 1, "Multiple tags found"
    return next(iter(tags)).platform
