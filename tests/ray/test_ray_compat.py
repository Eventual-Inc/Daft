from __future__ import annotations

import pytest

from daft.runners.ray_compat import parse_ray_version


@pytest.mark.parametrize(
    ("version", "expected"),
    [
        ("2.58.0", (2, 58, 0)),
        ("2.58.0+beta.0", (2, 58, 0)),
    ],
)
def test_parse_ray_version(version: str, expected: tuple[int, ...]) -> None:
    assert parse_ray_version(version) == expected
