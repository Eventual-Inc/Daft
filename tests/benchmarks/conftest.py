from __future__ import annotations

import _pytest


# Monkeypatch to use comma delimiters when showing parameter lists.
# https://github.com/pytest-dev/pytest/blob/31d0b51039fc295dfb14bfc5d2baddebe11bb746/src/_pytest/python.py#L1190
# Related: https://github.com/pytest-dev/pytest/issues/3617
def id(self):
    return ":".join(self._idlist)


setattr(_pytest.python.CallSpec2, "id", property(id))


def pytest_make_parametrize_id(config, val, argname):
    return f"{argname}-{val}"
