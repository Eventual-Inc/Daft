from __future__ import annotations

from daft.filesystem import _resolve_paths_and_filesystem


def test_resolve_paths_strips_trailing_slash(tmp_path):
    # Regression: https://github.com/Eventual-Inc/Daft/issues/6978
    # A trailing "/" must be stripped at the resolver layer so it doesn't leak
    # into downstream "{dir}/{file}" joins as "{dir}//{file}".
    [resolved], _ = _resolve_paths_and_filesystem(f"{tmp_path}/")
    assert not resolved.endswith("/"), resolved
    [resolved_double], _ = _resolve_paths_and_filesystem(f"{tmp_path}//")
    assert not resolved_double.endswith("/"), resolved_double
