from __future__ import annotations

import re

from daft.datatype import DataType
from daft.functions import uuid


def test_uuid_column_generation(make_df) -> None:
    data = {"a": list(range(200))}
    df = make_df(data).with_column("uuid", uuid()).collect()

    assert len(df) == 200
    assert df.schema()["uuid"].dtype == DataType.string()

    values = df.to_pydict()["uuid"]
    assert len(set(values)) == 200
    uuid_re = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
    assert all(isinstance(v, str) and uuid_re.match(v) is not None for v in values)


def test_uuid_empty_table(make_df) -> None:
    data = {"a": []}
    df = make_df(data).with_column("uuid", uuid()).collect()
    assert len(df) == 0
    assert df.schema()["uuid"].dtype == DataType.string()
    assert df.to_pydict()["uuid"] == []

