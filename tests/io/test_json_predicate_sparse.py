from __future__ import annotations

import json
import daft


def test_ndjson_sparse_predicate_not_in_inferred_or_provided_schema(tmp_path):
    # Create NDJSON where predicate column 'x' is sparse and appears late
    lines = [
        {"y": "a"},
        {"y": "b"},
        {"y": "c"},
        {"x": 2, "y": "b"},
    ]
    p = tmp_path / "sparse.jsonl"
    p.write_text("\n".join(json.dumps(l) for l in lines))

    # Provide schema that only includes 'y'; 'x' is missing from provided and likely from inference
    provided_schema = {"y": daft.DataType.string()}

    # Read with include_columns=['y'] and predicate on sparse column 'x'
    df = daft.read_json(str(p), schema=provided_schema).where(daft.col("x") == 2).select("y")

    # Expect single row 'b' and ensure no FieldNotFound or silent misfiltering
    assert df.count_rows() == 1
    assert df.schema() == daft.Schema.from_pydict({"y": daft.DataType.string()})
    assert df.to_pylist() == [{"y": "b"}]
