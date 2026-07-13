from __future__ import annotations

import datetime
import os
import uuid

import pytest

import daft


@pytest.mark.integration()
def test_read_mongodb_nested_smoke() -> None:
    uri = os.environ.get("DAFT_TEST_MONGODB_URI")
    if not uri:
        pytest.skip("Set DAFT_TEST_MONGODB_URI to run MongoDB integration smoke test")
    pymongo = pytest.importorskip("pymongo")

    db_name = f"daft_mongodb_smoke_{uuid.uuid4().hex}"
    collection_name = "orders"
    client = pymongo.MongoClient(uri)
    collection = client[db_name][collection_name]
    start = datetime.datetime(2025, 1, 1, tzinfo=datetime.timezone.utc)
    middle = datetime.datetime(2025, 1, 2, tzinfo=datetime.timezone.utc)
    end = datetime.datetime(2025, 1, 3, tzinfo=datetime.timezone.utc)

    try:
        collection.insert_many(
            [
                {
                    "updatedAt": start,
                    "profile": {"name": "Ada", "score": 7},
                    "tags": ["new", "paid"],
                    "events": [{"kind": "created", "at": start}],
                },
                {
                    "updatedAt": middle,
                    "profile": {"name": "Grace", "score": 9},
                    "tags": ["repeat"],
                    "events": [{"kind": "updated", "at": middle}],
                },
            ]
        )

        df = daft.read_mongodb(
            uri=uri,
            database=db_name,
            collection=collection_name,
            schema={
                "_id": daft.DataType.string(),
                "updatedAt": daft.DataType.timestamp("ms"),
                "profile": daft.DataType.struct(
                    {
                        "name": daft.DataType.string(),
                        "score": daft.DataType.int64(),
                    }
                ),
                "tags": daft.DataType.list(daft.DataType.string()),
                "events": daft.DataType.list(
                    daft.DataType.struct(
                        {
                            "kind": daft.DataType.string(),
                            "at": daft.DataType.timestamp("ms"),
                        }
                    )
                ),
            },
            partition_field="updatedAt",
            partition_ranges=[(start, middle), (middle, end)],
        )

        result = df.collect().to_pydict()

        assert sorted(result["profile"], key=lambda item: item["name"]) == [
            {"name": "Ada", "score": 7},
            {"name": "Grace", "score": 9},
        ]
        assert sorted(result["tags"]) == [["new", "paid"], ["repeat"]]
        assert len(result["_id"]) == 2
    finally:
        client.drop_database(db_name)
        client.close()
