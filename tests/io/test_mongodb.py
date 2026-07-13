from __future__ import annotations

import datetime
import json

import pytest

import daft
import daft.io._mongodb as mongodb_io
from daft.daft import IOConfig, PyPushdowns, StorageConfig
from daft.io._mongodb import MongoScanOperator, _json_dumps_document, _json_dumps_hint, _json_dumps_value, read_mongodb


@pytest.mark.parametrize(
    ("kwargs", "exc", "message"),
    [
        ({"uri": ""}, ValueError, "uri must be non-empty"),
        ({"database": ""}, ValueError, "database must be non-empty"),
        ({"collection": ""}, ValueError, "collection must be non-empty"),
        ({"uri": 1}, TypeError, "uri must be a string"),
    ],
)
def test_read_mongodb_rejects_invalid_location_arguments(kwargs, exc, message):
    args = {
        "uri": "mongodb://example.invalid:27017",
        "database": "db",
        "collection": "events",
    }
    args.update(kwargs)
    with pytest.raises(exc, match=message):
        read_mongodb(
            args["uri"],
            args["database"],
            args["collection"],
            {"_id": daft.DataType.string()},
        )


def test_read_mongodb_requires_partition_field_with_ranges():
    with pytest.raises(ValueError, match="partition_field is required"):
        read_mongodb(
            "mongodb://example.invalid:27017",
            "db",
            "events",
            {"_id": daft.DataType.string()},
            partition_ranges=[(0, 10)],
        )


@pytest.mark.parametrize(
    ("partition_field", "exc", "message"),
    [
        ("", ValueError, "partition_field must be non-empty"),
        (1, TypeError, "partition_field must be a string"),
    ],
)
def test_read_mongodb_rejects_invalid_partition_field(partition_field, exc, message):
    with pytest.raises(exc, match=message):
        read_mongodb(
            "mongodb://example.invalid:27017",
            "db",
            "events",
            {"value": daft.DataType.int64()},
            partition_field=partition_field,
            partition_ranges=[(0, 10)],
        )


def test_read_mongodb_allows_partition_field_outside_output_schema():
    df = read_mongodb(
        "mongodb://example.invalid:27017",
        "db",
        "events",
        {"value": daft.DataType.int64()},
        partition_field="_id",
        partition_ranges=[(0, 10)],
    )

    assert df.schema().column_names() == ["value"]


@pytest.mark.parametrize(
    ("max_time_ms", "message"),
    [
        (0, "max_time_ms must be > 0"),
        (2**64, "max_time_ms must fit in u64"),
    ],
)
def test_read_mongodb_rejects_invalid_max_time(max_time_ms, message):
    with pytest.raises(ValueError, match=message):
        read_mongodb(
            "mongodb://example.invalid:27017",
            "db",
            "events",
            {"_id": daft.DataType.string()},
            max_time_ms=max_time_ms,
        )


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"batch_size": True}, "batch_size must be an integer"),
        ({"batch_size": "100"}, "batch_size must be an integer"),
        ({"max_time_ms": True}, "max_time_ms must be an integer"),
        ({"max_time_ms": "1000"}, "max_time_ms must be an integer"),
    ],
)
def test_read_mongodb_rejects_non_integer_numeric_options(kwargs, message):
    with pytest.raises(TypeError, match=message):
        read_mongodb(
            "mongodb://example.invalid:27017",
            "db",
            "events",
            {"_id": daft.DataType.string()},
            **kwargs,
        )


def test_read_mongodb_rejects_oversized_batch_size():
    with pytest.raises(ValueError, match="batch_size must fit in u32"):
        read_mongodb(
            "mongodb://example.invalid:27017",
            "db",
            "events",
            {"_id": daft.DataType.string()},
            batch_size=2**32,
        )


def test_read_mongodb_rejects_mixed_projection_modes():
    with pytest.raises(ValueError, match="cannot mix inclusion and exclusion"):
        read_mongodb(
            "mongodb://example.invalid:27017",
            "db",
            "events",
            {"included": daft.DataType.string(), "excluded": daft.DataType.string()},
            projection={"included": 1, "excluded": 0},
        )


def test_read_mongodb_preserves_compound_hint_key_order():
    assert _json_dumps_hint({"status": 1, "created_at": 1}) == '{"status":1,"created_at":1}'


def test_mongodb_json_helpers_encode_dates_as_extended_json():
    assert json.loads(_json_dumps_document({"created_at": datetime.datetime(1970, 1, 1, 0, 0, 0, 1)})) == {
        "created_at": {"$date": {"$numberLong": "0"}}
    }
    assert json.loads(
        _json_dumps_value(
            datetime.datetime(
                1970,
                1,
                1,
                5,
                30,
                tzinfo=datetime.timezone(datetime.timedelta(hours=5, minutes=30)),
            )
        )
    ) == {"$date": {"$numberLong": "0"}}
    assert json.loads(_json_dumps_value(datetime.date(1970, 1, 2))) == {"$date": {"$numberLong": "86400000"}}
    assert json.loads(
        _json_dumps_value(datetime.datetime(1969, 12, 31, 23, 59, 59, 999999, tzinfo=datetime.timezone.utc))
    ) == {"$date": {"$numberLong": "-1"}}


def test_mongodb_json_helpers_accept_canonical_binary_extended_json():
    assert json.loads(_json_dumps_value({"$binary": {"base64": "aGVsbG8=", "subType": "00"}})) == {
        "$binary": {"base64": "aGVsbG8=", "subType": "00"}
    }


def test_mongodb_json_helpers_reject_non_string_keys():
    with pytest.raises(TypeError, match="keys must be strings"):
        _json_dumps_document({1: "bad"})


@pytest.mark.parametrize("value", [float("inf"), float("-inf"), float("nan")])
def test_mongodb_json_helpers_reject_nonfinite_values(value):
    with pytest.raises(ValueError, match="finite"):
        _json_dumps_value(value)


def test_mongodb_json_helpers_reject_oversized_integers():
    with pytest.raises(ValueError, match="fit in i64"):
        _json_dumps_value(2**63)


@pytest.mark.parametrize(
    ("value", "exc", "message"),
    [
        ({"$oid": "not-an-object-id"}, ValueError, r"\$oid value must be a 24-character hex string"),
        ({"$oid": 1}, TypeError, r"\$oid extended JSON value must be a string"),
        ({"$oid": "000000000000000000000000", "extra": 1}, ValueError, r"\$oid extended JSON object"),
        ({"$numberLong": str(2**63)}, ValueError, r"\$numberLong value is outside"),
        ({"$numberLong": "1", "extra": 1}, ValueError, r"\$numberLong extended JSON object"),
        ({"$numberInt": str(2**31)}, ValueError, r"\$numberInt value is outside"),
        ({"$numberInt": "1", "extra": 1}, ValueError, r"\$numberInt extended JSON object"),
        ({"$numberDouble": "NaN"}, ValueError, r"\$numberDouble value must be finite"),
        ({"$numberDouble": "1.0", "extra": 1}, ValueError, r"\$numberDouble extended JSON object"),
        ({"$date": "2024-01-01T00:00:00Z"}, TypeError, r"\$date value must be"),
        ({"$date": 0, "extra": 1}, ValueError, r"\$date extended JSON object"),
        ({"$date": {"$numberLong": str(2**63)}}, ValueError, r"\$date \$numberLong value is outside"),
        ({"$date": {"$numberLong": "0", "extra": 1}}, ValueError, r"\$date object must only"),
        ({"$binary": {"base64": "not base64", "subType": "00"}}, ValueError, r"\$binary base64"),
        (
            {"$binary": {"base64": "aGVsbG8=", "subType": "00"}, "extra": 1},
            ValueError,
            r"\$binary extended JSON object",
        ),
        ({"$binary": {"base64": "aGVsbG8=", "subType": "0"}}, ValueError, r"\$binary subType"),
        (
            {"$binary": {"base64": "aGVsbG8=", "subType": "00", "extra": 1}},
            ValueError,
            r"\$binary object must only",
        ),
    ],
)
def test_mongodb_json_helpers_reject_invalid_extended_json(value, exc, message):
    with pytest.raises(exc, match=message):
        _json_dumps_value(value)


def test_read_mongodb_rejects_malformed_partition_ranges():
    with pytest.raises(TypeError, match="partition_ranges must contain"):
        read_mongodb(
            "mongodb://example.invalid:27017",
            "db",
            "events",
            {"_id": daft.DataType.string()},
            partition_field="_id",
            partition_ranges=["not-a-range"],
        )


def test_read_mongodb_rejects_unserializable_partition_bounds():
    with pytest.raises(TypeError, match="bytes values"):
        read_mongodb(
            "mongodb://example.invalid:27017",
            "db",
            "events",
            {"_id": daft.DataType.string()},
            partition_field="_id",
            partition_ranges=[(b"lower", b"upper")],
        )


def test_read_mongodb_rejects_nonfinite_partition_bounds():
    with pytest.raises(ValueError, match="finite"):
        read_mongodb(
            "mongodb://example.invalid:27017",
            "db",
            "events",
            {"value": daft.DataType.float64()},
            partition_field="value",
            partition_ranges=[(0.0, float("inf"))],
        )


@pytest.mark.parametrize("partition_range", [(10, 10), (20, 10)])
def test_read_mongodb_rejects_empty_or_reversed_partition_ranges(partition_range):
    with pytest.raises(ValueError, match="lower < upper"):
        read_mongodb(
            "mongodb://example.invalid:27017",
            "db",
            "events",
            {"_id": daft.DataType.string(), "value": daft.DataType.int64()},
            partition_field="value",
            partition_ranges=[partition_range],
        )


def test_read_mongodb_rejects_empty_datetime_partition_range():
    with pytest.raises(ValueError, match="lower < upper"):
        read_mongodb(
            "mongodb://example.invalid:27017",
            "db",
            "events",
            {"created_at": daft.DataType.timestamp("ms")},
            partition_field="created_at",
            partition_ranges=[(datetime.date(2024, 1, 1), datetime.date(2024, 1, 1))],
        )


def test_mongodb_scan_operator_plans_one_task_per_explicit_range():
    schema = daft.Schema.from_pydict({"_id": daft.DataType.string(), "value": daft.DataType.int64()})
    operator = MongoScanOperator(
        uri="mongodb://example.invalid:27017",
        database="db",
        collection="events",
        schema=schema,
        filter_json='{"kind":"click"}',
        projection_json=None,
        hint_json='{"_id":1}',
        max_time_ms=10_000,
        partition_field="_id",
        partition_ranges=[
            ({"$oid": "000000000000000000000000"}, {"$oid": "100000000000000000000000"}),
            ({"$oid": "100000000000000000000000"}, {"$oid": "200000000000000000000000"}),
        ],
        batch_size=128,
        storage_config=StorageConfig(True, IOConfig()),
    )

    tasks = list(operator.to_scan_tasks(PyPushdowns(columns=["_id"])))

    assert len(tasks) == 2
    assert operator.partitioning_keys() == []
    assert operator.can_absorb_limit() is False
    assert operator._hint_json == '{"_id":1}'
    assert operator._max_time_ms == 10_000


def test_mongodb_scan_operator_respects_empty_explicit_ranges():
    schema = daft.Schema.from_pydict({"_id": daft.DataType.string()})
    operator = MongoScanOperator(
        uri="mongodb://example.invalid:27017",
        database="db",
        collection="events",
        schema=schema,
        filter_json=None,
        projection_json=None,
        hint_json=None,
        max_time_ms=None,
        partition_field="_id",
        partition_ranges=[],
        batch_size=128,
        storage_config=StorageConfig(True, IOConfig()),
    )

    assert list(operator.to_scan_tasks(PyPushdowns(columns=["_id"]))) == []


def test_mongodb_scan_operator_strips_limit_for_multiple_scan_tasks(monkeypatch):
    schema = daft.Schema.from_pydict({"_id": daft.DataType.string()})
    operator = MongoScanOperator(
        uri="mongodb://example.invalid:27017",
        database="db",
        collection="events",
        schema=schema,
        filter_json=None,
        projection_json=None,
        hint_json=None,
        max_time_ms=None,
        partition_field="_id",
        partition_ranges=[
            ({"$oid": "000000000000000000000000"}, {"$oid": "100000000000000000000000"}),
            ({"$oid": "100000000000000000000000"}, {"$oid": "200000000000000000000000"}),
        ],
        batch_size=128,
        storage_config=StorageConfig(True, IOConfig()),
    )
    captured_pushdowns = []

    class FakeScanTask:
        @staticmethod
        def mongo_scan_task(**kwargs):
            captured_pushdowns.append(kwargs["pushdowns"])
            return kwargs

    monkeypatch.setattr(mongodb_io, "ScanTask", FakeScanTask)

    tasks = list(
        operator.to_scan_tasks(
            PyPushdowns(
                columns=["_id"],
                filters=(daft.col("_id") == "abc")._expr,
                partition_filters=(daft.col("_id") == "abc")._expr,
                limit=5,
            )
        )
    )

    assert len(tasks) == 2
    assert [pushdowns.limit for pushdowns in captured_pushdowns] == [None, None]
    assert [pushdowns.columns for pushdowns in captured_pushdowns] == [["_id"], ["_id"]]
    assert [pushdowns.filters for pushdowns in captured_pushdowns] == [None, None]
    assert [pushdowns.partition_filters for pushdowns in captured_pushdowns] == [None, None]


def test_mongodb_scan_operator_preserves_limit_for_single_remaining_scan_task(monkeypatch):
    schema = daft.Schema.from_pydict({"_id": daft.DataType.string()})
    operator = MongoScanOperator(
        uri="mongodb://example.invalid:27017",
        database="db",
        collection="events",
        schema=schema,
        filter_json='{"_id":{"$gte":{"$oid":"100000000000000000000000"}}}',
        projection_json=None,
        hint_json=None,
        max_time_ms=None,
        partition_field="_id",
        partition_ranges=[
            ({"$oid": "000000000000000000000000"}, {"$oid": "100000000000000000000000"}),
            ({"$oid": "100000000000000000000000"}, {"$oid": "200000000000000000000000"}),
        ],
        batch_size=128,
        storage_config=StorageConfig(True, IOConfig()),
    )
    captured_pushdowns = []

    class FakeScanTask:
        @staticmethod
        def mongo_scan_task(**kwargs):
            captured_pushdowns.append(kwargs["pushdowns"])
            return kwargs

    monkeypatch.setattr(mongodb_io, "ScanTask", FakeScanTask)

    tasks = list(
        operator.to_scan_tasks(
            PyPushdowns(
                columns=["_id"],
                filters=(daft.col("_id") >= "abc")._expr,
                limit=5,
            )
        )
    )

    assert len(tasks) == 1
    assert captured_pushdowns[0].limit == 5
    assert captured_pushdowns[0].columns == ["_id"]
    assert captured_pushdowns[0].filters is None


def test_mongodb_scan_operator_absorbs_generic_filter_pushdown_when_strict_disabled(monkeypatch):
    schema = daft.Schema.from_pydict({"kind": daft.DataType.string(), "value": daft.DataType.int64()})
    operator = MongoScanOperator(
        uri="mongodb://example.invalid:27017",
        database="db",
        collection="events",
        schema=schema,
        filter_json='{"kind":"click"}',
        projection_json=None,
        hint_json=None,
        max_time_ms=None,
        partition_field=None,
        partition_ranges=None,
        batch_size=128,
        storage_config=StorageConfig(True, IOConfig()),
    )

    class FakeScanTask:
        @staticmethod
        def mongo_scan_task(**kwargs):
            return kwargs

    monkeypatch.setattr(mongodb_io, "ScanTask", FakeScanTask)

    tasks = list(
        operator.to_scan_tasks(
            PyPushdowns(
                columns=["value"],
                filters=(daft.col("value") >= 10)._expr,
            )
        )
    )

    assert len(tasks) == 1
    assert json.loads(tasks[0]["config"].filter_json) == {
        "$and": [
            {"kind": "click"},
            {"value": {"$gte": 10}},
        ]
    }
    assert tasks[0]["pushdowns"].filters is None


def test_mongodb_scan_operator_does_not_retain_generic_filter_between_planning_calls(monkeypatch):
    schema = daft.Schema.from_pydict({"value": daft.DataType.int64()})
    operator = MongoScanOperator(
        uri="mongodb://example.invalid:27017",
        database="db",
        collection="events",
        schema=schema,
        filter_json=None,
        projection_json=None,
        hint_json=None,
        max_time_ms=None,
        partition_field=None,
        partition_ranges=None,
        batch_size=128,
        storage_config=StorageConfig(True, IOConfig()),
    )

    class FakeScanTask:
        @staticmethod
        def mongo_scan_task(**kwargs):
            return kwargs

    monkeypatch.setattr(mongodb_io, "ScanTask", FakeScanTask)

    first_tasks = list(operator.to_scan_tasks(PyPushdowns(filters=(daft.col("value") >= 10)._expr)))
    second_tasks = list(operator.to_scan_tasks(PyPushdowns(filters=(daft.col("value") < 5)._expr)))
    unfiltered_tasks = list(operator.to_scan_tasks(PyPushdowns()))

    assert json.loads(first_tasks[0]["config"].filter_json) == {"value": {"$gte": 10}}
    assert json.loads(second_tasks[0]["config"].filter_json) == {"value": {"$lt": 5}}
    assert unfiltered_tasks[0]["config"].filter_json is None


def test_mongodb_scan_operator_prunes_ranges_from_generic_filter_pushdown_when_strict_disabled(monkeypatch):
    schema = daft.Schema.from_pydict({"value": daft.DataType.int64()})
    operator = MongoScanOperator(
        uri="mongodb://example.invalid:27017",
        database="db",
        collection="events",
        schema=schema,
        filter_json=None,
        projection_json=None,
        hint_json=None,
        max_time_ms=None,
        partition_field="value",
        partition_ranges=[(0, 10), (10, 20), (20, 30)],
        batch_size=128,
        storage_config=StorageConfig(True, IOConfig()),
    )

    class FakeScanTask:
        @staticmethod
        def mongo_scan_task(**kwargs):
            return kwargs

    monkeypatch.setattr(mongodb_io, "ScanTask", FakeScanTask)

    tasks = list(operator.to_scan_tasks(PyPushdowns(filters=(daft.col("value") >= 15)._expr)))

    assert len(tasks) == 2
    assert [task["config"].partition_lower_json for task in tasks] == ["10", "20"]
    assert [task["config"].partition_upper_json for task in tasks] == ["20", "30"]
    assert json.loads(tasks[0]["config"].filter_json) == {"value": {"$gte": 15}}


def test_mongodb_scan_operator_rejects_unsupported_generic_filter_pushdown():
    schema = daft.Schema.from_pydict({"value": daft.DataType.int64()})
    operator = MongoScanOperator(
        uri="mongodb://example.invalid:27017",
        database="db",
        collection="events",
        schema=schema,
        filter_json=None,
        projection_json=None,
        hint_json=None,
        max_time_ms=None,
        partition_field=None,
        partition_ranges=None,
        batch_size=128,
        storage_config=StorageConfig(True, IOConfig()),
    )

    with pytest.raises(ValueError, match="cannot be represented as a MongoDB filter"):
        list(operator.to_scan_tasks(PyPushdowns(filters=((daft.col("value") + 1) > 10)._expr)))


def test_mongodb_scan_operator_pushes_supported_filters_and_keeps_residuals():
    schema = daft.Schema.from_pydict({"kind": daft.DataType.string(), "value": daft.DataType.int64()})
    operator = MongoScanOperator(
        uri="mongodb://example.invalid:27017",
        database="db",
        collection="events",
        schema=schema,
        filter_json='{"kind":"click"}',
        projection_json=None,
        hint_json=None,
        max_time_ms=None,
        partition_field=None,
        partition_ranges=None,
        batch_size=128,
        storage_config=StorageConfig(True, IOConfig()),
    )

    pushed, remaining = operator.push_filters(
        [
            (daft.col("value") >= 10)._expr,
            ((daft.col("value") + 1) > 10)._expr,
        ]
    )

    assert len(pushed) == 1
    assert len(remaining) == 1
    assert operator.can_absorb_filter() is True
    assert operator.can_absorb_limit() is True
    assert json.loads(operator._combined_filter_json()) == {
        "$and": [
            {"kind": "click"},
            {"value": {"$gte": 10}},
        ]
    }


def test_mongodb_scan_operator_partially_pushes_supported_conjunction():
    schema = daft.Schema.from_pydict({"value": daft.DataType.int64()})
    operator = MongoScanOperator(
        uri="mongodb://example.invalid:27017",
        database="db",
        collection="events",
        schema=schema,
        filter_json=None,
        projection_json=None,
        hint_json=None,
        max_time_ms=None,
        partition_field=None,
        partition_ranges=None,
        batch_size=128,
        storage_config=StorageConfig(True, IOConfig()),
    )

    pushed, remaining = operator.push_filters([((daft.col("value") >= 10) & ((daft.col("value") + 1) > 10))._expr])

    assert len(pushed) == 1
    assert len(remaining) == 1
    assert json.loads(operator._combined_filter_json()) == {"value": {"$gte": 10}}


def test_mongodb_scan_operator_preserves_pushed_filter_across_later_calls():
    schema = daft.Schema.from_pydict({"value": daft.DataType.int64()})
    operator = MongoScanOperator(
        uri="mongodb://example.invalid:27017",
        database="db",
        collection="events",
        schema=schema,
        filter_json=None,
        projection_json=None,
        hint_json=None,
        max_time_ms=None,
        partition_field=None,
        partition_ranges=None,
        batch_size=128,
        storage_config=StorageConfig(True, IOConfig()),
    )
    pushed_expr = (daft.col("value") >= 10)._expr
    residual_expr = ((daft.col("value") + 1) > 10)._expr

    operator.push_filters([pushed_expr])
    operator.push_filters([pushed_expr])
    operator.push_filters([residual_expr])

    assert json.loads(operator._combined_filter_json()) == {"value": {"$gte": 10}}


def test_mongodb_scan_operator_dedupes_overlapping_pushed_filters():
    schema = daft.Schema.from_pydict({"value": daft.DataType.int64()})
    operator = MongoScanOperator(
        uri="mongodb://example.invalid:27017",
        database="db",
        collection="events",
        schema=schema,
        filter_json=None,
        projection_json=None,
        hint_json=None,
        max_time_ms=None,
        partition_field=None,
        partition_ranges=None,
        batch_size=128,
        storage_config=StorageConfig(True, IOConfig()),
    )

    operator.push_filters([(daft.col("value") >= 10)._expr])
    operator.push_filters([((daft.col("value") >= 10) & (daft.col("value") < 20))._expr])

    assert json.loads(operator._combined_filter_json()) == {"$and": [{"value": {"$gte": 10}}, {"value": {"$lt": 20}}]}


def test_mongodb_scan_operator_dedupes_user_and_pushed_filters():
    schema = daft.Schema.from_pydict({"value": daft.DataType.int64()})
    operator = MongoScanOperator(
        uri="mongodb://example.invalid:27017",
        database="db",
        collection="events",
        schema=schema,
        filter_json='{"value":{"$gte":10}}',
        projection_json=None,
        hint_json=None,
        max_time_ms=None,
        partition_field=None,
        partition_ranges=None,
        batch_size=128,
        storage_config=StorageConfig(True, IOConfig()),
    )

    operator.push_filters([((daft.col("value") >= 10) & (daft.col("value") < 20))._expr])

    assert json.loads(operator._combined_filter_json()) == {"$and": [{"value": {"$gte": 10}}, {"value": {"$lt": 20}}]}


def test_mongodb_scan_operator_elides_empty_user_filter_when_combining():
    schema = daft.Schema.from_pydict({"value": daft.DataType.int64()})
    operator = MongoScanOperator(
        uri="mongodb://example.invalid:27017",
        database="db",
        collection="events",
        schema=schema,
        filter_json="{}",
        projection_json=None,
        hint_json=None,
        max_time_ms=None,
        partition_field=None,
        partition_ranges=None,
        batch_size=128,
        storage_config=StorageConfig(True, IOConfig()),
    )

    assert operator._combined_filter_json() is None

    operator.push_filters([(daft.col("value") < 20)._expr])

    assert json.loads(operator._combined_filter_json()) == {"value": {"$lt": 20}}


def test_mongodb_scan_operator_preserves_malformed_user_and_filter_when_combining():
    schema = daft.Schema.from_pydict({"value": daft.DataType.int64()})
    operator = MongoScanOperator(
        uri="mongodb://example.invalid:27017",
        database="db",
        collection="events",
        schema=schema,
        filter_json='{"$and":[{"value":{"$gte":10}},5]}',
        projection_json=None,
        hint_json=None,
        max_time_ms=None,
        partition_field=None,
        partition_ranges=None,
        batch_size=128,
        storage_config=StorageConfig(True, IOConfig()),
    )

    operator.push_filters([(daft.col("value") < 20)._expr])

    assert json.loads(operator._combined_filter_json()) == {
        "$and": [
            {"$and": [{"value": {"$gte": 10}}, 5]},
            {"value": {"$lt": 20}},
        ]
    }


def test_mongodb_scan_operator_preserves_empty_and_operator_when_combining():
    schema = daft.Schema.from_pydict({"value": daft.DataType.int64()})
    operator = MongoScanOperator(
        uri="mongodb://example.invalid:27017",
        database="db",
        collection="events",
        schema=schema,
        filter_json='{"$and":[]}',
        projection_json=None,
        hint_json=None,
        max_time_ms=None,
        partition_field=None,
        partition_ranges=None,
        batch_size=128,
        storage_config=StorageConfig(True, IOConfig()),
    )

    operator.push_filters([(daft.col("value") < 20)._expr])

    assert json.loads(operator._combined_filter_json()) == {"$and": [{"$and": []}, {"value": {"$lt": 20}}]}


def test_mongodb_scan_operator_pushes_null_safe_not_equal_shape():
    schema = daft.Schema.from_pydict({"value": daft.DataType.int64()})
    operator = MongoScanOperator(
        uri="mongodb://example.invalid:27017",
        database="db",
        collection="events",
        schema=schema,
        filter_json=None,
        projection_json=None,
        hint_json=None,
        max_time_ms=None,
        partition_field=None,
        partition_ranges=None,
        batch_size=128,
        storage_config=StorageConfig(True, IOConfig()),
    )

    pushed, remaining = operator.push_filters([(daft.col("value") != 10)._expr])

    assert len(pushed) == 1
    assert remaining == []
    assert json.loads(operator._combined_filter_json()) == {
        "$and": [
            {"value": {"$exists": True}},
            {"value": {"$ne": None}},
            {"value": {"$ne": 10}},
        ]
    }


def test_mongodb_scan_operator_prunes_numeric_partition_ranges_from_pushed_filter():
    schema = daft.Schema.from_pydict({"value": daft.DataType.int64()})
    operator = MongoScanOperator(
        uri="mongodb://example.invalid:27017",
        database="db",
        collection="events",
        schema=schema,
        filter_json=None,
        projection_json=None,
        hint_json=None,
        max_time_ms=None,
        partition_field="value",
        partition_ranges=[(0, 10), (10, 20), (20, 30)],
        batch_size=128,
        storage_config=StorageConfig(True, IOConfig()),
    )
    operator.push_filters([((daft.col("value") >= 15) & (daft.col("value") < 22))._expr])

    assert operator._partition_ranges_for_filter(operator._combined_filter_json()) == [
        (10, 20),
        (20, 30),
    ]


def test_mongodb_scan_operator_prunes_all_ranges_for_impossible_numeric_filter():
    schema = daft.Schema.from_pydict({"value": daft.DataType.int64()})
    operator = MongoScanOperator(
        uri="mongodb://example.invalid:27017",
        database="db",
        collection="events",
        schema=schema,
        filter_json='{"value":{"$gte":20,"$lt":10}}',
        projection_json=None,
        hint_json=None,
        max_time_ms=None,
        partition_field="value",
        partition_ranges=[(0, 10), (10, 20), (20, 30)],
        batch_size=128,
        storage_config=StorageConfig(True, IOConfig()),
    )

    assert operator._partition_ranges_for_filter(operator._combined_filter_json()) == []


@pytest.mark.parametrize(
    ("filter_json", "expected_ranges"),
    [
        ('{"value":{"$lt":10}}', [(0, 10)]),
        ('{"value":{"$lte":10}}', [(0, 10), (10, 20)]),
        ('{"value":{"$gt":10}}', [(10, 20), (20, 30)]),
        ('{"value":{"$gte":10}}', [(10, 20), (20, 30)]),
    ],
)
def test_mongodb_scan_operator_prunes_numeric_half_open_boundaries(filter_json, expected_ranges):
    schema = daft.Schema.from_pydict({"value": daft.DataType.int64()})
    operator = MongoScanOperator(
        uri="mongodb://example.invalid:27017",
        database="db",
        collection="events",
        schema=schema,
        filter_json=filter_json,
        projection_json=None,
        hint_json=None,
        max_time_ms=None,
        partition_field="value",
        partition_ranges=[(0, 10), (10, 20), (20, 30)],
        batch_size=128,
        storage_config=StorageConfig(True, IOConfig()),
    )

    assert operator._partition_ranges_for_filter(operator._combined_filter_json()) == expected_ranges


def test_mongodb_scan_operator_prunes_datetime_partition_ranges_from_user_filter():
    schema = daft.Schema.from_pydict({"created_at": daft.DataType.timestamp("ms")})
    operator = MongoScanOperator(
        uri="mongodb://example.invalid:27017",
        database="db",
        collection="events",
        schema=schema,
        filter_json=_json_dumps_document({"created_at": {"$gte": datetime.date(2024, 2, 1)}}),
        projection_json=None,
        hint_json=None,
        max_time_ms=None,
        partition_field="created_at",
        partition_ranges=[
            (datetime.date(2024, 1, 1), datetime.date(2024, 2, 1)),
            (datetime.date(2024, 2, 1), datetime.date(2024, 3, 1)),
            (datetime.date(2024, 3, 1), datetime.date(2024, 4, 1)),
        ],
        batch_size=128,
        storage_config=StorageConfig(True, IOConfig()),
    )

    assert operator._partition_ranges_for_filter(operator._combined_filter_json()) == [
        (datetime.date(2024, 2, 1), datetime.date(2024, 3, 1)),
        (datetime.date(2024, 3, 1), datetime.date(2024, 4, 1)),
    ]


def test_mongodb_scan_operator_rejects_boolean_extended_json_date_filter():
    schema = daft.Schema.from_pydict({"created_at": daft.DataType.timestamp("ms")})
    partition_ranges = [
        (datetime.date(2024, 1, 1), datetime.date(2024, 2, 1)),
        (datetime.date(2024, 2, 1), datetime.date(2024, 3, 1)),
    ]
    operator = MongoScanOperator(
        uri="mongodb://example.invalid:27017",
        database="db",
        collection="events",
        schema=schema,
        filter_json='{"created_at":{"$date":true}}',
        projection_json=None,
        hint_json=None,
        max_time_ms=None,
        partition_field="created_at",
        partition_ranges=partition_ranges,
        batch_size=128,
        storage_config=StorageConfig(True, IOConfig()),
    )

    with pytest.raises(TypeError, match=r"\$date milliseconds must be an integer"):
        operator._combined_filter_json()


def test_mongodb_scan_operator_prunes_partition_ranges_from_or_filter():
    schema = daft.Schema.from_pydict({"value": daft.DataType.int64()})
    operator = MongoScanOperator(
        uri="mongodb://example.invalid:27017",
        database="db",
        collection="events",
        schema=schema,
        filter_json='{"$or":[{"value":{"$lt":5}},{"value":{"$gte":25}}]}',
        projection_json=None,
        hint_json=None,
        max_time_ms=None,
        partition_field="value",
        partition_ranges=[(0, 10), (10, 20), (20, 30)],
        batch_size=128,
        storage_config=StorageConfig(True, IOConfig()),
    )

    assert operator._partition_ranges_for_filter(operator._combined_filter_json()) == [
        (0, 10),
        (20, 30),
    ]


def test_mongodb_scan_operator_keeps_all_ranges_for_opaque_or_branch():
    schema = daft.Schema.from_pydict({"other": daft.DataType.int64(), "value": daft.DataType.int64()})
    operator = MongoScanOperator(
        uri="mongodb://example.invalid:27017",
        database="db",
        collection="events",
        schema=schema,
        filter_json='{"$or":[{"other":1},{"value":{"$gte":25}}]}',
        projection_json=None,
        hint_json=None,
        max_time_ms=None,
        partition_field="value",
        partition_ranges=[(0, 10), (10, 20), (20, 30)],
        batch_size=128,
        storage_config=StorageConfig(True, IOConfig()),
    )

    assert operator._partition_ranges_for_filter(operator._combined_filter_json()) == [
        (0, 10),
        (10, 20),
        (20, 30),
    ]


def test_mongodb_scan_operator_does_not_prune_on_partially_opaque_in_values():
    schema = daft.Schema.from_pydict({"value": daft.DataType.int64()})
    operator = MongoScanOperator(
        uri="mongodb://example.invalid:27017",
        database="db",
        collection="events",
        schema=schema,
        filter_json='{"value":{"$in":[5,{"nested":1}]}}',
        projection_json=None,
        hint_json=None,
        max_time_ms=None,
        partition_field="value",
        partition_ranges=[(0, 10), (10, 20), (20, 30)],
        batch_size=128,
        storage_config=StorageConfig(True, IOConfig()),
    )

    assert operator._partition_ranges_for_filter(operator._combined_filter_json()) == [
        (0, 10),
        (10, 20),
        (20, 30),
    ]


def test_mongodb_scan_operator_does_not_prune_opaque_in_even_with_range():
    schema = daft.Schema.from_pydict({"value": daft.DataType.int64()})
    operator = MongoScanOperator(
        uri="mongodb://example.invalid:27017",
        database="db",
        collection="events",
        schema=schema,
        filter_json='{"value":{"$in":[{"nested":1}],"$gte":25}}',
        projection_json=None,
        hint_json=None,
        max_time_ms=None,
        partition_field="value",
        partition_ranges=[(0, 10), (10, 20), (20, 30)],
        batch_size=128,
        storage_config=StorageConfig(True, IOConfig()),
    )

    assert operator._partition_ranges_for_filter(operator._combined_filter_json()) == [
        (0, 10),
        (10, 20),
        (20, 30),
    ]


@pytest.mark.parametrize(
    "filter_json",
    [
        '{"value":{"$eq":{"nested":1},"$gte":25}}',
        '{"value":{"$in":"not-a-list","$gte":25}}',
    ],
)
def test_mongodb_scan_operator_does_not_prune_on_malformed_partition_operator(filter_json):
    schema = daft.Schema.from_pydict({"value": daft.DataType.int64()})
    operator = MongoScanOperator(
        uri="mongodb://example.invalid:27017",
        database="db",
        collection="events",
        schema=schema,
        filter_json=filter_json,
        projection_json=None,
        hint_json=None,
        max_time_ms=None,
        partition_field="value",
        partition_ranges=[(0, 10), (10, 20), (20, 30)],
        batch_size=128,
        storage_config=StorageConfig(True, IOConfig()),
    )

    assert operator._partition_ranges_for_filter(operator._combined_filter_json()) == [
        (0, 10),
        (10, 20),
        (20, 30),
    ]


def test_mongodb_scan_operator_rejects_out_of_range_extended_json_number():
    schema = daft.Schema.from_pydict({"value": daft.DataType.int64()})
    operator = MongoScanOperator(
        uri="mongodb://example.invalid:27017",
        database="db",
        collection="events",
        schema=schema,
        filter_json='{"value":{"$numberLong":"9223372036854775808"}}',
        projection_json=None,
        hint_json=None,
        max_time_ms=None,
        partition_field="value",
        partition_ranges=[(0, 10), (10, 20), (20, 30)],
        batch_size=128,
        storage_config=StorageConfig(True, IOConfig()),
    )

    with pytest.raises(ValueError, match=r"\$numberLong value is outside"):
        operator._combined_filter_json()


def test_mongodb_scan_operator_prunes_all_ranges_for_empty_in_filter():
    schema = daft.Schema.from_pydict({"value": daft.DataType.int64()})
    operator = MongoScanOperator(
        uri="mongodb://example.invalid:27017",
        database="db",
        collection="events",
        schema=schema,
        filter_json='{"value":{"$in":[]}}',
        projection_json=None,
        hint_json=None,
        max_time_ms=None,
        partition_field="value",
        partition_ranges=[(0, 10), (10, 20), (20, 30)],
        batch_size=128,
        storage_config=StorageConfig(True, IOConfig()),
    )

    assert operator._partition_ranges_for_filter(operator._combined_filter_json()) == []


def test_mongodb_scan_operator_prunes_object_id_partition_ranges_from_user_filter():
    schema = daft.Schema.from_pydict({"_id": daft.DataType.string()})
    operator = MongoScanOperator(
        uri="mongodb://example.invalid:27017",
        database="db",
        collection="events",
        schema=schema,
        filter_json='{"_id":{"$oid":"100000000000000000000000"}}',
        projection_json=None,
        hint_json=None,
        max_time_ms=None,
        partition_field="_id",
        partition_ranges=[
            ({"$oid": "000000000000000000000000"}, {"$oid": "100000000000000000000000"}),
            ({"$oid": "100000000000000000000000"}, {"$oid": "200000000000000000000000"}),
            ({"$oid": "200000000000000000000000"}, {"$oid": "300000000000000000000000"}),
        ],
        batch_size=128,
        storage_config=StorageConfig(True, IOConfig()),
    )

    assert operator._partition_ranges_for_filter(operator._combined_filter_json()) == [
        ({"$oid": "100000000000000000000000"}, {"$oid": "200000000000000000000000"})
    ]


def test_mongodb_scan_operator_prunes_object_id_ranges_case_insensitively():
    schema = daft.Schema.from_pydict({"_id": daft.DataType.string()})
    operator = MongoScanOperator(
        uri="mongodb://example.invalid:27017",
        database="db",
        collection="events",
        schema=schema,
        filter_json='{"_id":{"$oid":"ABCDEF000000000000000000"}}',
        projection_json=None,
        hint_json=None,
        max_time_ms=None,
        partition_field="_id",
        partition_ranges=[
            ({"$oid": "000000000000000000000000"}, {"$oid": "abcdef000000000000000000"}),
            ({"$oid": "abcdef000000000000000000"}, {"$oid": "ffffffffffffffffffffffff"}),
        ],
        batch_size=128,
        storage_config=StorageConfig(True, IOConfig()),
    )

    assert operator._partition_ranges_for_filter(operator._combined_filter_json()) == [
        ({"$oid": "abcdef000000000000000000"}, {"$oid": "ffffffffffffffffffffffff"})
    ]


def test_mongodb_scan_operator_rejects_malformed_object_id_filter():
    schema = daft.Schema.from_pydict({"_id": daft.DataType.string()})
    operator = MongoScanOperator(
        uri="mongodb://example.invalid:27017",
        database="db",
        collection="events",
        schema=schema,
        filter_json='{"_id":{"$oid":"not-an-object-id"}}',
        projection_json=None,
        hint_json=None,
        max_time_ms=None,
        partition_field="_id",
        partition_ranges=[
            ({"$oid": "000000000000000000000000"}, {"$oid": "100000000000000000000000"}),
            ({"$oid": "100000000000000000000000"}, {"$oid": "200000000000000000000000"}),
        ],
        batch_size=128,
        storage_config=StorageConfig(True, IOConfig()),
    )

    with pytest.raises(ValueError, match=r"\$oid value must be a 24-character hex string"):
        operator._combined_filter_json()


@pytest.mark.parametrize(
    ("filter_json", "partition_field", "partition_ranges", "message"),
    [
        (
            '{"_id":{"$oid":"100000000000000000000000","extra":true}}',
            "_id",
            [
                ({"$oid": "000000000000000000000000"}, {"$oid": "100000000000000000000000"}),
                ({"$oid": "100000000000000000000000"}, {"$oid": "200000000000000000000000"}),
            ],
            r"\$oid extended JSON object",
        ),
        (
            '{"value":{"$numberLong":"15","extra":true}}',
            "value",
            [(0, 10), (10, 20)],
            r"\$numberLong extended JSON object",
        ),
        (
            '{"created_at":{"$date":{"$numberLong":"1704067200000","extra":true}}}',
            "created_at",
            [
                (
                    datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc),
                    datetime.datetime(2024, 2, 1, tzinfo=datetime.timezone.utc),
                ),
                (
                    datetime.datetime(2024, 2, 1, tzinfo=datetime.timezone.utc),
                    datetime.datetime(2024, 3, 1, tzinfo=datetime.timezone.utc),
                ),
            ],
            r"\$date object must only",
        ),
    ],
)
def test_mongodb_scan_operator_rejects_noncanonical_extended_json(
    filter_json, partition_field, partition_ranges, message
):
    schema = daft.Schema.from_pydict(
        {
            "_id": daft.DataType.string(),
            "value": daft.DataType.int64(),
            "created_at": daft.DataType.int64(),
        }
    )
    operator = MongoScanOperator(
        uri="mongodb://example.invalid:27017",
        database="db",
        collection="events",
        schema=schema,
        filter_json=filter_json,
        projection_json=None,
        hint_json=None,
        max_time_ms=None,
        partition_field=partition_field,
        partition_ranges=partition_ranges,
        batch_size=128,
        storage_config=StorageConfig(True, IOConfig()),
    )

    with pytest.raises(ValueError, match=message):
        operator._combined_filter_json()
