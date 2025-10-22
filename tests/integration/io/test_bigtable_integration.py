from __future__ import annotations

from typing import Any

import pytest
from google.cloud.bigtable.table import Table

import daft
from daft.dataframe import DataFrame
from daft.functions import serialize
from tests.integration.io.conftest import bigtable_emulator_setup


@pytest.mark.integration()
class TestBigtableIntegration:
    """Integration tests for Bigtable using the emulator.

    For more details, see: https://cloud.google.com/bigtable/docs/emulator
    """

    @pytest.fixture
    def sample_data(self):
        return daft.from_pydict(
            {
                "id": ["user1", "user2", "user3", "user4", "user5"],
                "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
                "age": [25, 30, 35, 28, 32],
                "city": ["New York", "London", "Tokyo", "Paris", "Sydney"],
                "email": [
                    "alice@example.com",
                    "bob@example.com",
                    "charlie@example.com",
                    "diana@example.com",
                    "eve@example.com",
                ],
            }
        )

    def _get_client_kwargs_for_ray(self, bigtable_emulator_config):
        """Helper method to get client_kwargs with emulator_host when using Ray runner."""
        import os

        daft_runner = os.environ.get("DAFT_RUNNER", "").lower()
        client_kwargs = {}
        if daft_runner == "ray":
            client_kwargs["emulator_host"] = bigtable_emulator_config["emulator_host"]
        return client_kwargs

    @staticmethod
    def compare_tables(daft_table: DataFrame, bigtable_table: Table, row_key_column: str):
        type_map: dict[str, daft.DataType] = {}
        for field in daft_table.schema():
            type_map[field.name] = field.dtype

        incompatible_columns = []
        for field in daft_table.schema():
            if field.dtype.is_binary() or field.dtype.is_string() or field.dtype.is_integer():
                continue
            incompatible_columns.append(field.name)

        if incompatible_columns:
            for col in incompatible_columns:
                daft_table = daft_table.with_column(col, serialize(daft_table[col], format="json"))

        def _convert_bigtable_value(column: str, value_bytes: bytes) -> Any:
            if type_map[column].is_integer():
                return int.from_bytes(value_bytes, byteorder="big")
            elif type_map[column].is_binary():
                return value_bytes
            else:
                return value_bytes.decode("utf-8")

        bigtable_dict = {}
        for row in bigtable_table.read_rows():
            row_key = row.row_key.decode("utf-8")
            bigtable_dict[row_key] = {}
            for _col_family, columns in row.cells.items():
                for column, values in columns.items():
                    column = column.decode("utf-8")
                    value = _convert_bigtable_value(column, values[0].value)
                    bigtable_dict[row_key][column] = value

        daft_dict = {}
        for row in daft_table:
            row_key = row[row_key_column]
            del row[row_key_column]
            daft_dict[row_key] = row

        for row_key, daft_row_data in daft_dict.items():
            if row_key is None or row_key == "":
                continue
            assert row_key in bigtable_dict, f"Row key {row_key} not found in Bigtable"

            for col_name, daft_value in daft_row_data.items():
                if daft_value is None:
                    continue
                assert col_name in bigtable_dict[row_key], f"Column {col_name} not found in Bigtable row {row_key}"

                assert (
                    daft_value == bigtable_dict[row_key][col_name]
                ), f"Value mismatch for row {row_key}, column {col_name}: Daft='{daft_value}', Bigtable='{bigtable_dict[row_key][col_name]}'"

        for row_key in bigtable_dict:
            assert row_key in daft_dict, f"Extra row {row_key} found in Bigtable that wasn't in Daft DataFrame"

    def test_bigtable_write_basic(self, sample_data, bigtable_emulator_config):
        with bigtable_emulator_setup(bigtable_emulator_config) as bt_setup:
            row_key_column = "id"

            result_df = sample_data.write_bigtable(
                project_id=bigtable_emulator_config["project_id"],
                instance_id=bigtable_emulator_config["instance_id"],
                table_id=bigtable_emulator_config["table_id"],
                row_key_column=row_key_column,
                column_family_mappings={
                    "name": "cf1",
                    "age": "cf1",
                    "city": "cf2",
                    "email": "cf2",
                },
                client_kwargs=self._get_client_kwargs_for_ray(bigtable_emulator_config),
            )

            assert result_df.count_rows() == 1
            write_responses = result_df.to_pydict()["write_responses"]
            assert len(write_responses) == 1
            assert write_responses[0].result["status"] == "success"
            assert write_responses[0].result["rows_written"] == 5

            TestBigtableIntegration.compare_tables(sample_data, bt_setup["table"], row_key_column)

    def test_bigtable_write_with_client_kwargs(self, sample_data, bigtable_emulator_config):
        with bigtable_emulator_setup(bigtable_emulator_config) as bt_setup:
            row_key_column = "id"
            client_kwargs = {"read_only": False}
            client_kwargs.update(self._get_client_kwargs_for_ray(bigtable_emulator_config))
            result_df = sample_data.write_bigtable(
                project_id=bigtable_emulator_config["project_id"],
                instance_id=bigtable_emulator_config["instance_id"],
                table_id=bigtable_emulator_config["table_id"],
                row_key_column=row_key_column,
                column_family_mappings={"name": "cf1", "age": "cf1", "city": "cf2", "email": "cf2", "active": "cf1"},
                client_kwargs=client_kwargs,
            )

            assert result_df.count_rows() == 1
            write_responses = result_df.to_pydict()["write_responses"]
            assert len(write_responses) == 1
            assert write_responses[0].result["status"] == "success"
            assert write_responses[0].result["rows_written"] == 5

            TestBigtableIntegration.compare_tables(sample_data, bt_setup["table"], row_key_column)

    def test_bigtable_write_fails_with_read_only(self, sample_data, bigtable_emulator_config):
        with bigtable_emulator_setup(bigtable_emulator_config) as _bt_setup:
            row_key_column = "id"
            client_kwargs = {"read_only": True}
            client_kwargs.update(self._get_client_kwargs_for_ray(bigtable_emulator_config))
            # The "performadministrative" typo is from the client library.
            with pytest.raises(
                RuntimeError,
                match="Exception occurred while writing to Bigtable Data Sink: ValueError: A read-only client cannot also performadministrative actions.",
            ):
                sample_data.write_bigtable(
                    project_id=bigtable_emulator_config["project_id"],
                    instance_id=bigtable_emulator_config["instance_id"],
                    table_id=bigtable_emulator_config["table_id"],
                    row_key_column=row_key_column,
                    column_family_mappings={
                        "name": "cf1",
                        "age": "cf1",
                        "city": "cf2",
                        "email": "cf2",
                        "active": "cf1",
                    },
                    client_kwargs=client_kwargs,
                )

    def test_bigtable_write_with_write_kwargs(self, sample_data, bigtable_emulator_config):
        with bigtable_emulator_setup(bigtable_emulator_config) as bt_setup:
            row_key_column = "id"
            result_df = sample_data.write_bigtable(
                project_id=bigtable_emulator_config["project_id"],
                instance_id=bigtable_emulator_config["instance_id"],
                table_id=bigtable_emulator_config["table_id"],
                row_key_column=row_key_column,
                column_family_mappings={"name": "cf1", "age": "cf1", "city": "cf2", "email": "cf2", "active": "cf1"},
                write_kwargs={"flush_count": 2},
                client_kwargs=self._get_client_kwargs_for_ray(bigtable_emulator_config),
            )

            assert result_df.count_rows() == 1
            write_responses = result_df.to_pydict()["write_responses"]
            assert len(write_responses) == 1
            assert write_responses[0].result["status"] == "success"
            assert write_responses[0].result["rows_written"] == 5

            TestBigtableIntegration.compare_tables(sample_data, bt_setup["table"], row_key_column)

    def test_bigtable_write_empty_dataframe(self, bigtable_emulator_config):
        empty_df = daft.from_pydict({"id": [], "name": [], "age": []})
        row_key_column = "id"
        with bigtable_emulator_setup(bigtable_emulator_config) as _bt_setup:
            result_df = empty_df.write_bigtable(
                project_id=bigtable_emulator_config["project_id"],
                instance_id=bigtable_emulator_config["instance_id"],
                table_id=bigtable_emulator_config["table_id"],
                row_key_column=row_key_column,
                column_family_mappings={"name": "cf1", "age": "cf1"},
                client_kwargs=self._get_client_kwargs_for_ray(bigtable_emulator_config),
            )

            assert result_df.count_rows() == 0

    def test_bigtable_write_with_column_nulls(self, bigtable_emulator_config):
        data_with_nulls = daft.from_pydict(
            {
                "id": ["user1", "user2", "user3"],
                "name": ["Alice", None, "Charlie"],
                "age": [25, 30, None],
                "city": [None, "London", "Tokyo"],
            }
        )
        row_key_column = "id"

        with bigtable_emulator_setup(bigtable_emulator_config) as bt_setup:
            result_df = data_with_nulls.write_bigtable(
                project_id=bigtable_emulator_config["project_id"],
                instance_id=bigtable_emulator_config["instance_id"],
                table_id=bigtable_emulator_config["table_id"],
                row_key_column=row_key_column,
                column_family_mappings={"name": "cf1", "age": "cf1", "city": "cf2"},
                client_kwargs=self._get_client_kwargs_for_ray(bigtable_emulator_config),
            )

            assert result_df.count_rows() == 1
            write_responses = result_df.to_pydict()["write_responses"]
            assert write_responses[0].result["status"] == "success"
            assert write_responses[0].result["rows_written"] == 3

            TestBigtableIntegration.compare_tables(data_with_nulls, bt_setup["table"], row_key_column)

    def test_bigtable_write_with_row_key_nulls(self, bigtable_emulator_config):
        data_with_nulls = daft.from_pydict(
            {
                "id": [None, "user2", "user3", None, "user5"],
                "name": ["Alice", None, "Charlie", None, "Eve"],
                "age": [25, 30, None, 28, 32],
                "city": [None, "London", "Tokyo", "Paris", "Sydney"],
            }
        )
        row_key_column = "id"

        with bigtable_emulator_setup(bigtable_emulator_config) as bt_setup:
            result_df = data_with_nulls.write_bigtable(
                project_id=bigtable_emulator_config["project_id"],
                instance_id=bigtable_emulator_config["instance_id"],
                table_id=bigtable_emulator_config["table_id"],
                row_key_column=row_key_column,
                column_family_mappings={"name": "cf1", "age": "cf1", "city": "cf2"},
                client_kwargs=self._get_client_kwargs_for_ray(bigtable_emulator_config),
            )

            assert result_df.count_rows() == 1
            write_responses = result_df.to_pydict()["write_responses"]
            assert write_responses[0].result["status"] == "success"
            assert write_responses[0].result["rows_written"] == 3

            TestBigtableIntegration.compare_tables(data_with_nulls, bt_setup["table"], row_key_column)

    def test_bigtable_write_larger_dataset(self, bigtable_emulator_config):
        import random

        num_rows = 1000
        ids = [f"user_{i:03d}" for i in range(num_rows)]
        names = [f"User_{i}" for i in range(num_rows)]
        ages = [random.randint(18, 80) for _ in range(num_rows)]
        cities = [random.choice(["NYC", "LA", "Chicago", "Houston", "Phoenix"]) for _ in range(num_rows)]

        large_df = daft.from_pydict({"id": ids, "name": names, "age": ages, "city": cities})
        row_key_column = "id"

        with bigtable_emulator_setup(bigtable_emulator_config) as bt_setup:
            result_df = large_df.write_bigtable(
                project_id=bigtable_emulator_config["project_id"],
                instance_id=bigtable_emulator_config["instance_id"],
                table_id=bigtable_emulator_config["table_id"],
                row_key_column=row_key_column,
                column_family_mappings={"name": "cf1", "age": "cf1", "city": "cf2"},
                client_kwargs=self._get_client_kwargs_for_ray(bigtable_emulator_config),
            )

            assert result_df.count_rows() == 1
            write_responses = result_df.to_pydict()["write_responses"]
            assert write_responses[0].result["status"] == "success"
            assert write_responses[0].result["rows_written"] == num_rows

            TestBigtableIntegration.compare_tables(large_df, bt_setup["table"], row_key_column)

    def test_bigtable_write_different_data_types(self, bigtable_emulator_config):
        mixed_types_df = daft.from_pydict(
            {
                "id": ["row1", "row2", "row3"],
                "string_col": ["hello", "world", "test"],
                "int_col": [1, 2, 3],
                "float_col": [1.1, 2.2, 3.3],  # Incompatible type
                "bool_col": [True, False, True],  # Incompatible type
                "bytes_col": [b"bytes1", b"bytes2", b"bytes3"],
                "list_col": [[1, 2, 3], [4, 5, 6], [7, 8, 9]],  # Incompatible type
            }
        )
        row_key_column = "id"

        with bigtable_emulator_setup(bigtable_emulator_config) as bt_setup:
            result_df = mixed_types_df.write_bigtable(
                project_id=bigtable_emulator_config["project_id"],
                instance_id=bigtable_emulator_config["instance_id"],
                table_id=bigtable_emulator_config["table_id"],
                row_key_column=row_key_column,
                column_family_mappings={
                    "string_col": "cf1",
                    "int_col": "cf1",
                    "float_col": "cf1",
                    "bool_col": "cf1",
                    "bytes_col": "cf2",
                    "list_col": "cf2",
                },
                client_kwargs=self._get_client_kwargs_for_ray(bigtable_emulator_config),
            )

            assert result_df.count_rows() == 1
            write_responses = result_df.to_pydict()["write_responses"]
            assert write_responses[0].result["status"] == "success"
            assert write_responses[0].result["rows_written"] == 3

            TestBigtableIntegration.compare_tables(mixed_types_df, bt_setup["table"], row_key_column)

    def test_bigtable_write_incompatible_types_error(self, bigtable_emulator_config):
        mixed_types_df = daft.from_pydict(
            {
                "id": ["row1", "row2", "row3"],
                "string_col": ["hello", "world", "test"],
                "int_col": [1, 2, 3],
                "float_col": [1.1, 2.2, 3.3],  # Incompatible type
                "bool_col": [True, False, True],  # Incompatible type
                "bytes_col": [b"bytes1", b"bytes2", b"bytes3"],
                "list_col": [[1, 2, 3], [4, 5, 6], [7, 8, 9]],  # Incompatible type
            }
        )
        row_key_column = "id"
        with bigtable_emulator_setup(bigtable_emulator_config) as _bt_setup:
            with pytest.raises(
                ValueError,
                match="Bigtable only supports integer, binary, and string types. Found incompatible columns: float_col, bool_col, list_col. Set `serialize_incompatible_types=True` to automatically convert these to JSON, or convert them manually before writing.",
            ):
                mixed_types_df.write_bigtable(
                    project_id=bigtable_emulator_config["project_id"],
                    instance_id=bigtable_emulator_config["instance_id"],
                    table_id=bigtable_emulator_config["table_id"],
                    row_key_column=row_key_column,
                    column_family_mappings={
                        "string_col": "cf1",
                        "int_col": "cf1",
                        "float_col": "cf1",
                        "bool_col": "cf1",
                        "bytes_col": "cf2",
                        "list_col": "cf2",
                    },
                    serialize_incompatible_types=False,
                    client_kwargs=self._get_client_kwargs_for_ray(bigtable_emulator_config),
                )

    def test_bigtable_write_compatible_types_no_serialization(self, bigtable_emulator_config):
        compatible_types_df = daft.from_pydict(
            {
                "id": ["row1", "row2", "row3"],
                "string_col": ["hello", "world", "test"],
                "int_col": [1, 2, 3],
                "bytes_col": [b"bytes1", b"bytes2", b"bytes3"],
            }
        )
        row_key_column = "id"
        with bigtable_emulator_setup(bigtable_emulator_config) as bt_setup:
            result_df = compatible_types_df.write_bigtable(
                project_id=bigtable_emulator_config["project_id"],
                instance_id=bigtable_emulator_config["instance_id"],
                table_id=bigtable_emulator_config["table_id"],
                row_key_column=row_key_column,
                column_family_mappings={"string_col": "cf1", "int_col": "cf1", "bytes_col": "cf2"},
                serialize_incompatible_types=False,
                client_kwargs=self._get_client_kwargs_for_ray(bigtable_emulator_config),
            )

            assert result_df.count_rows() == 1
            write_responses = result_df.to_pydict()["write_responses"]
            assert write_responses[0].result["status"] == "success"
            assert write_responses[0].result["rows_written"] == 3

            TestBigtableIntegration.compare_tables(compatible_types_df, bt_setup["table"], row_key_column)

    def test_bigtable_write_error_handling(self, bigtable_emulator_config):
        """Test Bigtable write error handling."""
        # Create data with invalid row key (empty string).
        invalid_data = daft.from_pydict(
            {
                "id": ["", "valid_id"],  # Empty string should cause issues
                "name": ["Invalid", "Valid"],
                "age": [0, 25],
            }
        )
        row_key_column = "id"

        with bigtable_emulator_setup(bigtable_emulator_config) as bt_setup:
            result_df = invalid_data.write_bigtable(
                project_id=bigtable_emulator_config["project_id"],
                instance_id=bigtable_emulator_config["instance_id"],
                table_id=bigtable_emulator_config["table_id"],
                row_key_column=row_key_column,
                column_family_mappings={"name": "cf1", "age": "cf1"},
                client_kwargs=self._get_client_kwargs_for_ray(bigtable_emulator_config),
            )

            # Workflow should still succeed but rows with invalid row keys should be filtered out.
            assert result_df.count_rows() == 1
            write_responses = result_df.to_pydict()["write_responses"]
            assert write_responses[0].result["status"] in ["success"]
            assert write_responses[0].result["rows_written"] == 1

            TestBigtableIntegration.compare_tables(invalid_data, bt_setup["table"], row_key_column)

    def test_bigtable_write_missing_row_key_column(self, bigtable_emulator_config):
        sample_data = daft.from_pydict({"id": ["user1", "user2"], "name": ["Alice", "Bob"], "age": [25, 30]})
        row_key_column = "id"

        with bigtable_emulator_setup(bigtable_emulator_config) as _bt_setup:
            with pytest.raises(ValueError, match='Row key column "invalid_id" not found in schema'):
                sample_data.write_bigtable(
                    project_id=bigtable_emulator_config["project_id"],
                    instance_id=bigtable_emulator_config["instance_id"],
                    table_id=bigtable_emulator_config["table_id"],
                    row_key_column="invalid_id",
                    column_family_mappings={"name": "cf1", "age": "cf1"},
                    client_kwargs=self._get_client_kwargs_for_ray(bigtable_emulator_config),
                )

            # Test missing column family mapping.
            with pytest.raises(ValueError, match="Column family mappings missing for columns: \\['age'\\]"):
                sample_data.write_bigtable(
                    project_id=bigtable_emulator_config["project_id"],
                    instance_id=bigtable_emulator_config["instance_id"],
                    table_id=bigtable_emulator_config["table_id"],
                    row_key_column=row_key_column,
                    column_family_mappings={"name": "cf1"},  # Missing age mapping
                    client_kwargs=self._get_client_kwargs_for_ray(bigtable_emulator_config),
                )
