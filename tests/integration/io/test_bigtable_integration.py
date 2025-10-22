from __future__ import annotations

import pytest

import daft
from tests.integration.io.conftest import bigtable_emulator_setup


@pytest.mark.integration()
class TestBigTableIntegration:
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

    def test_bigtable_write_basic(self, sample_data, bigtable_emulator_config):
        with bigtable_emulator_setup(bigtable_emulator_config) as _bt_setup:
            result_df = sample_data.write_bigtable(
                project_id=bigtable_emulator_config["project_id"],
                instance_id=bigtable_emulator_config["instance_id"],
                table_id=bigtable_emulator_config["table_id"],
                row_key_column="id",
                column_family_mappings={
                    "name": "cf1",
                    "age": "cf1",
                    "city": "cf2",
                    "email": "cf2",
                },
            )

            assert result_df.count_rows() == 1
            write_responses = result_df.to_pydict()["write_responses"]
            assert len(write_responses) == 1
            assert write_responses[0].result["status"] == "success"
            assert write_responses[0].result["rows_written"] == 5

    def test_bigtable_write_with_client_kwargs(self, sample_data, bigtable_emulator_config):
        with bigtable_emulator_setup(bigtable_emulator_config) as _bt_setup:
            result_df = sample_data.write_bigtable(
                project_id=bigtable_emulator_config["project_id"],
                instance_id=bigtable_emulator_config["instance_id"],
                table_id=bigtable_emulator_config["table_id"],
                row_key_column="id",
                column_family_mappings={"name": "cf1", "age": "cf1", "city": "cf2", "email": "cf2", "active": "cf1"},
                client_kwargs={"read_only": False},
            )

            assert result_df.count_rows() == 1
            write_responses = result_df.to_pydict()["write_responses"]
            assert len(write_responses) == 1
            assert write_responses[0].result["status"] == "success"
            assert write_responses[0].result["rows_written"] == 5

    def test_bigtable_write_fails_with_read_only(self, sample_data, bigtable_emulator_config):
        with bigtable_emulator_setup(bigtable_emulator_config) as _bt_setup:
            # The "performadministrative" typo is from the client library.
            with pytest.raises(
                RuntimeError,
                match="Exception occurred while writing to BigTable Data Sink: ValueError: A read-only client cannot also performadministrative actions.",
            ):
                sample_data.write_bigtable(
                    project_id=bigtable_emulator_config["project_id"],
                    instance_id=bigtable_emulator_config["instance_id"],
                    table_id=bigtable_emulator_config["table_id"],
                    row_key_column="id",
                    column_family_mappings={
                        "name": "cf1",
                        "age": "cf1",
                        "city": "cf2",
                        "email": "cf2",
                        "active": "cf1",
                    },
                    client_kwargs={"read_only": True},
                )

    def test_bigtable_write_with_write_kwargs(self, sample_data, bigtable_emulator_config):
        with bigtable_emulator_setup(bigtable_emulator_config) as _bt_setup:
            result_df = sample_data.write_bigtable(
                project_id=bigtable_emulator_config["project_id"],
                instance_id=bigtable_emulator_config["instance_id"],
                table_id=bigtable_emulator_config["table_id"],
                row_key_column="id",
                column_family_mappings={"name": "cf1", "age": "cf1", "city": "cf2", "email": "cf2", "active": "cf1"},
                write_kwargs={"flush_count": 2},
            )

            assert result_df.count_rows() == 1
            write_responses = result_df.to_pydict()["write_responses"]
            assert len(write_responses) == 1
            assert write_responses[0].result["status"] == "success"
            assert write_responses[0].result["rows_written"] == 5

    def test_bigtable_write_empty_dataframe(self, bigtable_emulator_config):
        empty_df = daft.from_pydict({"id": [], "name": [], "age": []})

        with bigtable_emulator_setup(bigtable_emulator_config) as _bt_setup:
            result_df = empty_df.write_bigtable(
                project_id=bigtable_emulator_config["project_id"],
                instance_id=bigtable_emulator_config["instance_id"],
                table_id=bigtable_emulator_config["table_id"],
                row_key_column="id",
                column_family_mappings={"name": "cf1", "age": "cf1"},
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

        with bigtable_emulator_setup(bigtable_emulator_config) as _bt_setup:
            result_df = data_with_nulls.write_bigtable(
                project_id=bigtable_emulator_config["project_id"],
                instance_id=bigtable_emulator_config["instance_id"],
                table_id=bigtable_emulator_config["table_id"],
                row_key_column="id",
                column_family_mappings={"name": "cf1", "age": "cf1", "city": "cf2"},
            )

            assert result_df.count_rows() == 1
            write_responses = result_df.to_pydict()["write_responses"]
            assert write_responses[0].result["status"] == "success"
            assert write_responses[0].result["rows_written"] == 3

    def test_bigtable_write_with_row_key_nulls(self, bigtable_emulator_config):
        data_with_nulls = daft.from_pydict(
            {
                "id": [None, "user2", "user3", None, "user5"],
                "name": ["Alice", None, "Charlie", None, "Eve"],
                "age": [25, 30, None, 28, 32],
                "city": [None, "London", "Tokyo", "Paris", "Sydney"],
            }
        )

        with bigtable_emulator_setup(bigtable_emulator_config) as _bt_setup:
            result_df = data_with_nulls.write_bigtable(
                project_id=bigtable_emulator_config["project_id"],
                instance_id=bigtable_emulator_config["instance_id"],
                table_id=bigtable_emulator_config["table_id"],
                row_key_column="id",
                column_family_mappings={"name": "cf1", "age": "cf1", "city": "cf2"},
            )

            assert result_df.count_rows() == 1
            write_responses = result_df.to_pydict()["write_responses"]
            assert write_responses[0].result["status"] == "success"
            assert write_responses[0].result["rows_written"] == 3

    def test_bigtable_write_larger_dataset(self, bigtable_emulator_config):
        import random

        num_rows = 1000
        ids = [f"user_{i:03d}" for i in range(num_rows)]
        names = [f"User_{i}" for i in range(num_rows)]
        ages = [random.randint(18, 80) for _ in range(num_rows)]
        cities = [random.choice(["NYC", "LA", "Chicago", "Houston", "Phoenix"]) for _ in range(num_rows)]

        large_df = daft.from_pydict({"id": ids, "name": names, "age": ages, "city": cities})

        with bigtable_emulator_setup(bigtable_emulator_config) as _bt_setup:
            result_df = large_df.write_bigtable(
                project_id=bigtable_emulator_config["project_id"],
                instance_id=bigtable_emulator_config["instance_id"],
                table_id=bigtable_emulator_config["table_id"],
                row_key_column="id",
                column_family_mappings={"name": "cf1", "age": "cf1", "city": "cf2"},
            )

            assert result_df.count_rows() == 1
            write_responses = result_df.to_pydict()["write_responses"]
            assert write_responses[0].result["status"] == "success"
            assert write_responses[0].result["rows_written"] == num_rows

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

        with bigtable_emulator_setup(bigtable_emulator_config) as _bt_setup:
            result_df = mixed_types_df.write_bigtable(
                project_id=bigtable_emulator_config["project_id"],
                instance_id=bigtable_emulator_config["instance_id"],
                table_id=bigtable_emulator_config["table_id"],
                row_key_column="id",
                column_family_mappings={
                    "string_col": "cf1",
                    "int_col": "cf1",
                    "float_col": "cf1",
                    "bool_col": "cf1",
                    "bytes_col": "cf2",
                    "list_col": "cf2",
                },
            )

            assert result_df.count_rows() == 1
            write_responses = result_df.to_pydict()["write_responses"]
            assert write_responses[0].result["status"] == "success"
            assert write_responses[0].result["rows_written"] == 3

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

        with bigtable_emulator_setup(bigtable_emulator_config) as _bt_setup:
            with pytest.raises(
                ValueError,
                match="BigTable only supports integer, binary, and string types. Found incompatible columns: float_col, bool_col, list_col. Set `serialize_incompatible_types=True` to automatically convert these to JSON, or convert them manually before writing.",
            ):
                mixed_types_df.write_bigtable(
                    project_id=bigtable_emulator_config["project_id"],
                    instance_id=bigtable_emulator_config["instance_id"],
                    table_id=bigtable_emulator_config["table_id"],
                    row_key_column="id",
                    column_family_mappings={
                        "string_col": "cf1",
                        "int_col": "cf1",
                        "float_col": "cf1",
                        "bool_col": "cf1",
                        "bytes_col": "cf2",
                        "list_col": "cf2",
                    },
                    serialize_incompatible_types=False,
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

        with bigtable_emulator_setup(bigtable_emulator_config) as _bt_setup:
            result_df = compatible_types_df.write_bigtable(
                project_id=bigtable_emulator_config["project_id"],
                instance_id=bigtable_emulator_config["instance_id"],
                table_id=bigtable_emulator_config["table_id"],
                row_key_column="id",
                column_family_mappings={"string_col": "cf1", "int_col": "cf1", "bytes_col": "cf2"},
                serialize_incompatible_types=False,
            )

            assert result_df.count_rows() == 1
            write_responses = result_df.to_pydict()["write_responses"]
            assert write_responses[0].result["status"] == "success"
            assert write_responses[0].result["rows_written"] == 3

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

        with bigtable_emulator_setup(bigtable_emulator_config) as _bt_setup:
            result_df = invalid_data.write_bigtable(
                project_id=bigtable_emulator_config["project_id"],
                instance_id=bigtable_emulator_config["instance_id"],
                table_id=bigtable_emulator_config["table_id"],
                row_key_column="id",
                column_family_mappings={"name": "cf1", "age": "cf1"},
            )

            # Workflow should still succeed but rows with invalid row keys should be filtered out.
            assert result_df.count_rows() == 1
            write_responses = result_df.to_pydict()["write_responses"]
            assert write_responses[0].result["status"] in ["success"]
            assert write_responses[0].result["rows_written"] == 1

    def test_bigtable_write_missing_row_key_column(self, bigtable_emulator_config):
        sample_data = daft.from_pydict({"id": ["user1", "user2"], "name": ["Alice", "Bob"], "age": [25, 30]})

        with bigtable_emulator_setup(bigtable_emulator_config) as _bt_setup:
            with pytest.raises(ValueError, match='Row key column "invalid_id" not found in schema'):
                sample_data.write_bigtable(
                    project_id=bigtable_emulator_config["project_id"],
                    instance_id=bigtable_emulator_config["instance_id"],
                    table_id=bigtable_emulator_config["table_id"],
                    row_key_column="invalid_id",
                    column_family_mappings={"name": "cf1", "age": "cf1"},
                )

            # Test missing column family mapping.
            with pytest.raises(ValueError, match="Column family mappings missing for columns: \\['age'\\]"):
                sample_data.write_bigtable(
                    project_id=bigtable_emulator_config["project_id"],
                    instance_id=bigtable_emulator_config["instance_id"],
                    table_id=bigtable_emulator_config["table_id"],
                    row_key_column="id",
                    column_family_mappings={"name": "cf1"},  # Missing age mapping
                )
