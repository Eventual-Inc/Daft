# from __future__ import annotations

# import contextlib
# import csv
# import datetime
# import json
# import pathlib
# import sys
# from typing import Callable, Union

from __future__ import annotations

# import pyarrow as pa
# import pyarrow.parquet as papq
# import pytest
# if sys.version_info < (3, 8):
#     from typing_extensions import Literal
# else:
#     from typing import Literal

# from daft.runners.partitioning import TableParseCSVOptions
# from daft.table import table_io

# InputType = Union[Literal["file"], Literal["filepath"], Literal["pathlib.Path"]]

# TEST_INPUT_TYPES = ["file", "path", "pathlib.Path"]
# TEST_DATA_LEN = 16
# TEST_DATA = {
#     "dates": [datetime.date(2020, 1, 1) + datetime.timedelta(days=i) for i in range(TEST_DATA_LEN)],
#     "strings": [f"foo_{i}" for i in range(TEST_DATA_LEN)],
#     "integers": [i for i in range(TEST_DATA_LEN)],
#     "floats": [float(i) for i in range(TEST_DATA_LEN)],
#     "bools": [True for i in range(TEST_DATA_LEN)],
#     "var_sized_arrays": [[i for _ in range(i)] for i in range(TEST_DATA_LEN)],
#     "fixed_sized_arrays": [[i for _ in range(4)] for i in range(TEST_DATA_LEN)],
#     "structs": [{"foo": i} for i in range(TEST_DATA_LEN)],
# }


# @contextlib.contextmanager
# def _resolve_parametrized_input_type(input_type: InputType, path: str) -> table_io.FileInput:
#     if input_type == "file":
#         with open(path, "rb") as f:
#             yield f
#     elif input_type == "path":
#         yield path
#     elif input_type == "pathlib.Path":
#         yield pathlib.Path(path)
#     else:
#         raise NotImplementedError(f"input_type={input_type}")


# ###
# # CSV Fixtures
# ###


# @pytest.fixture(scope="module")
# def csv_expected_data():
#     return {
#         "dates": TEST_DATA["dates"],
#         "strings": TEST_DATA["strings"],
#         "integers": TEST_DATA["integers"],
#         "floats": TEST_DATA["floats"],
#         "bools": TEST_DATA["bools"],
#         "var_sized_arrays": [str(l) for l in TEST_DATA["var_sized_arrays"]],
#         "fixed_sized_arrays": [str(l) for l in TEST_DATA["fixed_sized_arrays"]],
#         "structs": [str(s) for s in TEST_DATA["structs"]],
#     }


# @pytest.fixture(scope="function", params=[(it,) for it in TEST_INPUT_TYPES])
# def generate_csv_input(request, tmpdir: str) -> Callable[[TableParseCSVOptions], str]:
#     (input_type,) = request.param

#     @contextlib.contextmanager
#     def _generate(csv_options) -> InputType:
#         path = str(tmpdir + f"/data.csv")
#         headers = [cname for cname in TEST_DATA]
#         with open(path, "w") as f:
#             writer = csv.writer(f, delimiter=csv_options.delimiter)
#             if csv_options.header_index is not None:
#                 for _ in range(csv_options.header_index):
#                     writer.writerow(["extra-data-before-header" for _ in TEST_DATA])
#                 writer.writerow(headers)
#             writer.writerows([[TEST_DATA[cname][row] for cname in headers] for row in range(TEST_DATA_LEN)])

#         with _resolve_parametrized_input_type(input_type, path) as table_io_input:
#             yield table_io_input

#     yield _generate


# ###
# # JSON Fixtures
# ###


# class CustomJSONEncoder(json.JSONEncoder):
#     def default(self, obj):
#         if isinstance(obj, datetime.date):
#             return obj.isoformat()
#         else:
#             return super().default(obj)


# @pytest.fixture(scope="module")
# def json_expected_data():
#     return {
#         # NOTE: PyArrow JSON parser parses dates as timestamps, so this fails for us at the moment
#         # as we still lack timestamp type support.
#         # "dates": [datetime.datetime(d.year, d.month, d.day) for d in TEST_DATA["dates"]],
#         "strings": TEST_DATA["strings"],
#         "integers": TEST_DATA["integers"],
#         "floats": TEST_DATA["floats"],
#         "bools": TEST_DATA["bools"],
#         # TODO(jaychia): [SCHEMA] fix nested type casting logic
#         # "var_sized_arrays": TEST_DATA["var_sized_arrays"],
#         # "fixed_sized_arrays": TEST_DATA["fixed_sized_arrays"],
#         # "structs": TEST_DATA["structs"],
#     }


# @pytest.fixture(scope="function", params=[(it,) for it in TEST_INPUT_TYPES])
# def json_input(request, tmpdir: str) -> str:
#     (input_type,) = request.param

#     # NOTE: PyArrow JSON parser parses dates as timestamps, so this fails for us at the moment
#     # as we still lack timestamp type support.
#     skip_columns = {"dates"}

#     path = str(tmpdir + f"/data.json")
#     with open(path, "wb") as f:
#         for row in range(TEST_DATA_LEN):
#             row_data = {cname: TEST_DATA[cname][row] for cname in TEST_DATA if cname not in skip_columns}
#             f.write(json.dumps(row_data, default=CustomJSONEncoder().default).encode("utf-8"))
#             f.write(b"\n")

#     with _resolve_parametrized_input_type(input_type, path) as table_io_input:
#         yield table_io_input


# ###
# # Parquet fixtures
# ###


# @pytest.fixture(scope="module")
# def parquet_expected_data():
#     return {
#         "dates": TEST_DATA["dates"],
#         "strings": TEST_DATA["strings"],
#         "integers": TEST_DATA["integers"],
#         "floats": TEST_DATA["floats"],
#         "bools": TEST_DATA["bools"],
#         # TODO(jaychia): [SCHEMA] fix nested type casting logic
#         # "var_sized_arrays": TEST_DATA["var_sized_arrays"],
#         # "fixed_sized_arrays": TEST_DATA["fixed_sized_arrays"],
#         # "structs": TEST_DATA["structs"],
#     }


# @pytest.fixture(scope="function", params=[(it,) for it in TEST_INPUT_TYPES])
# def parquet_input(request, tmpdir: str) -> str:
#     (input_type,) = request.param

#     path = str(tmpdir + f"/data.parquet")
#     papq.write_table(pa.Table.from_pydict(TEST_DATA), path)

#     with _resolve_parametrized_input_type(input_type, path) as table_io_input:
#         yield table_io_input
