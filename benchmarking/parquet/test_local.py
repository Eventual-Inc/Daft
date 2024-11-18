from pathlib import Path

import pytest

from tests.assets import get_asset_dir


def generate_parquet(dir: str, num_rows: int, num_cols: int, num_rowgroups: int):
    from datetime import datetime, timedelta

    import fastparquet
    import numpy as np
    import pandas as pd

    # Generate different data types for each column
    data = {}
    for i in range(num_cols):
        col_type = i % 5  # Cycle through 5 different data types

        if col_type == 0:
            # Integer columns
            data[f"int_{i}"] = np.random.randint(low=0, high=1000000, size=num_rows, dtype=np.int64)
        elif col_type == 1:
            # Float columns
            data[f"float_{i}"] = np.random.uniform(low=0, high=1000, size=num_rows).astype(np.float64)
        elif col_type == 2:
            # String columns
            data[f"str_{i}"] = [f"str_{j}" for j in np.random.randint(0, 1000, size=num_rows)]
        elif col_type == 3:
            # Boolean columns
            data[f"bool_{i}"] = np.random.choice([True, False], size=num_rows)
        else:
            # Timestamp columns
            start_date = datetime(2020, 1, 1)
            dates = [start_date + timedelta(days=int(x)) for x in np.random.randint(0, 1825, size=num_rows)]
            data[f"date_{i}"] = dates

    df = pd.DataFrame(data)
    print(f"Writing {num_rows} rows, {num_cols} columns, {num_rowgroups} rowgroups to {dir}")
    fastparquet.write(
        filename=dir,
        data=df,
        row_group_offsets=num_rows // num_rowgroups,
        compression="SNAPPY",
    )
    print(f"Finished writing {dir}")


SIZE_CONFIGS = [
    (10_000_000, 1),  # Lots of rows, single col
    (1_000_000, 32),  # Balanced
    (10_000, 1024),  # Few rows, many cols
]

ROWGROUP_CONFIGS = [1, 8, 64]

LOCAL_DATA_FIXTURE_PATH = Path(get_asset_dir()) / "../../benchmarking/parquet/local_data"


def get_param_id(param):
    (num_rows, num_cols), num_rowgroups = param
    return f"{num_rows}_rows_{num_cols}_cols_{num_rowgroups}_rowgroups"


@pytest.fixture(
    scope="session",
    params=[(size, rowgroup) for size in SIZE_CONFIGS for rowgroup in ROWGROUP_CONFIGS],
    ids=lambda param: get_param_id(param),
)
def parquet_file(request):
    (num_rows, num_cols), num_rowgroups = request.param
    filepath = LOCAL_DATA_FIXTURE_PATH / f"test_{num_rows}rows_{num_cols}cols_{num_rowgroups}groups.parquet"
    if not filepath.parent.exists():
        filepath.parent.mkdir(parents=True)
    if not filepath.exists():
        generate_parquet(str(filepath), num_rows, num_cols, num_rowgroups)
    return str(filepath)


@pytest.mark.benchmark(group="read_parquet_local")
def test_read_parquet(parquet_file, benchmark):
    import daft

    def read_parquet():
        df = daft.read_parquet(parquet_file)
        return df.to_arrow()

    benchmark(read_parquet)
