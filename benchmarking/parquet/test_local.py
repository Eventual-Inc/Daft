from __future__ import annotations

from pathlib import Path

import pytest

from tests.assets import get_asset_dir


def generate_parquet(dir: str, num_rows: int, num_cols: int, num_rowgroups: int):
    import numpy as np
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as papq
    from faker import Faker

    # Initialize Faker
    Faker.seed(0)  # For reproducibility
    fake = Faker()

    # Generate small pools of fake data that we'll sample from
    POOL_SIZE = 1000  # Size of initial fake data pools

    # Pre-generate data pools
    name_pool = [fake.name() for _ in range(POOL_SIZE)]
    email_pool = [fake.email() for _ in range(POOL_SIZE)]
    company_pool = [fake.company() for _ in range(POOL_SIZE)]
    job_pool = [fake.job() for _ in range(POOL_SIZE)]
    address_pool = [fake.address().replace("\n", ", ") for _ in range(POOL_SIZE)]

    # Pre-generate date pools
    recent_dates = pd.date_range(end=pd.Timestamp.now(), periods=POOL_SIZE, freq="H")
    past_dates = pd.date_range(end=pd.Timestamp.now(), periods=POOL_SIZE, freq="D")
    future_dates = pd.date_range(start=pd.Timestamp.now(), periods=POOL_SIZE, freq="D")

    data = {}
    for i in range(num_cols):
        col_type = i % 5

        if col_type == 0:
            # Integer columns (vectorized operations)
            data_type = i % 3
            if data_type == 0:
                data[f"age_{i}"] = np.random.randint(0, 100, size=num_rows)
            elif data_type == 1:
                data[f"price_{i}"] = np.random.randint(0, 1000, size=num_rows)
            else:
                data[f"views_{i}"] = np.random.randint(0, 1000000, size=num_rows)

        elif col_type == 1:
            # Float columns (vectorized operations)
            data_type = i % 3
            if data_type == 0:
                data[f"rating_{i}"] = np.round(np.random.uniform(0, 5, size=num_rows), 1)
            elif data_type == 1:
                data[f"temp_{i}"] = np.round(np.random.uniform(-20, 45, size=num_rows), 1)
            else:
                data[f"percentage_{i}"] = np.round(np.random.uniform(0, 100, size=num_rows), 2)

        elif col_type == 2:
            # String columns (sampling from pre-generated pools)
            data_type = i % 5
            if data_type == 0:
                data[f"name_{i}"] = np.random.choice(name_pool, size=num_rows)
            elif data_type == 1:
                data[f"email_{i}"] = np.random.choice(email_pool, size=num_rows)
            elif data_type == 2:
                data[f"company_{i}"] = np.random.choice(company_pool, size=num_rows)
            elif data_type == 3:
                data[f"address_{i}"] = np.random.choice(address_pool, size=num_rows)
            else:
                data[f"job_{i}"] = np.random.choice(job_pool, size=num_rows)

        elif col_type == 3:
            # Boolean columns (vectorized operations)
            data[f"is_active_{i}"] = np.random.choice([True, False], size=num_rows)

        else:
            # Timestamp columns (sampling from pre-generated date ranges)
            data_type = i % 3
            if data_type == 0:
                data[f"recent_date_{i}"] = np.random.choice(recent_dates, size=num_rows)
            elif data_type == 1:
                data[f"past_date_{i}"] = np.random.choice(past_dates, size=num_rows)
            else:
                data[f"future_date_{i}"] = np.random.choice(future_dates, size=num_rows)

    df = pd.DataFrame(data)
    papq.write_table(
        table=pa.Table.from_pandas(df),
        where=dir,
        row_group_size=num_rows // num_rowgroups,
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
