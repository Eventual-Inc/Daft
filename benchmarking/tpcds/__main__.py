import argparse
from datetime import datetime
from pathlib import Path

import daft
from daft.sql import SQLCatalog


def generate_tpcds_catalog(tpcds_gen_folder: Path):
    if not tpcds_gen_folder.exists():
        raise RuntimeError
    return SQLCatalog(
        tables={
            file.stem: daft.read_parquet(path=str(file))
            for file in tpcds_gen_folder.iterdir()
            if file.is_file() and file.suffix == ".parquet"
        }
    )


def run_query(query_index: int, catalog: SQLCatalog) -> str:
    sql_queries_path = Path("benchmarking") / "tpcds" / "queries"
    query_file = sql_queries_path / f"{str(query_index).zfill(2)}.sql"
    with open(query_file) as f:
        query = f.read()

    start = datetime.now()

    try:
        daft.sql(query, catalog=catalog).collect()
        success = True
        error_message = None
    except Exception as e:
        success = False
        error_message = str(e)

    end = datetime.now()
    duration = end - start
    return duration, success, error_message if error_message else None


def run_benchmarks(tpcds_gen_folder: Path):
    successes = {}
    failures = {}

    catalog = generate_tpcds_catalog(tpcds_gen_folder)
    for query_index in range(1, 100):
        duration, was_successful, error_msg = run_query(query_index, catalog)
        if was_successful:
            successes[query_index] = duration
        else:
            failures[query_index] = (duration, error_msg)

    return successes, failures

    # failed = []
    # failed_messages = []
    # success = []
    # success_duration = []
    # queries = []

    # for i in range(1, 100):
    #     query = open(f"{TPCDS_QUERIES_PATH}/{str(i).zfill(2)}.sql").read()
    #     try:
    #         start = time.time()

    #         daft.sql(query, catalog=catalog).collect()
    #         duration = time.time() - start
    #         print(f"Query {i} ran successfully")
    #         success.append(i)
    #         success_duration.append(duration)
    #     except Exception as e:
    #         failed.append(i)
    #         queries.append(query)
    #         failed_messages.append(str(e))

    # pl.Config.set_fmt_str_lengths(10000)
    # pl.Config.set_fmt_table_cell_list_len(100)
    # pl.Config.set_tbl_rows(100)
    # df = pl.DataFrame({"q": failed, "query": queries, "message": failed_messages})
    # success_df = pl.DataFrame({"q": success, "duration": success_duration})
    # df.group_by("message").agg(pl.len(), pl.col("q")).sort("len", descending=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--tpcds-gen-folder",
        default="data/tpcds-dbgen",
        help="Path to the folder containing the TPC-DS dsdgen tool and generated data",
    )
    parser.add_argument("--scale-factor", default=0.01, type=float, help="Scale factor to run on in GB")
    args = parser.parse_args()

    tpcds_gen_folder = Path(args.tpcds_gen_folder)
    assert tpcds_gen_folder.exists()
    assert args.scale_factor > 0

    run_benchmarks(tpcds_gen_folder)
