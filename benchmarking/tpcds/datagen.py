import argparse
import logging
import os

import duckdb

logger = logging.getLogger(__name__)


def gen_tpcds(basedir: str, scale_factor: float):
    db = duckdb.connect()
    db.sql(f"call dsdgen(sf = {scale_factor})")
    if not os.path.exists(basedir):
        os.makedirs(basedir)
    for item in db.sql("show tables").fetchall():
        tbl = item[0]
        db.sql(f"COPY {tbl} TO '{basedir}/{tbl}.parquet'")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--tpch-gen-folder",
        default="data/tpch-dbgen",
        help="Path to the folder containing the TPCH dbgen tool and generated data",
    )
    parser.add_argument("--scale-factor", default=0.01, help="Scale factor to run on in GB", type=float)

    args = parser.parse_args()
    num_parts = args.scale_factor

    logger.info(
        "Generating data at %s with: scale_factor=%s num_parts=%s generate_sqlite_db=%s generate_parquet=%s",
        args.tpch_gen_folder,
        args.scale_factor,
        num_parts,
    )

    gen_tpcds(basedir=args.tpch_gen_folder, scale_factor=args.scale_factor)
