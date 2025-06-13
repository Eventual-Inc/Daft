from __future__ import annotations

import argparse
import logging
from pathlib import Path

import duckdb

logger = logging.getLogger(__name__)


def generate_alter_stmt(db: duckdb.DuckDBPyConnection, table_name: str) -> list[str]:
    """Finds all decimal columns and casts them to double."""
    query = f"""
    SELECT
      'ALTER TABLE {table_name} ALTER COLUMN ' || column_name || ' TYPE DOUBLE;'
    FROM information_schema.columns
    WHERE data_type LIKE 'DECIMAL%'
    AND table_name = '{table_name}';"""
    items = db.sql(query).fetchall()
    return [item[0] for item in items]


def gen_tpcds(dir: Path, scale_factor: float, cast_decimal: bool = False):
    if dir.exists():
        assert dir.is_dir(), "The location in which to generate the data must be a directory"
        logger.info(
            "The directory %s already exists; doing nothing",
            dir,
        )
        return

    dir.mkdir(parents=True, exist_ok=True)
    db = duckdb.connect(database=dir / "tpcds.db")
    db.sql(f"call dsdgen(sf = {scale_factor})")
    for item in db.sql("show tables").fetchall():
        tbl = item[0]
        if cast_decimal:
            for alter in generate_alter_stmt(db, tbl):
                db.sql(alter)
        parquet_file = dir / f"{tbl}.parquet"
        print(f"Exporting {tbl} to {parquet_file}")
        db.sql(f"COPY {tbl} TO '{parquet_file}'")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--tpcds-gen-folder",
        default="data/tpcds-dbgen",
        type=Path,
        help="Path to the folder containing the TPC-DS dsdgen tool and generated data",
    )
    parser.add_argument("--scale-factor", default=0.01, help="Scale factor to run on in GB", type=float)
    parser.add_argument(
        "--cast-decimal",
        action="store_true",
        help="Cast all decimal columns to double/float64 during generation",
    )
    args = parser.parse_args()

    tpcds_gen_folder: Path = args.tpcds_gen_folder
    assert args.scale_factor > 0

    logger.info(
        "Generating data at %s with: scale_factor=%s",
        tpcds_gen_folder,
        args.scale_factor,
    )

    scaled_tpcds_gen_folder = tpcds_gen_folder / str(args.scale_factor)
    gen_tpcds(dir=scaled_tpcds_gen_folder, scale_factor=args.scale_factor, cast_decimal=args.cast_decimal)
