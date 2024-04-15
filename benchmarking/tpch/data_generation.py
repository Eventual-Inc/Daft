from __future__ import annotations

import argparse
import logging
import math
import os
import shlex
import sqlite3
import subprocess
from glob import glob

import daft

logger = logging.getLogger(__name__)

SCHEMA = {
    "part": [
        "P_PARTKEY",
        "P_NAME",
        "P_MFGR",
        "P_BRAND",
        "P_TYPE",
        "P_SIZE",
        "P_CONTAINER",
        "P_RETAILPRICE",
        "P_COMMENT",
    ],
    "supplier": [
        "S_SUPPKEY",
        "S_NAME",
        "S_ADDRESS",
        "S_NATIONKEY",
        "S_PHONE",
        "S_ACCTBAL",
        "S_COMMENT",
    ],
    "partsupp": [
        "PS_PARTKEY",
        "PS_SUPPKEY",
        "PS_AVAILQTY",
        "PS_SUPPLYCOST",
        "PS_COMMENT",
    ],
    "customer": [
        "C_CUSTKEY",
        "C_NAME",
        "C_ADDRESS",
        "C_NATIONKEY",
        "C_PHONE",
        "C_ACCTBAL",
        "C_MKTSEGMENT",
        "C_COMMENT",
    ],
    "orders": [
        "O_ORDERKEY",
        "O_CUSTKEY",
        "O_ORDERSTATUS",
        "O_TOTALPRICE",
        "O_ORDERDATE",
        "O_ORDERPRIORITY",
        "O_CLERK",
        "O_SHIPPRIORITY",
        "O_COMMENT",
    ],
    "lineitem": [
        "L_ORDERKEY",
        "L_PARTKEY",
        "L_SUPPKEY",
        "L_LINENUMBER",
        "L_QUANTITY",
        "L_EXTENDEDPRICE",
        "L_DISCOUNT",
        "L_TAX",
        "L_RETURNFLAG",
        "L_LINESTATUS",
        "L_SHIPDATE",
        "L_COMMITDATE",
        "L_RECEIPTDATE",
        "L_SHIPINSTRUCT",
        "L_SHIPMODE",
        "L_COMMENT",
    ],
    "nation": [
        "N_NATIONKEY",
        "N_NAME",
        "N_REGIONKEY",
        "N_COMMENT",
    ],
    "region": [
        "R_REGIONKEY",
        "R_NAME",
        "R_COMMENT",
    ],
}


TPCH_SQLITE_TABLE_CREATION_SQL = [
    """
        CREATE TABLE NATION (
        N_NATIONKEY INTEGER PRIMARY KEY NOT NULL,
        N_NAME      TEXT NOT NULL,
        N_REGIONKEY INTEGER NOT NULL,
        N_COMMENT   TEXT,
        FOREIGN KEY (N_REGIONKEY) REFERENCES REGION(R_REGIONKEY)
        );
    """,
    """
        CREATE TABLE REGION (
        R_REGIONKEY INTEGER PRIMARY KEY NOT NULL,
        R_NAME      TEXT NOT NULL,
        R_COMMENT   TEXT
        );
    """,
    """
        CREATE TABLE PART (
        P_PARTKEY     INTEGER PRIMARY KEY NOT NULL,
        P_NAME        TEXT NOT NULL,
        P_MFGR        TEXT NOT NULL,
        P_BRAND       TEXT NOT NULL,
        P_TYPE        TEXT NOT NULL,
        P_SIZE        INTEGER NOT NULL,
        P_CONTAINER   TEXT NOT NULL,
        P_RETAILPRICE REAL NOT NULL,
        P_COMMENT     TEXT NOT NULL
        );
    """,
    """
        CREATE TABLE SUPPLIER (
        S_SUPPKEY   INTEGER PRIMARY KEY NOT NULL,
        S_NAME      TEXT NOT NULL,
        S_ADDRESS   TEXT NOT NULL,
        S_NATIONKEY INTEGER NOT NULL,
        S_PHONE     TEXT NOT NULL,
        S_ACCTBAL   REAL NOT NULL,
        S_COMMENT   TEXT NOT NULL,
        FOREIGN KEY (S_NATIONKEY) REFERENCES NATION(N_NATIONKEY)
        );
    """,
    """
        CREATE TABLE PARTSUPP (
        PS_PARTKEY    INTEGER NOT NULL,
        PS_SUPPKEY    INTEGER NOT NULL,
        PS_AVAILQTY   INTEGER NOT NULL,
        PS_SUPPLYCOST REAL NOT NULL,
        PS_COMMENT    TEXT NOT NULL,
        PRIMARY KEY (PS_PARTKEY, PS_SUPPKEY),
        FOREIGN KEY (PS_SUPPKEY) REFERENCES SUPPLIER(S_SUPPKEY),
        FOREIGN KEY (PS_PARTKEY) REFERENCES PART(P_PARTKEY)
        );
    """,
    """
        CREATE TABLE CUSTOMER (
        C_CUSTKEY    INTEGER PRIMARY KEY NOT NULL,
        C_NAME       TEXT NOT NULL,
        C_ADDRESS    TEXT NOT NULL,
        C_NATIONKEY  INTEGER NOT NULL,
        C_PHONE      TEXT NOT NULL,
        C_ACCTBAL    REAL   NOT NULL,
        C_MKTSEGMENT TEXT NOT NULL,
        C_COMMENT    TEXT NOT NULL,
        FOREIGN KEY (C_NATIONKEY) REFERENCES NATION(N_NATIONKEY)
        );
    """,
    """
        CREATE TABLE ORDERS (
        O_ORDERKEY      INTEGER PRIMARY KEY NOT NULL,
        O_CUSTKEY       INTEGER NOT NULL,
        O_ORDERSTATUS   TEXT NOT NULL,
        O_TOTALPRICE    REAL NOT NULL,
        O_ORDERDATE     DATE NOT NULL,
        O_ORDERPRIORITY TEXT NOT NULL,
        O_CLERK         TEXT NOT NULL,
        O_SHIPPRIORITY  INTEGER NOT NULL,
        O_COMMENT       TEXT NOT NULL,
        FOREIGN KEY (O_CUSTKEY) REFERENCES CUSTOMER(C_CUSTKEY)
        );
    """,
    """
        CREATE TABLE LINEITEM (
        L_ORDERKEY      INTEGER NOT NULL,
        L_PARTKEY       INTEGER NOT NULL,
        L_SUPPKEY       INTEGER NOT NULL,
        L_LINENUMBER    INTEGER NOT NULL,
        L_QUANTITY      REAL NOT NULL,
        L_EXTENDEDPRICE REAL NOT NULL,
        L_DISCOUNT      REAL NOT NULL,
        L_TAX           REAL NOT NULL,
        L_RETURNFLAG    TEXT NOT NULL,
        L_LINESTATUS    TEXT NOT NULL,
        L_SHIPDATE      DATE NOT NULL,
        L_COMMITDATE    DATE NOT NULL,
        L_RECEIPTDATE   DATE NOT NULL,
        L_SHIPINSTRUCT  TEXT NOT NULL,
        L_SHIPMODE      TEXT NOT NULL,
        L_COMMENT       TEXT NOT NULL,
        PRIMARY KEY (L_ORDERKEY, L_LINENUMBER),
        FOREIGN KEY (L_ORDERKEY) REFERENCES ORDERS(O_ORDERKEY),
        FOREIGN KEY (L_PARTKEY, L_SUPPKEY) REFERENCES PARTSUPP(PS_PARTKEY, PS_SUPPKEY)
        );
    """,
]


def gen_sqlite_db(csv_filepath: str, num_parts: int) -> str:
    """Generates a SQLite DB from a folder filled with generated CSVs

    Args:
        csv_filepath (str): path to folder with generated CSVs
        sqlite_db_file_path (str): path to write sqlite db to
        num_parts (int): number of parts of CSVs
    """
    sqlite_db_file_path = os.path.join(csv_filepath, "TPC-H.db")

    def import_table(table, table_path):
        if not os.path.isfile(table_path):
            raise FileNotFoundError(f"Did not find {table_path}")
        subprocess.check_output(
            shlex.split(f"sqlite3 {sqlite_db_file_path} -cmd '.mode csv' '.separator |' '.import {table_path} {table}'")
        )

    if not os.path.exists(sqlite_db_file_path):
        logger.info("Generating SQLite DB at %s", sqlite_db_file_path)
        con = sqlite3.connect(sqlite_db_file_path)
        cur = con.cursor()
        for creation_sql in TPCH_SQLITE_TABLE_CREATION_SQL:
            cur.execute(creation_sql)

        static_tables = ["nation", "region"]
        partitioned_tables = ["customer", "lineitem", "orders", "partsupp", "part", "supplier"]
        single_partition_tables = static_tables + partitioned_tables if num_parts == 1 else static_tables
        multi_partition_tables = [] if num_parts == 1 else partitioned_tables

        for table in single_partition_tables:
            import_table(table, f"{csv_filepath}/{table}.tbl")

        for table in multi_partition_tables:
            for part_idx in range(num_parts):
                import_table(table, f"{csv_filepath}/{table}.tbl.{part_idx}")
    else:
        logger.info("Cached SQLite DB already exists at %s, skipping SQLite DB generation", sqlite_db_file_path)

    return sqlite_db_file_path


def gen_csv_files(basedir: str, num_parts: int, scale_factor: float) -> str:
    """Generates CSV files

    Args:
        basedir (str): path to generate files into
        num_parts (int): number of parts to generate
        scale_factor (float): scale factor for generation

    Returns:
        str: path to folder with generated CSV files
    """
    cachedir = os.path.join(basedir, ("%.1f" % scale_factor).replace(".", "_"), str(num_parts))
    if not os.path.exists(cachedir):
        # If running in CI, use a scale factor of 0.2
        # Otherwise, check for SCALE_FACTOR env variable or default to 1
        logger.info("Cloning tpch dbgen repo")
        subprocess.check_output(shlex.split(f"git clone https://github.com/electrum/tpch-dbgen {cachedir}"))
        subprocess.check_output("make", cwd=cachedir)

        logger.info("Generating %s for tpch", num_parts)
        subprocess.check_output(shlex.split(f"./dbgen -v -f -s {scale_factor}"), cwd=cachedir)

        # The tool sometimes generates weird files with bad permissioning, we override that manually here
        subprocess.check_output(shlex.split("chmod -R u+rwx ."), cwd=cachedir)

        # Split the files manually
        if num_parts > 1:
            for tbl in ["customer", "lineitem", "orders", "partsupp", "part", "supplier"]:
                csv_file = os.path.join(cachedir, f"{tbl}.tbl")
                with open(csv_file) as f:
                    num_lines = sum(1 for _ in f)
                num_lines_per_part = math.ceil(num_lines / num_parts)
                with open(csv_file, "rb") as f:
                    for i in range(num_parts):
                        csv_part_file = os.path.join(cachedir, f"{tbl}.tbl.{i}")
                        with open(csv_part_file, "wb") as out:
                            for _ in range(num_lines_per_part):
                                out.write(f.readline())
                os.remove(csv_file)

        # The tool generates CSV files with a trailing delimiter, we get rid of that here
        csv_files = glob(f"{cachedir}/*.tbl*")
        for csv_file in csv_files:
            subprocess.check_output(shlex.split(f"sed -e 's/|$//' -i.bak {csv_file}"))
        backup_files = glob(f"{cachedir}/*.bak")
        for backup_file in backup_files:
            os.remove(backup_file)

    else:
        logger.info(
            "Cached directory for scale_factor=%s num_parts=%s already exists at %s, skipping CSV file generation",
            scale_factor,
            num_parts,
            cachedir,
        )

    return cachedir


def gen_parquet(csv_files_location: str) -> str:
    """Generates Parquet from generated CSV files

    Args:
        csv_files_location (str): path to folder with generated CSV files

    Returns:
        str: path to generated Parquet files
    """
    PARQUET_FILE_PATH = os.path.join(csv_files_location, "parquet")
    for tab_name in SCHEMA.keys():
        table_parquet_path = os.path.join(PARQUET_FILE_PATH, tab_name)
        if not os.path.exists(table_parquet_path):
            logger.info("Generating Parquet Files for %s", tab_name)
            df = daft.read_csv(
                os.path.join(csv_files_location, f"{tab_name}.tbl*"),
                has_headers=False,
                delimiter="|",
            ).exclude("")
            df = df.select(
                *[
                    df[autogen_col_name].alias(actual_col_name)
                    for actual_col_name, autogen_col_name in zip(SCHEMA[tab_name], df.column_names)
                ]
            )
            df = df.write_parquet(table_parquet_path)
            df.collect()
            assert os.path.exists(table_parquet_path), f"Parquet files not generated by Daft at {table_parquet_path}"
        else:
            logger.info(
                "Cached Parquet files for table %s already exists at %s, skipping Parquet file generation",
                tab_name,
                table_parquet_path,
            )
    return PARQUET_FILE_PATH


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--tpch_gen_folder",
        default="data/tpch-dbgen",
        help="Path to the folder containing the TPCH dbgen tool and generated data",
    )
    parser.add_argument("--scale_factor", default=10.0, help="Scale factor to run on in GB", type=float)
    parser.add_argument(
        "--num_parts", default=None, help="Number of parts to generate (defaults to 1 part per GB)", type=int
    )
    parser.add_argument(
        "--generate_sqlite_db", action="store_true", help="Whether to generate a SQLite DB or not, defaults to False"
    )
    parser.add_argument(
        "--generate_parquet", action="store_true", help="Whether to generate Parquet files or not, defaults to False"
    )
    args = parser.parse_args()
    num_parts = math.ceil(args.scale_factor) if args.num_parts is None else args.num_parts

    logger.info(
        "Generating data at %s with: scale_factor=%s num_parts=%s generate_sqlite_db=%s generate_parquet=%s",
        args.tpch_gen_folder,
        args.scale_factor,
        num_parts,
        args.generate_sqlite_db,
        args.generate_parquet,
    )

    csv_folder = gen_csv_files(basedir=args.tpch_gen_folder, scale_factor=args.scale_factor, num_parts=num_parts)
    if args.generate_parquet:
        gen_parquet(csv_folder)
    if args.generate_sqlite_db:
        gen_sqlite_db(csv_folder, num_parts)
