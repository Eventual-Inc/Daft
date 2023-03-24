from __future__ import annotations

import os


def get_asset_dir() -> str:
    dir_path = os.path.dirname(os.path.realpath(__file__))
    return dir_path


ASSET_FOLDER = get_asset_dir()

TPCH_QUERIES = f"{ASSET_FOLDER}/tpch-sqlite-queries"
TPCH_DBGEN_DIR = f"{ASSET_FOLDER}/../../data/tpch-dbgen"
