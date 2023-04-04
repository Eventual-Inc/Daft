from __future__ import annotations

import os


def get_asset_dir() -> str:
    dir_path = os.path.dirname(os.path.realpath(__file__))
    return dir_path


ASSET_FOLDER = get_asset_dir()

COOKBOOK_DATA_CSV = f"{ASSET_FOLDER}/311-service-requests.24.csv"
