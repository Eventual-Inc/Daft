from __future__ import annotations

import os


def get_asset_dir() -> str:
    dir_path = os.path.dirname(os.path.realpath(__file__))
    return dir_path


ASSET_FOLDER = get_asset_dir()

IRIS_CSV = f"{ASSET_FOLDER}/iris.csv"
SERVICE_REQUESTS_CSV = f"{ASSET_FOLDER}/311-service-requests.24.csv"
SERVICE_REQUESTS_CSV_FOLDER = f"{ASSET_FOLDER}/311-service-requests.24"
SERVICE_REQUESTS_PARTIAL_EMPTY_CSV_FOLDER = f"{ASSET_FOLDER}/311-service-requests.24.partial_empty"

SERVICE_REQUESTS_PARQUET = f"{ASSET_FOLDER}/311-service-requests.24.parquet"
SERVICE_REQUESTS_PARQUET_FOLDER = f"{ASSET_FOLDER}/311-service-requests.24.parquet_folder"
