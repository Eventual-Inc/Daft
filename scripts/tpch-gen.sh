#! /bin/bash

TPCH_SQLITE_DIR="data/tpch-sqlite"

git clone --recursive git@github.com:lovasoa/TPCH-sqlite.git data/tpch-sqlite
cd $TPCH_SQLITE_DIR
SCALE_FACTOR=${1:-1} make
