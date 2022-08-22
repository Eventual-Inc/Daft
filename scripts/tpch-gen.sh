#! /bin/bash

TPCH_SQLITE_DIR="data/tpch-sqlite"
SCALE_FACTOR

if [ ! -d "$TPCH_SQLITE_DIR" ]; then
    git clone --recursive git@github.com:lovasoa/TPCH-sqlite.git data/tpch-sqlite
fi
cd $TPCH_SQLITE_DIR
SCALE_FACTOR=$${1:-1} make
