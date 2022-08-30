#! /bin/bash

TPCH_SQLITE_DIR="data/tpch-sqlite"

git clone --recursive git@github.com:lovasoa/TPCH-sqlite.git data/tpch-sqlite
cd $TPCH_SQLITE_DIR
SCALE_FACTOR=${1:-1} make

# Split files if SCALE_FACTOR is more than 1gb
if (($SCALE_FACTOR > 1)); then
    for tbl in tpch-dbgen/*.tbl; do
        tbl_part_prefix="$tbl."
        linecount=$(wc -l $tbl | awk '{print $1}')
        lines_per_file=$(($linecount / $SCALE_FACTOR + 1))
        split -d -l $lines_per_file $tbl $tbl_part_prefix
    done
fi
