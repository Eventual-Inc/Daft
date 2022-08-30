#! /bin/bash

TPCH_SQLITE_DIR="data/tpch-sqlite"
TPCH_DBGEN_DIR="data/tpch-dbgen"
db="data/TPC-H.db"
SCALE_FACTOR=$1

git clone --recursive git@github.com:lovasoa/TPCH-sqlite.git $TPCH_SQLITE_DIR
git clone https://github.com/electrum/tpch-dbgen $TPCH_DBGEN_DIR

# Create SQLite database
sqlite3 "$db" < $TPCH_SQLITE_DIR/sqlite-ddl.sql

# Build binaries for data generation
cd $TPCH_DBGEN_DIR
make

# Generate all the files in parts
numparts=$(echo "($1 / 1) + (($1 % 1) > 0)" | bc)
echo Generating total number of parts: $numparts

for (( partidx=1; partidx<=$numparts; partidx++ ))
do
    echo Generating part $partidx
   ./dbgen -v -f -s $SCALE_FACTOR -S $partidx -C 4
done

# Load each table into SQLite
cd ../..

import_sqlite () {
    echo "Importing table '$1' part $2..." >&2
    data_file="$TPCH_DBGEN_DIR/$1.tbl"

    # If $2 is provided, this is the part index - we search for a file suffixed by .$2
    if [ ! -z $2 ]; then
        data_file="$data_file.$2"
    fi

    if [ ! -e "$data_file" ]; then
        echo "'$data_file' doesnt exist. Skipping..." >&2
        RET_CODE=1
        continue
    fi

    fifo=$(mktemp -u)
    mkfifo $fifo
    sed -e 's/|$//' < "$data_file" > "$fifo" &
    (
        echo ".mode csv";
        echo ".separator |";
        echo -n ".import $fifo ";
        echo $1 | tr a-z A-Z;
    ) | sqlite3 "$db"
    rm $fifo

    if [ $? != 0 ]; then
        echo "Import failed." >&2
        RET_CODE=1
    fi
}

STATIC_TABLES=( nation region )
TABLES=( customer lineitem orders partsupp part supplier )

for table in ${STATIC_TABLES[@]}; do
    import_sqlite $table
done

for table in ${TABLES[@]}; do
    for (( partidx=1; partidx<=$numparts; partidx++ )); do
        import_sqlite $table $partidx
    done
done
