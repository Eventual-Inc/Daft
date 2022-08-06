#! /bin/bash

mkdir -p data
git clone https://github.com/electrum/tpch-dbgen.git data/tpch
cd data/tpch

# Patch files if running on mac
if [ "$(uname)" == "Darwin" ]; then
    sed -i '' 's;<malloc.h>;<sys/malloc.h>;g' bm_utils.c
    sed -i '' 's;<malloc.h>;<sys/malloc.h>;g' varsub.c
fi

make
./dbgen -s 1 -f

for I in *.tbl; do sed 's/|$//' $I > ${I/tbl/csv}; done;
for I in *.csv; do sed -i '' 's/|/,/g' $I ; done;
