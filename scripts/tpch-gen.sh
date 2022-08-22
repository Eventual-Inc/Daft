#! /bin/bash

git clone --recursive git@github.com:lovasoa/TPCH-sqlite.git data/tpch-sqlite
cd data/tpch-sqlite
SCALE_FACTOR=$1 make
