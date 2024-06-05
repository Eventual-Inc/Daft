# Arrow2

Arrow2 is a Rust library that implements data structures and functionality enabling
interoperability with the arrow format.

The typical use-case for this library is to perform CPU and memory-intensive analytics in a format that supports heterogeneous data structures, null values, and IPC and FFI interfaces across languages.

Arrow2 is divided in 5 main APIs:

* a [low-level API](./low_level.md) to efficiently operate with contiguous memory regions
* a [high-level API](./high_level.md) to operate with arrow arrays
* a [metadata API](./metadata.md) to declare and operate with logical types and metadata
* a [compute API](./compute.md) with operators to operate over arrays
* an IO API with interfaces to read from, and write to, other formats
    * Arrow
        * [Read files](./io/ipc_read.md)
        * [Read streams](./io/ipc_stream_read.md)
        * [Memory map files](./io/ipc_mmap.md)
        * [Write](./io/ipc_write.md)
    * CSV
        * [Read](./io/csv_read.md)
        * [Write](./io/csv_write.md)
    * Parquet
        * [Read](./io/parquet_read.md)
        * [Write](./io/parquet_write.md)
    * JSON and NDJSON
        * [Read](./io/json_read.md)
        * [Write](./io/json_write.md)
    * Avro
        * [Read](./io/avro_read.md)
        * [Write](./io/avro_write.md)
    * ODBC
        * [Read and write](./io/odbc.md)
