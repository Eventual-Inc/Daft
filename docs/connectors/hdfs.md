# Reading from and Writing to HDFS

Daft supports reading and writing data to [HDFS](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html) (Hadoop Distributed File System) and understands natively the URL protocol `hdfs://` as referring to data that resides in an HDFS cluster.

Under the hood, Daft accesses HDFS through [OpenDAL](https://opendal.apache.org/)'s `services-hdfs` backend, which talks to the HDFS name node via the JNI-based `libhdfs` client.

!!! warning "HDFS support is an opt-in build feature"

    Unlike the object store backends, the HDFS backend links against a JVM (`libhdfs`) and is therefore not compiled into Daft by default. To use it you must build Daft from source with the `hdfs` feature enabled, and have a JDK and Hadoop native libraries available at build and runtime. The JVM shared library must be discoverable by the dynamic linker:

    ```bash
    export JAVA_HOME=/path/to/jdk
    export CLASSPATH=$(hadoop classpath --glob)

    # The dynamic linker must be able to find libjvm. Locate the directory that
    # contains it under $JAVA_HOME (e.g. `find $JAVA_HOME -name 'libjvm.*'`) and
    # add it, along with the Hadoop native libs, to the library path.
    # Example (macOS, JDK 8):
    export DYLD_LIBRARY_PATH="$JAVA_HOME/jre/lib/server:/path/to/hadoop/lib/native"
    # On Linux, use LD_LIBRARY_PATH instead of DYLD_LIBRARY_PATH.

    maturin develop --features "python,hdfs"
    ```

    If HDFS support is not compiled in, Daft emits a warning and the `hdfs://` scheme will not be available.

    These variables are also required at runtime (not just at build time) so the JVM can be loaded.

## URL Format

URLs to data in HDFS come in the form:

```text
hdfs://{NAMENODE_HOST}:{NAMENODE_PORT}/{PATH}
```

- `NAMENODE_HOST:NAMENODE_PORT` identifies the HDFS name node. URLs are always written in this full form: setting `name_node` in the config changes which address Daft connects to (the config value takes precedence), but it does not make the URL authority optional.
- `PATH` is the absolute path to the file or directory inside the HDFS namespace.

## Specifying the Name Node

In most cases the URL authority is all you need: it identifies the name node, and no config object is required. Pass an [`HdfsConfig`][daft.io.HdfsConfig] via a [`daft.io.IOConfig`][daft.io.IOConfig] only when you want behavior the URL cannot express, such as a custom root.

=== "Name Node from URL"

    ```python
    import daft

    # The name node is taken from the URL authority, so no explicit config is needed.
    df = daft.read_parquet("hdfs://namenode:9000/my_path/**/*")
    ```

=== "Custom Root"

    ```python
    import daft
    from daft.io import IOConfig, HdfsConfig

    io_config = IOConfig(
        hdfs=HdfsConfig(
            name_node="hdfs://namenode:9000",
            # Prefix applied to all paths: URL path {path} maps to /warehouse/{path} in HDFS.
            root="/warehouse",
        )
    )

    # Actually reads from /warehouse/table/**/* in HDFS.
    df = daft.read_parquet("hdfs://namenode:9000/table/**/*", io_config=io_config)
    ```

An `IOConfig` can be passed per operation through the `io_config=` keyword argument as shown above, or set globally for all subsequent calls with [`daft.set_planning_config`][daft.context.set_planning_config].

## Writing Data

Daft supports writing data to HDFS:

```python
import daft

df = daft.from_pydict({"a": [1, 2, 3], "b": ["x", "y", "z"]})

# URL authority serves as the name node — no explicit config required.
df.write_parquet("hdfs://namenode:9000/output/")
```

If you need to set a custom root or override the name node, pass an [`HdfsConfig`][daft.io.HdfsConfig] via `io_config=`, the same way as in the read examples above.

## Configuration Options

The [`HdfsConfig`][daft.io.HdfsConfig] object supports the following options:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `name_node` | `str` | `None` | Name node address in `scheme://host:port` format, e.g. `"hdfs://namenode:9000"`. If not set, the URL authority is used. |
| `root` | `str` | `None` (`"/"`) | Path prefix for all operations: the URL path is resolved as `{root}/{path}` inside HDFS. Useful for factoring out a common prefix or switching environments (e.g. `/dev` vs `/prod`) without changing URLs. |

!!! tip "Name node resolution"

    - If no `name_node` is set in the config, the URL authority (`hdfs://host:port/...`) is used as the name node.
    - If `name_node` is set explicitly, it takes precedence over the URL authority: connections always go to the configured address, and a mismatching URL authority is silently ignored rather than reported as an error.
    - Without a configured `name_node`, the URL must include a host. With an explicit `name_node`, authority-free URLs such as `hdfs:///path` work for reads, but writes currently still require the URL to include a host.

## Supported Operations

Daft supports the following operations with HDFS:

- **Read**: `read_parquet`, `read_csv`, `read_json`, and other file readers
- **Write**: `write_parquet`, `write_csv`, `write_json`
- **List**: Listing files with glob pattern matching
- **Delete**: Deleting files
