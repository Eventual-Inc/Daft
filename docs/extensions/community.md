# Community Extensions

!!! tip "Want to build your own extension?"

    Start with the [Extensions overview](overview.md), the [Python UDF docs](../custom-code/func.md),
    or the [Native Extension Authoring Guide](authoring.md).

Community extensions are reusable packages that add functionality to Daft
outside of the core repository. They may be pure-Python UDF-based extensions,
native ABI extensions, custom DataSource/DataSink connectors, or higher-level
domain libraries that combine Daft expressions, UDFs, file types, model calls,
and distributed execution patterns.

Projects listed here are maintained independently of Daft's release cadence.
Each project installs and versions independently of Daft.

| Name | Kind | Repository | Description |
|---|---|---|---|
| [daft-h3](#daft-h3) | Native ABI | [gweaverbiodev/daft-h3](https://github.com/gweaverbiodev/daft-h3) | Native [H3](https://h3geo.org/) geospatial indexing functions. |
| [daft-lance](#daft-lance) | Python UDF-based | [daft-engine/daft-lance](https://github.com/daft-engine/daft-lance) | Lance-specific distributed operations for compaction, scalar indexing, column merging, and REST catalog operations. |
| [daft-html](#daft-html) | Native ABI | [daft-engine/daft-html](https://github.com/daft-engine/daft-html) | Native HTML document processing functions exposed as Daft expressions. |
| [daft-geo](#daft-geo) | Native ABI / Datatypes | [daft-engine/daft-geo](https://github.com/daft-engine/daft-geo) | Geospatial prototype showing native functions and extension-backed datatypes. |
| [daft-qdrant](#daft-qdrant) | Python UDF-based | [qdrant-labs/daft-qdrant](https://github.com/qdrant-labs/daft-qdrant) | Write vector embeddings and their payloads into [Qdrant](https://qdrant.tech/) collections. |
| [daft-doris](#daft-doris) | Custom DataSource/DataSink | [jiangxt2/daft-doris](https://github.com/jiangxt2/daft-doris) | Apache Doris reads and HTTP Stream Load writes for Daft. |

To propose a new extension for this list, open a PR against this page.

## daft-h3

`daft-h3` adds H3 geospatial indexing functions such as latitude/longitude to
cell conversion, cell traversal, grid distance, parent resolution, and string
conversion.

```bash
pip install daft-h3
```

```python
import daft
import daft_h3
from daft import col

daft.load_extension(daft_h3)

df = daft.from_pydict({"lat": [37.7749], "lng": [-122.4194]})
df = df.select(
    daft_h3.h3_latlng_to_cell(col("lat"), col("lng"), 7).alias("cell"),
)
df = df.select(daft_h3.h3_cell_to_str(col("cell")).alias("hex"))
df.show()
```

See the [daft-h3 README](https://github.com/gweaverbiodev/daft-h3#readme) for
the full list of functions and behavior details.

## daft-lance

`daft-lance` extends Daft with Lance-specific distributed maintenance and data
management operations. It is a Python UDF-based extension: internally, it uses
Daft's Python UDF and class-UDF APIs to distribute Lance tasks across Daft
queries, while users interact with simple Python functions.

```bash
pip install daft-lance
```

```python
from daft_lance import compact_files, create_scalar_index

compact_files("s3://bucket/my_dataset")

create_scalar_index(
    "s3://bucket/my_dataset",
    column="name",
    index_type="INVERTED",
)
```

See the [daft-lance README](https://github.com/daft-engine/daft-lance#readme)
for additional operations such as column merging and REST catalog writes.

## daft-html

`daft-html` adds native HTML processing functions such as `html_to_text`,
`html_extract_links`, `html_extract_tables`, and CSS-selector extraction.

```bash
pip install daft-html
```

```python
import daft
import daft_html
from daft import col
from daft_html import html_to_text

daft.load_extension(daft_html)

df = daft.from_pydict({"html": ["<html><body><h1>Hello</h1></body></html>"]})
df = df.select(html_to_text(col("html")).alias("text"))
df.show()
```

See the [daft-html README](https://github.com/daft-engine/daft-html#readme)
for the full list of document and CSS-selector functions.

## daft-geo

`daft-geo` is a geospatial prototype showing native Daft functions and
extension-backed datatypes. It defines `Point2D` and `Point3D` as
`DataType.extension(...)` values and provides point construction, accessors,
Euclidean distance, and haversine distance.

```bash
pip install git+https://github.com/daft-engine/daft-geo.git
```

!!! note

    `daft-geo` is not yet published to PyPI. The command above installs the
    latest commit from the repository's default branch, so behavior may change
    between installs.

```python
import daft
import daft_geo
from daft import col

daft.load_extension(daft_geo)

df = daft.from_pydict({"x": [0.0], "y": [1.0]})
df = df.select(daft_geo.point2d(col("x"), col("y")).alias("point"))
df = df.select(daft_geo.x(col("point")).alias("x"))
df.show()
```

See the [daft-geo README](https://github.com/daft-engine/daft-geo#readme) for
current capabilities and caveats.

## daft-qdrant

`daft-qdrant` adds a `write_qdrant` DataFrame method for writing vector
embeddings and their payloads into [Qdrant](https://qdrant.tech/) collections.

```bash
pip install daft-qdrant
```

```python
import daft
import daft_qdrant

df = daft.from_pydict({
    "id": [1, 2, 3],
    "vector": [[0.1, 0.2, 0.3], [0.4, 0.5, 0.6], [0.7, 0.8, 0.9]],
    "label": ["cat", "dog", "bird"],
})

df.write_qdrant("my-collection", url="http://localhost:6333")
```

See the [daft-qdrant README](https://github.com/qdrant-labs/daft-qdrant#readme) for embedding pipeline examples and additional options.

## daft-doris

`daft-doris` is an independently maintained Python custom connector for
Apache Doris. It supports reads from Doris physical tables and batch writes
through HTTP Stream Load. The read transport must be selected explicitly;
Flight SQL is available as an opt-in experimental transport.

```bash
pip install "daft-doris[doris]"
```

For Flight SQL read support, install the optional Flight extra:

```bash
pip install "daft-doris[doris-flight]"
```

Read a Doris table:

```python
from daft_doris import read_doris

df = read_doris(
    host="doris-fe.example",
    database="analytics",
    table="events",
    transport="mysql",
)
```

Write a Daft DataFrame through Stream Load:

```python
import daft

from daft_doris import DorisConnection, DorisTable, SecretRef, write_doris

df = daft.from_pydict(
    {
        "event_id": [1, 2],
        "score": [95.0, 88.0],
    }
)
result = write_doris(
    df,
    connection=DorisConnection(
        host="doris-fe.example.com",
        username="daft_writer",
        password=SecretRef.env("DORIS_PASSWORD"),
        http_port=8030,
        redirect_hosts=("doris-be.example.com",),
        redirect_ports=(8040,),
        redirect_policy="public",
    ),
    table=DorisTable(database="analytics", name="events"),
    operation="load",
)
```

The connector is currently in Alpha and is maintained independently of Daft's
release cadence. The current release supports Python 3.12–3.13 and Daft
0.7.23; its Daft dependency is bounded to `>=0.7.23,<0.7.24`, so installing it
alongside a newer Daft release may cause the resolver to install Daft 0.7.23.
See the [`daft-doris` repository](https://github.com/jiangxt2/daft-doris#readme)
for supported scope, compatibility information, and limitations.
