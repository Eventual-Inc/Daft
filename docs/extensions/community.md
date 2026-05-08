# Community Extensions

!!! tip "Want to build your own extension?"

    Start with the [Extensions overview](overview.md), the [Python UDF docs](../custom-code/func.md),
    or the [Native Extension Authoring Guide](authoring.md).

Community extensions are reusable packages that add functionality to Daft
outside of the core repository. They may be pure-Python UDF-based extensions,
native ABI extensions, or higher-level domain libraries that combine Daft
expressions, UDFs, file types, model calls, and distributed execution patterns.

Projects listed here are maintained independently of Daft's release cadence.
Each project installs and versions independently of Daft.

| Name | Kind | Repository | Description |
|---|---|---|---|
| [daft-h3](#daft-h3) | Native ABI | [gweaverbiodev/daft-h3](https://github.com/gweaverbiodev/daft-h3) | Native [H3](https://h3geo.org/) geospatial indexing functions. |
| [daft-lance](#daft-lance) | Python UDF-based | [daft-engine/daft-lance](https://github.com/daft-engine/daft-lance) | Lance-specific distributed operations for compaction, scalar indexing, column merging, and REST catalog operations. |
| [daft-html](#daft-html) | Native ABI | [daft-engine/daft-html](https://github.com/daft-engine/daft-html) | Native HTML document processing functions exposed as Daft expressions. |
| [daft-geo](#daft-geo) | Native ABI / Datatypes | [daft-engine/daft-geo](https://github.com/daft-engine/daft-geo) | Geospatial prototype showing native functions and extension-backed datatypes. |

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
