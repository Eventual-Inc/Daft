# Community Extensions

Community extensions are built by external contributors and maintained
independently of Daft's release cadence. Extensions listed here have been
reviewed by the Daft maintainers. Each is its own PyPI package, so install
and version independently of Daft.

| Name | Repository | Description |
|---|---|---|
| [daft-h3](#daft-h3) | [gweaverbiodev/daft-h3](https://github.com/gweaverbiodev/daft-h3) | Native [H3](https://h3geo.org/) geospatial indexing functions, maintained by [Garrett Weaver](https://github.com/gweaverbiodev). |

To propose a new extension for this list, open a PR against this page.

## daft-h3

```bash
pip install daft-h3
```

```python
import daft
import daft_h3
from daft import col
from daft.session import Session

sess = Session()
sess.load_extension(daft_h3)

with sess:
    df = daft.from_pydict({"lat": [37.7749], "lng": [-122.4194]})
    df = df.select(
        daft_h3.h3_latlng_to_cell(col("lat"), col("lng"), 7).alias("cell"),
    ).collect()
    df = df.select(daft_h3.h3_cell_to_str(col("cell")).alias("hex")).collect()
    df.show()
```

See the [daft-h3 README](https://github.com/gweaverbiodev/daft-h3#readme) for the full list of functions and behavior details.
