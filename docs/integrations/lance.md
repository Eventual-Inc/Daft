# LanceDB

[LanceDB](https://github.com/lancedb/lancedb) is an open-source database for vector-search built with persistent storage, which greatly simplifies retrieval, filtering and management of embeddings.

Daft currently supports reading from and writing to a LanceDB table. To use Daft with LanceDB, you will need to install Daft with the 'lance' option specific like so:

```bash
pip install daft[lance]
```

## Create a DataFrame from a LanceDB table

You can create a Daft DataFrame by reading from a local LanceDB table with [`read_lance`][daft.read_lance]:

```python
df = daft.read_lance("/tmp/lance/my_table.lance")
df.show()
```
``` {title=Output}
╭───────╮
│ a     │
│ ---   │
│ Int64 │
╞═══════╡
│ 1     │
├╌╌╌╌╌╌╌┤
│ 2     │
├╌╌╌╌╌╌╌┤
│ 3     │
├╌╌╌╌╌╌╌┤
│ 4     │
╰───────╯

(Showing first 4 of 4 rows)
```

Likewise, you can also create a Daft DataFrame from reading a LanceDB table from a public S3 bucket:

```python
from daft.io import S3Config
s3_config = S3Config(region="us-west-2", anonymous=True)
df = daft.read_lance("s3://daft-public-data/lance/words-test-dataset", io_config=s3_config)
df.show()
```

## Write to a LanceDB table

You can write a Daft DataFrame to a LanceDB table with [`write_lance`][daft.DataFrame.write_lance]

```python
import daft
df = daft.from_pydict({"a": [1, 2, 3, 4]})
df.write_lance("/tmp/lance/my_table.lance")
```
``` {title=Output}
╭───────────────┬──────────────────┬─────────────────┬─────────╮
│ num_fragments ┆ num_deleted_rows ┆ num_small_files ┆ version │
│ ---           ┆ ---              ┆ ---             ┆ ---     │
│ Int64         ┆ Int64            ┆ Int64           ┆ Int64   │
╞═══════════════╪══════════════════╪═════════════════╪═════════╡
│ 1             ┆ 0                ┆ 1               ┆ 1       │
╰───────────────┴──────────────────┴─────────────────┴─────────╯

(Showing first 1 of 1 rows)
```
