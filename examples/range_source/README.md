# range_source

A simple Daft extension source that generates integer ranges.

## Build & Install

```bash
cd examples/range_source
pip install -e ".[test]"
```

## Usage

```python
import daft
import range_source

daft.load_extension(range_source)
df = range_source.range(n=1000, partitions=4)
df.show()
```

## Test

```bash
pytest tests/
```
