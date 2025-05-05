from pathlib import Path

import daft
from daft.sql.sql import SQLCatalog


def generate_catalog(dir: Path):
    if not dir.exists():
        raise RuntimeError(f"Directory not found: {dir}")
    return SQLCatalog(
        tables={
            file.stem: daft.read_parquet(path=str(file))
            for file in dir.iterdir()
            if file.is_file() and file.suffix == ".parquet"
        }
    )


def parse_questions_str(questions: str) -> list[int]:
    if questions == "*":
        return list(range(1, 100))

    nums = set()
    for split in filter(lambda str: str, questions.split(",")):
        try:
            num = int(split)
            nums.add(num)
        except ValueError:
            ints = split.split("-")
            assert (
                len(ints) == 2
            ), f"A range must include two numbers split by a dash (i.e., '-'); instead got '{split}'"
            [lower, upper] = ints
            try:
                lower = int(lower)
                upper = int(upper)
                assert lower <= upper
                for index in range(lower, upper + 1):
                    nums.add(index)
            except ValueError:
                raise ValueError(f"Invalid range: {split}")

    return nums


def convert_decimals_to_float(parquet_path):
    """Convert decimal columns in a Parquet file to float64."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    table = pq.read_table(parquet_path)
    schema = table.schema

    new_fields = []
    needs_conversion = False
    for field in schema:
        if pa.types.is_decimal(field.type):
            needs_conversion = True
            new_fields.append(pa.field(field.name, pa.float64(), field.nullable))
        else:
            new_fields.append(field)

    if not needs_conversion:
        return False

    new_schema = pa.schema(new_fields)

    new_columns = []
    for i, col in enumerate(table.columns):
        if pa.types.is_decimal(schema[i].type):
            new_columns.append(col.cast(pa.float64()))
        else:
            new_columns.append(col)

    new_table = pa.Table.from_arrays(new_columns, schema=new_schema)
    pq.write_table(new_table, parquet_path)
    return True


def convert_all_tpcds_decimals(data_dir):
    """Convert all decimal columns to float64 in all TPC-DS Parquet files."""
    from pathlib import Path

    data_dir = Path(data_dir)
    converted = []

    for parquet_file in data_dir.glob("*.parquet"):
        if convert_decimals_to_float(parquet_file):
            converted.append(parquet_file.name)

    if converted:
        print(f"Converted decimal columns to float64 in: {', '.join(converted)}")
    else:
        print("No decimal columns found that needed conversion")
