# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "faker",
#     "pyarrow",
#     "typer",
# ]
# ///

import faker
import pyarrow as pa
import typer
from pyarrow import parquet as pq

fake = faker.Faker()


name_to_func = {
    "name": fake.name,
    "address": fake.address,
    "email": fake.email,
    "location": fake.latlng,
    "bs": fake.bs,
    "bool": fake.boolean,
    "time": fake.time_object,
    "city": fake.city,
    "state": fake.state,
    "date_time": fake.date_time,
    "paragraph": fake.paragraph,
    "nested_paragraph": fake.paragraphs,
    "Conrad": fake.catch_phrase,
    "image_url": fake.image_url,
    "credit_card": fake.credit_card_full,
    "uuid": fake.uuid4,
}


def create_rows(num=1):
    data = {}

    for name, func in name_to_func.items():
        data[name] = [func() for _ in range(num)]

    return data


def main(target_file: str, rows: int, row_group_size: int = 128 * 1024):
    typer.echo(f"Target file path: {target_file}")
    typer.echo(f"Target Rows: {rows}")
    typer.echo(f"Row Group Size: {row_group_size}")

    data = create_rows(8)
    table = pa.table(data)
    schema = table.schema

    file = pq.ParquetWriter(target_file, schema=schema, compression="zstd", store_schema=True)

    rows_written = 0
    while rows_written < rows:
        target_rows = min((rows - rows_written), row_group_size)
        data = create_rows(target_rows)
        table = pa.table(data)
        print(table)
        file.write_table(table)
        rows_written += len(table)

    file.close()


if __name__ == "__main__":
    typer.run(main)
