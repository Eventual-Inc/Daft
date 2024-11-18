import csv
import sys
from pathlib import Path

file = Path(sys.argv[1])
assert file.exists()

output = Path(sys.argv[2])
assert not output.exists()


def make_md_row(row: list[str]) -> str:
    return f'|{"|".join(row)}|\n'


with open(file) as file:
    with open(output, "w+") as output:
        csv_reader = csv.reader(file)
        header = next(csv_reader)

        header_str = make_md_row(header)
        output.write(header_str)

        separator_str = make_md_row(["-"] * len(header))
        output.write(separator_str)

        for row in csv_reader:
            row_str = make_md_row(row)
            output.write(row_str)
