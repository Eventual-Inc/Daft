import importlib
import inspect
import re
from functools import lru_cache
from pathlib import Path
from typing import Optional

EXPRESSIONS_SECTIONS = {"constructors", "generic", "numeric", "logical", "aggregation"}
IO_SECTIONS = {"input", "output"}
AGG_SECTIONS = {"dataframe aggregations"}
CONFIG_SECTIONS = {"setting the runner", "setting configurations"}

SECTION_MAP = {
    "expressions": EXPRESSIONS_SECTIONS,
    "io": IO_SECTIONS,
    "agg": AGG_SECTIONS,
    "config": CONFIG_SECTIONS,
}

# File configurations
FILE_CONFIGS = [
    ("docs/api/io.md", IO_SECTIONS),
    ("docs/api/dataframe.md", None),
    ("docs/api/expressions.md", EXPRESSIONS_SECTIONS),
    ("docs/api/aggregations.md", AGG_SECTIONS),
    ("docs/api/config.md", CONFIG_SECTIONS),
]


@lru_cache(maxsize=256)
def get_function_info(expr: str) -> tuple[str, str]:
    """Get function name and docstring first line. Cached to avoid repeated imports."""
    if not expr.startswith("daft."):
        func_name = expr.split(".")[-1]
        return func_name, "[Invalid expression: must start with 'daft.']"

    try:
        parts = expr.split(".")

        if len(parts) >= 3 and parts[-2][0].isupper():
            # Class method (e.g., daft.DataFrame.method_name)
            module_path = ".".join(parts[:-2])
            class_name = parts[-2]
            method_name = parts[-1]
            mod = importlib.import_module(module_path)
            cls = getattr(mod, class_name)
            method = getattr(cls, method_name)
            doc = inspect.getdoc(method)
            return method_name, doc.strip().split("\n")[0] if doc else ""
        else:
            # Function (e.g., daft.io.read_csv)
            module_path, func_name = expr.rsplit(".", 1)
            mod = importlib.import_module(module_path)
            func = getattr(mod, func_name)
            doc = inspect.getdoc(func)
            return func_name, doc.strip().split("\n")[0] if doc else ""
    except (ImportError, AttributeError) as e:
        func_name = expr.split(".")[-1]
        raise ImportError(f"Could not import {expr}: {e}") from e


def make_table(expressions: list[str]) -> list[str]:
    """Generate markdown table from sorted expressions."""
    if not expressions:
        return []

    table = [
        "<!-- BEGIN GENERATED TABLE -->",
        "| Method | Description |",
        "|--------|-------------|",
    ]

    # Sort by function name and generate table rows
    sorted_exprs = sorted(expressions, key=lambda x: x.split(".")[-1])

    for expr in sorted_exprs:
        func_name, description = get_function_info(expr)
        table.append(f"| [`{func_name}`][{expr}] | {description} |")

    table.append("<!-- END GENERATED TABLE -->")
    return [line + "\n" for line in table]


def process_section(lines: list[str], start_idx: int, section_filter: Optional[set[str]]) -> tuple[list[str], int]:
    """Process a single section starting at start_idx. Returns (processed_lines, next_idx)."""
    i = start_idx
    heading_line = lines[i]
    heading = heading_line.lstrip("#").strip()

    # Skip section if not in filter
    if section_filter and heading.lower() not in section_filter:
        section_lines = [heading_line]
        i += 1
        while i < len(lines) and not lines[i].startswith("#"):
            section_lines.append(lines[i])
            i += 1
        return section_lines, i

    # Process section
    result_lines = [heading_line]
    i += 1

    # Collect intro lines (before table or :::)
    intro_lines = []
    while i < len(lines) and not lines[i].startswith("#"):
        line = lines[i]
        if line.strip().startswith("<!-- BEGIN GENERATED TABLE") or line.strip().startswith(":::"):
            break
        intro_lines.append(line)
        i += 1

    # Skip existing generated table - always regenerate completely
    if i < len(lines) and lines[i].strip().startswith("<!-- BEGIN GENERATED TABLE"):
        while i < len(lines) and not lines[i].strip().startswith("<!-- END GENERATED TABLE"):
            i += 1
        if i < len(lines):
            i += 1

    # Collect ALL expressions from remaining content (this handles incremental additions)
    expressions = []

    while i < len(lines) and not lines[i].startswith("#"):
        line = lines[i]
        if line.strip().startswith(":::"):
            match = re.match(r"^::: (daft\.[\w\.]+)", line)
            if match:
                expressions.append(match.group(1))
        i += 1

    # Build result with regenerated table and sorted content
    result_lines.extend(intro_lines)

    if expressions:
        # Add spacing before table if needed
        if result_lines and result_lines[-1].strip():
            result_lines.append("\n")

        # Generate table with ALL expressions (old + new), sorted alphabetically
        result_lines.extend(make_table(expressions))
        result_lines.append("\n")

        # Add sorted ::: blocks - this ensures the entire list is alphabetized
        sorted_exprs = sorted(expressions, key=lambda x: x.split(".")[-1])
        result_lines.extend(f"::: {expr}\n" for expr in sorted_exprs)
        result_lines.append("\n")

    return result_lines, i


def process_markdown(md_path: str, process_sections: Optional[set[str]] = None) -> None:
    """Process a single markdown file."""
    path = Path(md_path)
    if not path.exists():
        print(f"Warning: File {md_path} not found")
        return

    # Read file
    with open(path, encoding="utf-8") as f:
        lines = f.readlines()

    # Set up section filter
    section_filter = None if process_sections is None else {s.lower() for s in process_sections}

    # Process file
    result_lines = []
    i = 0

    while i < len(lines):
        if lines[i].startswith("#"):
            # Process section
            section_lines, next_i = process_section(lines, i, section_filter)
            result_lines.extend(section_lines)
            i = next_i
        else:
            # Copy non-heading lines as-is
            result_lines.append(lines[i])
            i += 1

    # Write result
    with open(path, "w", encoding="utf-8") as f:
        f.writelines(result_lines)


def process_all_files() -> None:
    """Process all configured markdown files."""
    for file_path, sections in FILE_CONFIGS:
        print(f"Processing {file_path}...")
        process_markdown(file_path, sections)


if __name__ == "__main__":
    process_all_files()
