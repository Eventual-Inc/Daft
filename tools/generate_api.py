import importlib
import inspect
import re

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


def get_first_line_docstring(obj):
    doc = inspect.getdoc(obj)
    if doc:
        return doc.strip().split("\n")[0]
    return ""


def make_table(rows):
    if not rows:
        return []
    table = [
        "<!-- BEGIN GENERATED TABLE -->",
        "| Method | Description |",
        "|--------|-------------|",
    ]
    table += sorted(
        rows, key=lambda row: re.search(r"\[`([^`]+)`", row).group(1) if re.search(r"\[`([^`]+)`", row) else ""
    )
    table.append("<!-- END GENERATED TABLE -->")
    return [line + "\n" for line in table]


def process_markdown(md_path, process_sections=None):
    # If process_sections is None, process all sections; otherwise, filter by the given set
    section_filter = None if process_sections is None else set(s.lower() for s in process_sections)
    with open(md_path) as f:
        lines = f.readlines()

    out_lines = []
    i = 0
    while i < len(lines):
        line = lines[i]
        # Detect heading
        if line.startswith("#"):
            heading = line.lstrip("#").strip()
            if section_filter and heading.lower() not in section_filter:
                out_lines.append(line)
                i += 1
                # Copy section content without processing
                while i < len(lines) and not lines[i].startswith("#"):
                    out_lines.append(lines[i])
                    i += 1
                continue
            out_lines.append(line)
            i += 1

            # Preserve all lines after heading up to next heading, table, or :::
            section_intro = []
            exprs = []
            while i < len(lines) and not lines[i].startswith("#"):
                current_line = lines[i]
                # Stop intro at start of table or :::
                if current_line.strip().startswith("<!-- BEGIN GENERATED TABLE") or current_line.strip().startswith(
                    ":::"
                ):
                    break
                section_intro.append(current_line)
                i += 1
            # Skip old generated table if present
            if i < len(lines) and lines[i].strip().startswith("<!-- BEGIN GENERATED TABLE"):
                while i < len(lines) and not lines[i].strip().startswith("<!-- END GENERATED TABLE"):
                    i += 1
                if i < len(lines):
                    i += 1
            # Collect section content after intro and old table
            section_lines = []
            while i < len(lines) and not lines[i].startswith("#"):
                current_line = lines[i]
                if current_line.strip().startswith(":::"):
                    match = re.match(r"^::: ([\w\.]+)", current_line)
                    if match:
                        exprs.append(match.group(1))
                section_lines.append(current_line)
                i += 1
            # Output: heading, intro, table, sorted ::: blocks
            out_lines.extend(section_intro)
            if exprs:
                if out_lines and out_lines[-1].strip() != "":
                    out_lines.append("\n")
                table_rows = []
                sorted_exprs = sorted(exprs, key=lambda x: x.split(".")[-1])
                for expr in sorted_exprs:
                    parts = expr.split(".")
                    if len(parts) >= 3 and parts[-2][0].isupper():
                        module_path = ".".join(parts[:-2])
                        class_name = parts[-2]
                        method_name = parts[-1]
                        mod = importlib.import_module(module_path)
                        cls = getattr(mod, class_name)
                        method = getattr(cls, method_name)
                        first_line = get_first_line_docstring(method)
                        table_rows.append(f"| [`{method_name}`][{expr}] | {first_line} |")
                    else:
                        module_path, func_name = expr.rsplit(".", 1)
                        mod = importlib.import_module(module_path)
                        func = getattr(mod, func_name)
                        first_line = get_first_line_docstring(func)
                        table_rows.append(f"| [`{func_name}`][{expr}] | {first_line} |")
                out_lines.extend(make_table(table_rows))
                if exprs:
                    out_lines.append("\n")
                # Output sorted ::: blocks
                out_lines.extend(f"::: {expr}\n" for expr in sorted_exprs)
                out_lines.append("\n")
        else:
            out_lines.append(line)
            i += 1

    with open(md_path, "w") as f:
        f.writelines(out_lines)


if __name__ == "__main__":
    # Add optional parameter for specifying sections to process
    process_markdown("docs/api/io.md", IO_SECTIONS)
    process_markdown("docs/api/dataframe.md")
    process_markdown("docs/api/expressions.md", EXPRESSIONS_SECTIONS)
    process_markdown("docs/api/aggregations.md", AGG_SECTIONS)
    process_markdown("docs/api/config.md", CONFIG_SECTIONS)
