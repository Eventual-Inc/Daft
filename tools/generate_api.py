import importlib
import inspect
import re

EXPRESSIONS_SECTIONS = {"Constructors", "Generic", "Numeric", "Logical", "Aggregation"}


def get_first_line_docstring(obj):
    doc = inspect.getdoc(obj)
    if doc:
        return doc.strip().split("\n")[0]
    return ""


def make_table(rows, table_type):
    if not rows:
        return []
    header = "| Method | Description |" if table_type == "dataframe" else "| Expression | Description |"
    table = [
        "",
        "<!-- BEGIN GENERATED TABLE -->",
        header,
        "|--------|-------------|" if table_type == "dataframe" else "|------------|-------------|",
    ]
    table += sorted(
        rows, key=lambda row: re.search(r"\[`([^`]+)`", row).group(1) if re.search(r"\[`([^`]+)`", row) else ""
    )
    table.append("<!-- END GENERATED TABLE -->")
    return [line + "\n" for line in table]


def process_markdown(md_path, table_type):
    with open(md_path) as f:
        lines = f.readlines()

    out_lines = []
    i = 0
    while i < len(lines):
        line = lines[i]
        # Detect heading
        if line.startswith("#"):
            heading = line.lstrip("#").strip()
            out_lines.append(line)
            i += 1

            # Collect section content
            exprs = []
            section_content = []
            while i < len(lines) and not lines[i].startswith("#"):
                if lines[i].strip().startswith(":::"):
                    match = re.match(r"^::: ([\w\.]+)", lines[i])
                    if match:
                        exprs.append(match.group(1))
                section_content.append(lines[i])
                i += 1

            # For expressions.md, only process the 5 named sections
            if table_type == "expressions":
                if heading not in EXPRESSIONS_SECTIONS:
                    out_lines.extend(section_content)
                    continue

            if exprs:
                # Generate table
                table_rows = []
                sorted_exprs = sorted(exprs, key=lambda x: x.split(".")[-1])
                for expr in sorted_exprs:
                    parts = expr.split(".")
                    if len(parts) >= 3 and (parts[-2].endswith("Expression") or parts[-2].endswith("DataFrame")):
                        module_path = ".".join(parts[:-2])
                        method_name = parts[-1]
                        mod = importlib.import_module(module_path)
                        cls = getattr(mod, parts[-2])
                        method = getattr(cls, method_name)
                        first_line = get_first_line_docstring(method)
                        table_rows.append(f"| [`{method_name}`][{expr}] | {first_line} |")
                    else:
                        module_path, func_name = expr.rsplit(".", 1)
                        mod = importlib.import_module(module_path)
                        func = getattr(mod, func_name)
                        first_line = get_first_line_docstring(func)
                        table_rows.append(f"| [`{func_name}`][{expr}] | {first_line} |")
                out_lines.extend(make_table(table_rows, table_type))
                out_lines.append("\n")
                out_lines.extend(f"::: {expr}\n" for expr in sorted_exprs)
                out_lines.append("\n")
            else:
                out_lines.extend(section_content)
        else:
            out_lines.append(line)
            i += 1

    with open(md_path, "w") as f:
        f.writelines(out_lines)


if __name__ == "__main__":
    files_to_process = [
        ("docs/api/dataframe.md", "dataframe"),
        ("docs/api/expressions.md", "expressions"),
    ]
    for md_path, table_type in files_to_process:
        process_markdown(md_path, table_type)
