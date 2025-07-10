import importlib
import inspect
import re


def get_first_line_docstring(obj):
    doc = inspect.getdoc(obj)
    if doc:
        return doc.strip().split("\n")[0]
    return ""


def make_table(rows):
    if not rows:
        return []
    # Sort rows alphabetically by method name (the text between [ and ])
    sorted_rows = sorted(
        rows, key=lambda row: re.search(r"\[`([^`]+)`", row).group(1) if re.search(r"\[`([^`]+)`", row) else ""
    )
    table = ["", "<!-- BEGIN GENERATED TABLE -->", "| Expression | Description |", "|------------|-------------|"]
    table += sorted_rows
    table.append("<!-- END GENERATED TABLE -->")
    return [line + "\n" for line in table]


def is_class_method_section(section_header):
    """Check if this section should be treated as class methods."""
    class_method_sections = ["Constructors", "Generic", "Numeric", "Logical", "Aggregation"]
    section_name = section_header.replace("## ", "").strip()
    return section_name in class_method_sections


def process_markdown(md_path):
    with open(md_path) as f:
        lines = f.readlines()

    out_lines = []
    i = 0

    # Copy intro section
    while i < len(lines) and not lines[i].startswith("## "):
        out_lines.append(lines[i])
        i += 1

    # Process sections
    while i < len(lines):
        line = lines[i]

        if line.startswith("## "):
            section_name = line.replace("## ", "").strip()
            out_lines.append(line)
            i += 1

            # Only process the 5 specific sections
            if section_name in ["Constructors", "Generic", "Numeric", "Logical", "Aggregation"]:
                # Collect expressions efficiently
                exprs = []
                while i < len(lines) and not lines[i].startswith("## "):
                    if lines[i].strip().startswith(":::"):
                        match = re.match(r"^::: ([\w\.]+)", lines[i])
                        if match:
                            exprs.append(match.group(1))
                    i += 1

                if exprs:
                    # Generate table efficiently
                    table_rows = []
                    sorted_exprs = sorted(exprs, key=lambda x: x.split(".")[-1])

                    for expr in sorted_exprs:
                        parts = expr.split(".")
                        if len(parts) >= 3 and parts[-2].endswith("Expression"):
                            # Class method - cache module and class
                            module_path = ".".join(parts[:-2])
                            method_name = parts[-1]
                            mod = importlib.import_module(module_path)
                            cls = getattr(mod, parts[-2])
                            method = getattr(cls, method_name)
                            first_line = get_first_line_docstring(method)
                            table_rows.append(f"| [`{method_name}`][{expr}] | {first_line} |")
                        else:
                            # Regular function
                            module_path, func_name = expr.rsplit(".", 1)
                            mod = importlib.import_module(module_path)
                            func = getattr(mod, func_name)
                            first_line = get_first_line_docstring(func)
                            table_rows.append(f"| [`{func_name}`][{expr}] | {first_line} |")

                    # Add table and sorted expressions efficiently
                    out_lines.extend(make_table(table_rows))
                    out_lines.append("\n")
                    out_lines.extend(f"::: {expr}\n" for expr in sorted_exprs)
                    out_lines.append("\n")
            else:
                # Copy other sections as-is
                while i < len(lines) and not lines[i].startswith("## "):
                    out_lines.append(lines[i])
                    i += 1
        else:
            out_lines.append(line)
            i += 1

    with open(md_path, "w") as f:
        f.writelines(out_lines)


if __name__ == "__main__":
    process_markdown("docs/api/expressions.md")
