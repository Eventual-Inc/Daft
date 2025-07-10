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
    table = ["", "<!-- BEGIN GENERATED TABLE -->", "| Expression | Description |", "|------------|-------------|"]
    table += rows
    table.append("<!-- END GENERATED TABLE -->")
    return [line + "\n" for line in table]


def insert_or_replace_table(section_lines, new_table_lines):
    start_marker = "<!-- BEGIN GENERATED TABLE -->"
    end_marker = "<!-- END GENERATED TABLE -->"
    in_table = False
    result = []
    table_inserted = False
    for line in section_lines:
        if start_marker in line:
            in_table = True
            result.extend(new_table_lines)
            table_inserted = True
        elif end_marker in line:
            in_table = False
            continue  # skip the end marker, already added
        elif not in_table:
            result.append(line)
    if not table_inserted:
        # Insert table at the top of the section (after header)
        result = new_table_lines + result
    return result


def is_class_method_section(section_header):
    """Check if this section should be treated as class methods."""
    class_method_sections = ["constructors", "generic", "numerical", "logical", "aggregation"]
    section_name = section_header.lower().replace("## ", "").strip()
    return section_name in class_method_sections


def process_markdown(md_path):
    with open(md_path) as f:
        lines = f.readlines()

    # Find the end of the first paragraph (after the first blank line)
    out_lines = []
    i = 0
    while i < len(lines):
        out_lines.append(lines[i])
        if lines[i].strip() == "":
            i += 1
            break
        i += 1

    section_header = None
    section_exprs = []
    section_lines = []
    while i < len(lines):
        line = lines[i]
        header_match = re.match(r"^(## .+)", line)
        expr_match = re.match(r"^::: ([\w\.]+)", line)
        if header_match:
            # Process previous section
            if section_header and section_exprs:
                out_lines.append(section_header)
                table_rows = []
                is_class_method = is_class_method_section(section_header)

                if is_class_method:
                    for expr in section_exprs:
                        # Handle as class method (e.g., daft.expressions.expressions.Expression.apply)
                        parts = expr.split(".")
                        if len(parts) >= 3 and parts[-2].endswith("Expression"):
                            module_path = ".".join(parts[:-2])
                            class_name = parts[-2]
                            method_name = parts[-1]
                            mod = importlib.import_module(module_path)
                            cls = getattr(mod, class_name)
                            method = getattr(cls, method_name)
                            first_line = get_first_line_docstring(method)
                            table_rows.append(f"| [`{method_name}`][{expr}] | {first_line} |")
                        else:
                            # Fallback for regular functions in class method sections
                            module_path, func_name = expr.rsplit(".", 1)
                            mod = importlib.import_module(module_path)
                            func = getattr(mod, func_name)
                            first_line = get_first_line_docstring(func)
                            table_rows.append(f"| [`{func_name}`][{module_path}.{func_name}] | {first_line} |")

                    out_lines.extend(insert_or_replace_table(section_lines, make_table(table_rows)))
                else:
                    # For non-class method sections, just add the lines without processing
                    out_lines.extend(section_lines)
            # Start new section
            section_header = line
            section_exprs = []
            section_lines = []
        elif expr_match:
            section_exprs.append(expr_match.group(1))
            section_lines.append(line)
        else:
            section_lines.append(line)
        i += 1

    # Process the last section if any
    if section_header and section_exprs:
        out_lines.append(section_header)
        table_rows = []
        is_class_method = is_class_method_section(section_header)

        if is_class_method:
            for expr in section_exprs:
                # Handle as class method (e.g., daft.expressions.expressions.Expression.apply)
                parts = expr.split(".")
                if len(parts) >= 3 and parts[-2].endswith("Expression"):
                    module_path = ".".join(parts[:-2])
                    class_name = parts[-2]
                    method_name = parts[-1]
                    mod = importlib.import_module(module_path)
                    cls = getattr(mod, class_name)
                    method = getattr(cls, method_name)
                    first_line = get_first_line_docstring(method)
                    table_rows.append(f"| [`{method_name}`][{expr}] | {first_line} |")
                else:
                    # Fallback for regular functions in class method sections
                    module_path, func_name = expr.rsplit(".", 1)
                    mod = importlib.import_module(module_path)
                    func = getattr(mod, func_name)
                    first_line = get_first_line_docstring(func)
                    table_rows.append(f"| [`{func_name}`][{module_path}.{func_name}] | {first_line} |")

            out_lines.extend(insert_or_replace_table(section_lines, make_table(table_rows)))
        else:
            # For non-class method sections, just add the lines without processing
            out_lines.extend(section_lines)

    with open(md_path, "w") as f:
        f.writelines(out_lines)


if __name__ == "__main__":
    process_markdown("docs/api/expressions-2.md")
