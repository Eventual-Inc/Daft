#!/usr/bin/env python3
"""Convert doctest-style examples to markdown code blocks with separate output.

This script converts examples in docstrings from doctest format (>>>) to markdown
code blocks with separate output sections, improving copy-paste UX in documentation.

Usage:
    python tools/convert_doctest_to_markdown.py <file_path>
    python tools/convert_doctest_to_markdown.py daft/dataframe/dataframe.py
"""

import re
import sys
from pathlib import Path


def fix_double_empty_lines_in_code_blocks(content: str) -> str:
    """Fix double empty lines within Python code blocks.

    Args:
        content: The file content as a string

    Returns:
        The content with double empty lines fixed
    """
    lines = content.split("\n")
    result = []
    in_code_block = False
    i = 0

    while i < len(lines):
        line = lines[i]

        # Check if we're entering a Python code block
        if line.strip().endswith("```python"):
            in_code_block = True
            result.append(line)
            i += 1
            continue

        # Check if we're exiting a code block
        if in_code_block and line.strip().startswith("```"):
            in_code_block = False
            result.append(line)
            i += 1
            continue

        # If we're in a code block, check for consecutive empty lines
        if in_code_block:
            # Add the current line
            result.append(line)

            # If this line is empty, skip any additional consecutive empty lines
            if line.strip() == "":
                # Look ahead and skip consecutive empty lines
                j = i + 1
                while j < len(lines) and lines[j].strip() == "" and not lines[j].strip().startswith("```"):
                    j += 1
                # Move i to just before the next non-empty line (or end of consecutive empties)
                i = j - 1
        else:
            result.append(line)

        i += 1

    return "\n".join(result)


def convert_file(file_path):
    """Convert doctest examples in a file to markdown format."""
    with open(file_path) as f:
        content = f.read()

    lines = content.split("\n")
    result_lines = []
    i = 0

    while i < len(lines):
        line = lines[i]

        # Check if this is an Examples: line
        match = re.match(r"^(\s+)Examples:\s*$", line)

        if match:
            indent = match.group(1)
            example_indent = indent + "    "  # Examples content is indented 4 more spaces
            result_lines.append(line)
            i += 1

            # Process lines in this Examples section
            while i < len(lines):
                current = lines[i]

                # Stop if we hit a new section or end of docstring
                if current.strip() in ['"""', "'''"]:
                    break
                if re.match(r"^\s+(Tip|Note|Warning|Args|Returns|Raises|See Also|Attributes):", current):
                    break
                if current and not current.startswith(example_indent) and current.strip():
                    # Less indentation = end of Examples section
                    break

                # Check if this line starts a doctest block
                if current.startswith(example_indent + ">>>"):
                    # Collect the entire doctest block
                    doctest_block = []

                    while i < len(lines):
                        curr_line = lines[i]

                        # Check for end conditions
                        if curr_line.strip() in ['"""', "'''"]:
                            break
                        if re.match(r"^\s+(Tip|Note|Warning|Args|Returns|Raises|See Also|Attributes):", curr_line):
                            break
                        if curr_line and not curr_line.startswith(example_indent) and curr_line.strip():
                            break

                        # Add line to doctest block
                        if curr_line.startswith(example_indent):
                            content_line = curr_line[len(example_indent) :]
                            if (
                                content_line.startswith(">>> ")
                                or content_line.startswith(">>>")
                                or content_line.startswith("... ")
                                or content_line.startswith("...")
                                or (doctest_block and content_line.strip())
                            ):  # Output or continuation
                                doctest_block.append(content_line)
                                i += 1
                            elif not content_line.strip():
                                # Blank line - might be end of block
                                i += 1
                                break
                            else:
                                # Non-doctest content
                                break
                        elif curr_line == indent or not curr_line.strip():
                            # Blank line
                            i += 1
                            break
                        else:
                            break

                    # Convert the doctest block
                    if doctest_block:
                        code_lines = []
                        output_lines = []

                        for line_content in doctest_block:
                            if line_content.startswith(">>> "):
                                code_lines.append(line_content[4:])
                            elif line_content.startswith(">>>"):
                                code_lines.append(line_content[3:])
                            elif line_content.startswith("... "):
                                code_lines.append(line_content[4:])
                            elif line_content.startswith("..."):
                                code_lines.append(line_content[3:])
                            else:
                                # This is output
                                output_lines.append(line_content)

                        # Generate markdown blocks
                        result_lines.append(f"{example_indent}```python")
                        for code_line in code_lines:
                            result_lines.append(f"{example_indent}{code_line}")
                        result_lines.append(f"{example_indent}```")

                        if output_lines:
                            result_lines.append(f"{example_indent}")
                            result_lines.append(f'{example_indent}``` {{title="Output"}}')
                            for output_line in output_lines:
                                result_lines.append(f"{example_indent}{output_line}")
                            result_lines.append(f"{example_indent}```")

                        continue

                # Not a doctest line, just add it
                result_lines.append(current)
                i += 1
        else:
            result_lines.append(line)
            i += 1

    return "\n".join(result_lines)


def main():
    if len(sys.argv) != 2:
        print("Usage: python tools/convert_doctest_to_markdown.py <file_path>")
        print("Example: python tools/convert_doctest_to_markdown.py daft/dataframe/dataframe.py")
        sys.exit(1)

    file_path = Path(sys.argv[1])

    if not file_path.exists():
        print(f"Error: File {file_path} does not exist")
        sys.exit(1)

    print(f"Converting doctest examples in {file_path}...")

    # Convert the file
    converted = convert_file(file_path)

    # Fix double empty lines in code blocks
    converted = fix_double_empty_lines_in_code_blocks(converted)

    # Write back
    with open(file_path, "w") as f:
        f.write(converted)

    print("✓ Conversion complete!")
    print(f"  File: {file_path}")
    print("\nNext steps:")
    print(f"  1. Review the changes: git diff {file_path}")
    print("  2. Build docs to verify: JUPYTER_PLATFORM_DIRS=1 uv run mkdocs serve")
    print("  3. Note: This will break 'make doctests' - you may need to update testing strategy")


if __name__ == "__main__":
    main()
