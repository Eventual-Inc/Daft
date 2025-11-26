#!/usr/bin/env python3
"""Convert Markdown documentation files to Jupyter Notebooks.

This script converts Markdown files (like quickstart.md) into Jupyter notebooks
that can be run in Google Colab or locally.

Usage:
    python tools/convert_md_to_notebook.py docs/quickstart.md -o docs/notebooks/quickstart.ipynb
    python tools/convert_md_to_notebook.py docs/quickstart.md  # outputs to same dir with .ipynb extension
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path


def create_notebook_cell(cell_type: str, source: list[str], execution_count: int | None = None) -> dict:
    """Create a notebook cell."""
    cell = {
        "cell_type": cell_type,
        "metadata": {},
        "source": source,
    }
    if cell_type == "code":
        cell["execution_count"] = execution_count
        cell["outputs"] = []
    return cell


def convert_admonition_to_html(match: re.Match) -> str:
    """Convert MkDocs admonition syntax to HTML for notebook display."""
    admonition_type = match.group(1).lower()
    title = match.group(2) if match.group(2) else admonition_type.title()
    content = match.group(3).strip()

    # Map admonition types to colors/styles
    type_styles = {
        "note": ("#448aff", "Note"),
        "tip": ("#00c853", "Tip"),
        "warning": ("#ff9100", "Warning"),
        "danger": ("#ff1744", "Danger"),
        "info": ("#00b0ff", "Info"),
    }

    color, default_title = type_styles.get(admonition_type, ("#448aff", "Note"))
    if not title or title == '""':
        title = default_title

    return f"""<div style="background-color: {color}22; border-left: 4px solid {color}; padding: 12px; margin: 16px 0;">
<strong style="color: {color};">{title}</strong><br/>
{content}
</div>"""


def process_markdown_content(content: str) -> str:
    """Process markdown content to convert MkDocs-specific syntax."""
    # Convert !!! admonitions to HTML
    # Pattern: !!! type "optional title"\n    content (indented)
    admonition_pattern = r'!!!\s+(\w+)(?:\s+"([^"]*)")?\s*\n((?:[ ]{4}[^\n]*\n?)+)'

    def replace_admonition(match: re.Match) -> str:
        admonition_type = match.group(1)
        title = match.group(2)
        # Remove the 4-space indentation from content
        content_lines = match.group(3).split("\n")
        content = "\n".join(line[4:] if line.startswith("    ") else line for line in content_lines)
        # Create a fake match object for the converter
        return convert_admonition_to_html_direct(admonition_type, title, content.strip())

    content = re.sub(admonition_pattern, replace_admonition, content)

    # Remove HTML comments
    content = re.sub(r"<!--[\s\S]*?-->", "", content)

    # Remove MkDocs grid cards (these don't render well in notebooks)
    # Match the entire grid card block including its closing </div>
    content = re.sub(r'<div class="grid cards" markdown>\s*([\s\S]*?)\n</div>\s*', r"\1\n", content)

    # Convert relative links to absolute docs links
    # [text](connectors/iceberg.md) -> [text](https://docs.daft.ai/en/stable/connectors/iceberg/)
    def convert_link(match: re.Match) -> str:
        text = match.group(1)
        path = match.group(2)
        # Skip external links and anchors
        if path.startswith(("http://", "https://", "#")):
            return match.group(0)
        # Convert .md to docs URL
        path = path.replace(".md", "/")
        return f"[{text}](https://docs.daft.ai/en/stable/{path})"

    content = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", convert_link, content)

    return content


def convert_admonition_to_html_direct(admonition_type: str, title: str | None, content: str) -> str:
    """Convert admonition directly to HTML."""
    type_styles = {
        "note": ("#448aff", "Note"),
        "tip": ("#00c853", "Tip"),
        "warning": ("#ff9100", "Warning"),
        "danger": ("#ff1744", "Danger"),
        "info": ("#00b0ff", "Info"),
    }

    color, default_title = type_styles.get(admonition_type.lower(), ("#448aff", "Note"))
    display_title = title if title else default_title

    return f"""
<div style="background-color: {color}22; border-left: 4px solid {color}; padding: 12px; margin: 16px 0;">
<strong style="color: {color};">{display_title}</strong><br/>
{content}
</div>

"""


def parse_markdown_to_cells(content: str) -> list[dict]:
    """Parse markdown content into notebook cells."""
    cells = []

    # Split content into sections based on code blocks
    # Pattern matches ```python or ```{python} code blocks
    code_block_pattern = r"```(?:\{?)python\}?\n(.*?)```"

    parts = []
    last_end = 0

    # Find all code blocks and output blocks
    for match in re.finditer(code_block_pattern, content, re.DOTALL):
        # Add markdown before this code block
        if match.start() > last_end:
            md_content = content[last_end : match.start()].strip()
            if md_content:
                parts.append(("markdown", md_content))

        code_content = match.group(1).strip()
        # Skip pip install commands that are meant as shell commands
        if code_content.startswith("pip install"):
            # Convert to notebook !pip install
            parts.append(("code", "!" + code_content))
        else:
            parts.append(("code", code_content))

        last_end = match.end()

    # Add remaining markdown
    if last_end < len(content):
        md_content = content[last_end:].strip()
        if md_content:
            parts.append(("markdown", md_content))

    # Process parts into cells
    for part_type, part_content in parts:
        if part_type == "markdown":
            # Process markdown content
            processed = process_markdown_content(part_content)

            # Remove output blocks from markdown (they were shown after code)
            processed = re.sub(r'```\s*\{title="Output"\}\n.*?```', "", processed, flags=re.DOTALL)

            # Clean up extra whitespace
            processed = re.sub(r"\n{3,}", "\n\n", processed).strip()

            if processed:
                # Split source into lines for notebook format
                source_lines = processed.split("\n")
                source = [line + "\n" for line in source_lines[:-1]] + [source_lines[-1]]
                cells.append(create_notebook_cell("markdown", source))

        elif part_type == "code":
            source_lines = part_content.split("\n")
            source = [line + "\n" for line in source_lines[:-1]] + [source_lines[-1]]
            cells.append(create_notebook_cell("code", source))

    return cells


def create_colab_badge_markdown(notebook_path: str, branch: str = "main") -> str:
    """Create markdown for Colab badge to be inserted after the title."""
    # Assume notebooks will be in the repo under docs/notebooks/
    github_path = f"Eventual-Inc/Daft/blob/{branch}/{notebook_path}"
    colab_url = f"https://colab.research.google.com/github/{github_path}"

    return f"""
<a target="_blank" href="{colab_url}">
  <img src="https://colab.research.google.com/assets/colab-badge.svg" alt="Open In Colab"/>
</a>
"""


def convert_md_to_notebook(
    input_path: Path,
    output_path: Path | None = None,
    add_colab_badge: bool = True,
    branch: str = "main",
) -> Path:
    """Convert a markdown file to a Jupyter notebook.

    Args:
        input_path: Path to the input markdown file
        output_path: Path for output notebook (default: same name with .ipynb)
        add_colab_badge: Whether to add a Colab badge at the top
        branch: Git branch name for the Colab badge link (default: main)

    Returns:
        Path to the created notebook
    """
    if output_path is None:
        output_path = input_path.with_suffix(".ipynb")

    # Read markdown content
    content = input_path.read_text()

    # Add Colab badge after the title (# heading)
    if add_colab_badge:
        # Calculate relative path from repo root
        try:
            repo_root = Path(__file__).parent.parent
            rel_path = output_path.relative_to(repo_root)
        except ValueError:
            rel_path = output_path

        badge_md = create_colab_badge_markdown(str(rel_path), branch=branch)
        # Insert badge after the first # heading line
        content = re.sub(r"(^# .+\n)", r"\1" + badge_md, content, count=1, flags=re.MULTILINE)

    # Parse into cells
    cells = parse_markdown_to_cells(content)

    # Create notebook structure
    notebook = {
        "cells": cells,
        "metadata": {
            "kernelspec": {
                "display_name": "Python 3",
                "language": "python",
                "name": "python3",
            },
            "language_info": {
                "name": "python",
                "version": "3.10.0",
            },
        },
        "nbformat": 4,
        "nbformat_minor": 5,
    }

    # Write notebook
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(notebook, indent=1))

    return output_path


def main():
    parser = argparse.ArgumentParser(
        description="Convert Markdown documentation to Jupyter Notebook",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python tools/convert_md_to_notebook.py docs/quickstart.md
    python tools/convert_md_to_notebook.py docs/quickstart.md -o docs/notebooks/quickstart.ipynb
    python tools/convert_md_to_notebook.py docs/quickstart.md --no-colab-badge
        """,
    )
    parser.add_argument("input", type=Path, help="Input markdown file")
    parser.add_argument("-o", "--output", type=Path, help="Output notebook path")
    parser.add_argument(
        "--no-colab-badge",
        action="store_true",
        help="Don't add Colab badge to the notebook",
    )
    parser.add_argument(
        "--branch",
        default="main",
        help="Git branch for Colab badge link (default: main)",
    )

    args = parser.parse_args()

    output_path = convert_md_to_notebook(
        args.input,
        args.output,
        add_colab_badge=not args.no_colab_badge,
        branch=args.branch,
    )

    print(f"Created notebook: {output_path}")


if __name__ == "__main__":
    main()
