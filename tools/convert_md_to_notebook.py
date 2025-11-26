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
    # Remove HTML comments first
    content = re.sub(r"<!--[\s\S]*?-->", "", content)

    # Remove MkDocs grid cards (these don't render well in notebooks)
    # Match the entire grid card block including its closing </div>
    content = re.sub(r'<div class="grid cards" markdown>\s*([\s\S]*?)\n</div>\s*', r"\1\n", content)

    # Remove MkDocs icon syntax (:material-*:, :octicons-*:, :fontawesome-*:, etc.)
    # These don't render in notebooks. Also remove trailing space after the icon.
    content = re.sub(r":(?:material|octicons|fontawesome|simple)-[a-z0-9-]+:\s*", "", content)

    # Convert relative links to absolute docs links BEFORE converting admonitions
    # This ensures links inside admonitions get converted properly
    # [text](connectors/iceberg.md) -> [text](https://docs.daft.ai/en/stable/connectors/iceberg/)
    def convert_link(match: re.Match) -> str:
        text = match.group(1)
        path = match.group(2)
        # Skip external links and anchors
        if path.startswith(("http://", "https://", "#")):
            return match.group(0)
        # Convert .md to docs URL
        # index.md -> just the directory (e.g., examples/index.md -> examples/)
        path = re.sub(r"index\.md$", "", path)
        path = path.replace(".md", "/")
        # Ensure path ends with / for directory URLs
        if path and not path.endswith("/"):
            path += "/"
        return f"[{text}](https://docs.daft.ai/en/stable/{path})"

    content = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", convert_link, content)

    # Convert !!! admonitions to HTML (AFTER link conversion so links are absolute)
    # Pattern: !!! type "optional title"\n    content (indented)
    admonition_pattern = r'!!!\s+(\w+)(?:\s+"([^"]*)")?\s*\n((?:[ ]{4}[^\n]*\n?)+)'

    def replace_admonition(match: re.Match) -> str:
        admonition_type = match.group(1)
        title = match.group(2)
        # Remove the 4-space indentation from content
        content_lines = match.group(3).split("\n")
        admon_content = "\n".join(line[4:] if line.startswith("    ") else line for line in content_lines)
        return convert_admonition_to_html_direct(admonition_type, title, admon_content.strip())

    content = re.sub(admonition_pattern, replace_admonition, content)

    return content


def escape_angle_brackets_in_backticks(content: str) -> str:
    """Escape angle brackets inside backticks so they render as text, not HTML.

    For example: `<Image>` -> `&lt;Image&gt;`
    This prevents inline code like `<Image>` from being treated as HTML tags.
    """

    def escape_brackets(match: re.Match) -> str:
        code = match.group(1)
        # Escape < and > inside the backticks
        escaped = code.replace("<", "&lt;").replace(">", "&gt;")
        return f"`{escaped}`"

    return re.sub(r"`([^`]+)`", escape_brackets, content)


def convert_markdown_links_to_html(content: str) -> str:
    """Convert markdown links [text](url) to HTML <a> tags.

    This is needed because markdown inside HTML tags doesn't get rendered.
    """

    def replace_link(match: re.Match) -> str:
        text = match.group(1)
        url = match.group(2)
        return f'<a href="{url}">{text}</a>'

    return re.sub(r"\[([^\]]+)\]\(([^)]+)\)", replace_link, content)


def convert_code_blocks_to_html(content: str) -> str:
    """Convert markdown code blocks to HTML <pre><code> tags.

    This is needed for code blocks inside admonitions where markdown doesn't render.
    """

    def replace_code_block(match: re.Match) -> str:
        code = match.group(2)
        # Escape HTML entities in the code
        code = code.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        return f'<pre style="background-color: #f5f5f5; padding: 8px; border-radius: 4px; overflow-x: auto;"><code>{code}</code></pre>'

    # Match ```lang\ncode\n``` or ```\ncode\n```
    return re.sub(r"```(\w*)\n(.*?)```", replace_code_block, content, flags=re.DOTALL)


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

    # Convert code blocks to HTML first (before escaping backticks)
    content_html = convert_code_blocks_to_html(content)
    # Escape angle brackets in inline code (e.g., `<Image>` -> `&lt;Image&gt;`)
    # This must be done before converting markdown links to HTML
    content_html = escape_angle_brackets_in_backticks(content_html)
    # Convert markdown links to HTML since markdown doesn't render inside HTML tags
    content_html = convert_markdown_links_to_html(content_html)

    return f"""
<div style="background-color: {color}22; border-left: 4px solid {color}; padding: 12px; margin: 16px 0;">
<strong style="color: {color};">{display_title}</strong><br/>
{content_html}
</div>

"""


def parse_markdown_to_cells(content: str) -> list[dict]:
    """Parse markdown content into notebook cells."""
    cells = []

    # Pattern matches ```python or ```{python} code blocks, optionally followed by output blocks
    # Output blocks are ``` {title="Output"}\n...\n```
    # Use ^ to only match code blocks at the start of a line (not indented, e.g., inside admonitions)
    code_block_pattern = r"^```(?:\{?)python\}?\n(.*?)^```"
    output_block_pattern = r'```\s*\{title="Output"\}\n(.*?)```'

    parts = []
    last_end = 0

    # Find all code blocks (MULTILINE for ^ to match line starts, DOTALL for . to match newlines)
    for match in re.finditer(code_block_pattern, content, re.DOTALL | re.MULTILINE):
        # Add markdown before this code block
        if match.start() > last_end:
            md_content = content[last_end : match.start()].strip()
            if md_content:
                parts.append(("markdown", md_content, None))

        code_content = match.group(1).strip()

        # Check if there's an output block immediately after this code block
        output_content = None
        after_code = content[match.end() :].lstrip()
        output_match = re.match(output_block_pattern, after_code, re.DOTALL)
        if output_match:
            output_content = output_match.group(1).rstrip()
            # Update last_end to skip the output block too
            # Calculate actual position in original content
            whitespace_len = len(content[match.end() :]) - len(after_code)
            last_end = match.end() + whitespace_len + output_match.end()
        else:
            last_end = match.end()

        # Handle pip install commands
        if code_content.startswith("pip install"):
            parts.append(("code", "!" + code_content, output_content))
        else:
            parts.append(("code", code_content, output_content))

    # Add remaining markdown
    if last_end < len(content):
        md_content = content[last_end:].strip()
        if md_content:
            parts.append(("markdown", md_content, None))

    # Process parts into cells
    for part_type, part_content, output_content in parts:
        if part_type == "markdown":
            # Process markdown content
            processed = process_markdown_content(part_content)

            # Remove any remaining output blocks from markdown
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
            cell = create_notebook_cell("code", source)

            # Add output if present
            if output_content:
                cell["outputs"] = [
                    {
                        "output_type": "stream",
                        "name": "stdout",
                        "text": [line + "\n" for line in output_content.split("\n")],
                    }
                ]
                cell["execution_count"] = 1

            cells.append(cell)

    return cells


def create_colab_badge_markdown(notebook_path: str, branch: str = "main", for_mkdocs: bool = False) -> str:
    """Create markdown for Colab badge to be inserted after the title.

    Args:
        notebook_path: Path to the notebook file relative to repo root
        branch: Git branch name for the Colab badge link
        for_mkdocs: If True, use MkDocs-compatible markdown syntax instead of HTML
    """
    from urllib.parse import quote

    # Assume notebooks will be in the repo under docs/notebooks/
    # URL-encode the branch name to handle slashes (e.g., "tools/converter" -> "tools%2Fconverter")
    encoded_branch = quote(branch, safe="")
    github_path = f"Eventual-Inc/Daft/blob/{encoded_branch}/{notebook_path}"
    colab_url = f"https://colab.research.google.com/github/{github_path}"

    if for_mkdocs:
        # Use markdown image link syntax for MkDocs compatibility
        return f"[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)]({colab_url})\n"

    return f"""
<a target="_blank" href="{colab_url}">
  <img src="https://colab.research.google.com/assets/colab-badge.svg" alt="Open In Colab"/>
</a>
"""


def has_colab_badge(content: str) -> bool:
    """Check if the content already has a Colab badge."""
    return "colab.research.google.com" in content


# Regex pattern to match Colab badge in markdown (both HTML and markdown syntax)
COLAB_BADGE_PATTERN = r'(\[!\[Open In Colab\].*?\]\(https://colab\.research\.google\.com/github/[^\)]+\)\n*|<a[^>]*href="https://colab\.research\.google\.com/github/[^"]*"[^>]*>[\s\S]*?</a>\n*)'


def update_colab_badge_in_markdown(
    input_path: Path,
    notebook_path: Path,
    branch: str = "main",
) -> str:
    """Add or update a Colab badge in a markdown file.

    Args:
        input_path: Path to the markdown file to update
        notebook_path: Path to the notebook file (for generating the Colab link)
        branch: Git branch name for the Colab badge link

    Returns:
        "added" if badge was added, "updated" if existing badge was updated, "unchanged" if no change needed
    """
    content = input_path.read_text()

    # Calculate relative path from repo root for the notebook
    try:
        repo_root = Path(__file__).parent.parent
        rel_notebook_path = notebook_path.relative_to(repo_root)
    except ValueError:
        rel_notebook_path = notebook_path

    badge_md = create_colab_badge_markdown(str(rel_notebook_path), branch=branch, for_mkdocs=True)

    # Check if badge already exists
    if has_colab_badge(content):
        # Replace existing badge with new one
        new_content = re.sub(COLAB_BADGE_PATTERN, badge_md + "\n", content, count=1)
        if new_content != content:
            input_path.write_text(new_content)
            return "updated"
        return "unchanged"

    # Insert badge after the first # heading line
    new_content = re.sub(r"(^# .+\n)", r"\1\n" + badge_md + "\n", content, count=1, flags=re.MULTILINE)

    if new_content != content:
        input_path.write_text(new_content)
        return "added"

    return "unchanged"


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

    # Add Colab badge after the title (# heading) if not already present
    if add_colab_badge and not has_colab_badge(content):
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
    parser.add_argument(
        "--update-source",
        action="store_true",
        help="Also add Colab badge to the source markdown file (skips if already present)",
    )

    args = parser.parse_args()

    output_path = convert_md_to_notebook(
        args.input,
        args.output,
        add_colab_badge=not args.no_colab_badge,
        branch=args.branch,
    )

    print(f"Created notebook: {output_path}")

    # Optionally update the source markdown file
    if args.update_source and not args.no_colab_badge:
        result = update_colab_badge_in_markdown(args.input, output_path, branch=args.branch)
        if result == "added":
            print(f"Added Colab badge to: {args.input}")
        elif result == "updated":
            print(f"Updated Colab badge in: {args.input}")
        else:
            print(f"Colab badge unchanged in: {args.input}")


if __name__ == "__main__":
    main()
