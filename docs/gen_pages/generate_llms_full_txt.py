import re
from pathlib import Path

import mkdocs_gen_files

BASE_URL = "https://docs.daft.ai/en/stable/"


def parse_summary():
    """Parse SUMMARY.md and return an ordered list of (section, name, path) tuples."""
    summary_path = Path("docs/SUMMARY.md")
    with open(summary_path, encoding="utf-8") as f:
        lines = f.readlines()

    entries = []
    current_section = None

    for line in lines:
        line = line.rstrip()
        if not line.strip() or line.strip().startswith("<!--"):
            continue

        if line.startswith("* "):
            match = re.search(r"\[([^\]]+)\]\(([^)]+)\)", line)
            if match and not match.group(2).startswith("http"):
                name = match.group(1).replace("<sup>↗</sup>", "").strip()
                entries.append((current_section or "Other", name, match.group(2)))
            else:
                current_section = line[2:].strip()
            continue

        if line.startswith("    ") and current_section:
            match = re.search(r"\[([^\]]+)\]\(([^)]+)\)", line)
            if match and not match.group(2).startswith("http"):
                name = match.group(1).replace("<sup>↗</sup>", "").strip()
                entries.append((current_section, name, match.group(2)))

    return entries


def read_doc(file_path: str) -> str | None:
    """Read a markdown doc file, returning its content or None."""
    path = Path("docs") / file_path
    if path.is_dir():
        path = path / "index.md"
    if not path.exists() and not path.suffix:
        path = path.with_suffix(".md")
    if not path.exists():
        return None
    with open(path, encoding="utf-8") as f:
        return f.read()


def main():
    entries = parse_summary()

    parts = [
        "# Daft\n",
        "> Daft is a high-performance data engine providing simple and reliable data processing "
        "for any modality and scale, from local to petabyte-scale distributed workloads. "
        "The core engine is written in Rust and exposes both SQL and Python DataFrame interfaces "
        "as first-class citizens.\n",
    ]

    current_section = None
    for section, name, file_path in entries:
        if section != current_section:
            current_section = section
            parts.append(f"\n---\n\n## {section}\n")

        url = BASE_URL + file_path.replace(".md", "/")
        content = read_doc(file_path)
        if content is None:
            continue

        parts.append(f"\n---\n\n### {name}\n")
        parts.append(f"Source: {url}\n")
        parts.append(content)

    with mkdocs_gen_files.open("llms-full.txt", "w") as f:
        f.write("\n".join(parts))


main()
