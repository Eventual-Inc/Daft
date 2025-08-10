import re
from pathlib import Path
from urllib.parse import quote

import mkdocs_gen_files


def get_doc_structure():
    """Parse SUMMARY.md to extract the documentation structure."""
    summary_path = Path("docs/SUMMARY.md")

    if not summary_path.exists():
        raise FileNotFoundError("docs/SUMMARY.md not found")

    with open(summary_path, encoding="utf-8") as f:
        content = f.read()

    sections = {
        "Guide": {"files": []},
        "Examples": {"files": []},
        "Python API": {"files": []},
        "SQL Reference": {"files": []},
    }

    # Parse the markdown structure
    lines = content.split("\n")
    current_section = None

    for line in lines:
        line = line.rstrip()  # Keep leading spaces for indentation

        # Skip empty lines and comments
        if not line.strip() or line.strip().startswith("<!--"):
            continue

        # Check for main sections (lines starting with *)
        if line.startswith("* "):
            section_name = line[2:].strip()
            if section_name == "Guide":
                current_section = "Guide"
            elif section_name == "Examples":
                current_section = "Examples"
            elif section_name == "Python API":
                current_section = "Python API"
            elif section_name == "SQL Reference":
                current_section = "SQL Reference"
            else:
                current_section = None
            continue

        # Check for any indented lines (subsections and nested items)
        if line.startswith("    ") and current_section:
            match = re.search(r"\[([^\]]+)\]\(([^)]+)\)", line)
            if match:
                name = match.group(1)
                file_path = match.group(2)

                # Skip external links
                if file_path.startswith("http"):
                    continue

                # Create a readable name
                readable_name = name.replace("<sup>↗</sup>", "").strip()

                # Add to the appropriate section
                sections[current_section]["files"].append(
                    {
                        "name": readable_name,
                        "path": file_path,
                        "url": f"https://docs.daft.ai/en/stable/{file_path.replace('.md', '.txt')}",
                        "github_url": f"https://raw.githubusercontent.com/Eventual-Inc/Daft/main/docs/{quote(file_path)}",
                    }
                )

    return sections


def generate_llms_txt():
    """Generate the llms.txt file content."""
    sections = get_doc_structure()

    # Start with the header
    content = """# Daft

> Daft is a high-performance data engine providing simple and reliable data processing for any modality and scale, from local to petabyte-scale distributed workloads. The core engine is written in Rust and exposes both SQL and Python DataFrame interfaces as first-class citizens.

"""

    # Add each section
    for section_name, section_data in sections.items():
        if section_data["files"]:
            content += f"## {section_name}\n\n"

            # Sort files by path for consistent ordering
            sorted_files = sorted(section_data["files"], key=lambda x: x["path"])

            for file_info in sorted_files:
                content += f"- [{file_info['name']}]({file_info['url']})\n"

            content += "\n"

    return content


def main():
    """Generate and write the llms.txt file."""
    content = generate_llms_txt()

    # Write to llms.txt in the build directory using mkdocs_gen_files
    with mkdocs_gen_files.open("llms.txt", "w") as f:
        f.write(content)


main()
