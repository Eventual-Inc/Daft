from pathlib import Path

import mkdocs_gen_files


def main():
    """Generate .txt files for all markdown files in the docs directory."""
    docs_dir = Path("docs")

    for md_file in docs_dir.rglob("*.md"):
        relative_path = md_file.relative_to(docs_dir)

        txt_path = str(relative_path).replace(".md", ".txt")

        with open(md_file, encoding="utf-8") as f:
            content = f.read()

        with mkdocs_gen_files.open(txt_path, "w") as f:
            f.write(content)


main()
