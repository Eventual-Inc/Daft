import json
import os

# Store markdown source per page during build
_page_markdown_cache = {}


def _should_inject_markdown(src_path):
    """Check if this page should have markdown source injected."""
    # Quick start page
    if src_path == "quickstart.md":
        return True

    # Example pages (not the index)
    if src_path.startswith("examples/") and src_path != "examples/index.md":
        return True

    # Python API pages (not index pages)
    if src_path.startswith("api/") and not src_path.endswith("index.md"):
        return True

    return False


def on_config(config):
    """Set Read the Docs version information in config."""
    config.extra["rtd_version"] = os.environ.get("READTHEDOCS_VERSION", "unknown")
    return config


def on_page_markdown(markdown, page, config, files):
    """Capture raw markdown source before conversion to HTML."""
    if _should_inject_markdown(page.file.src_path):
        _page_markdown_cache[page.file.src_path] = markdown
    return markdown


def on_page_content(html, page, config, files):
    """Inject raw markdown source into the page for copy-as-markdown feature."""
    src_path = page.file.src_path
    if src_path in _page_markdown_cache:
        markdown_source = _page_markdown_cache[src_path]
        # Embed as JSON in a script tag (safe escaping)
        markdown_json = json.dumps(markdown_source)
        injection = f'<script id="page-markdown-source" type="application/json">{markdown_json}</script>'
        # Clean up cache to save memory
        del _page_markdown_cache[src_path]
        return html + injection
    return html
