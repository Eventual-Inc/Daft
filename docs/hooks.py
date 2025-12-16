import base64
import os
import re

# Store markdown source per page during build
_page_markdown_cache = {}

# Button HTML template - markdown is stored in data-md attribute
_COPY_BUTTON_TEMPLATE = """<button class="copy-page-btn" title="Copy page as Markdown" onclick="copyPageMarkdown(this)" data-md="{encoded_md}">
  <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" width="16" height="16">
    <path d="M16 1H4c-1.1 0-2 .9-2 2v14h2V3h12V1zm3 4H8c-1.1 0-2 .9-2 2v14c0 1.1.9 2 2 2h11c1.1 0 2-.9 2-2V7c0-1.1-.9-2-2-2zm0 16H8V7h11v14z"/>
  </svg>
  <span class="copy-page-btn-text">Copy page</span>
</button>"""


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

    # AI function pages (not overview)
    if src_path.startswith("ai-functions/") and src_path != "ai-functions/overview.md":
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
    """Inject copy button and markdown source into the page."""
    src_path = page.file.src_path
    if src_path in _page_markdown_cache:
        markdown_source = _page_markdown_cache[src_path]

        # Base64 encode the markdown for safe embedding in HTML attribute
        encoded_md = base64.b64encode(markdown_source.encode("utf-8")).decode("ascii")
        button_html = _COPY_BUTTON_TEMPLATE.format(encoded_md=encoded_md)

        # Wrap the first h1 with our header div and add the button
        def replace_h1(match):
            h1_tag = match.group(0)
            return f'<div class="copy-page-header">{h1_tag}{button_html}</div>'

        html = re.sub(r"<h1[^>]*>.*?</h1>", replace_h1, html, count=1, flags=re.DOTALL)

        # Clean up cache to save memory
        del _page_markdown_cache[src_path]
        return html
    return html
