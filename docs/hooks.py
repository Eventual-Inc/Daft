import os


def on_config(config):
    """Set Read the Docs version information in config."""
    config.extra["rtd_version"] = os.environ.get("READTHEDOCS_VERSION", "unknown")
    return config


def on_env(env, config, files):
    """Register custom Jinja2 filters for the documentation."""
    print("=" * 50)
    print("ON_ENV HOOK CALLED!")
    print(f"Environment type: {type(env)}")
    print(f"Available filters before: {list(env.filters.keys())[:5]}...")  # Show first 5 filters
    print("=" * 50)

    def test_examples_filter(text):
        """Simple test filter to verify we can process examples."""
        # Just wrap the text to show the filter is working
        return f"<!-- FILTER WORKING -->\n<!-- Input length: {len(text)} chars -->\n{text}\n<!-- END FILTER -->"

    # Register the test filter
    env.filters['test_examples'] = test_examples_filter

    print(f"Added 'test_examples' filter")
    print(f"'test_examples' in filters: {'test_examples' in env.filters}")

    return env
