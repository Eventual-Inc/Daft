import os


def on_config(config):
    """Set Read the Docs version information in config."""
    config.extra["rtd_version"] = os.environ.get("READTHEDOCS_VERSION", "unknown")
    return config
