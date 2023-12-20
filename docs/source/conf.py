# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

from __future__ import annotations

import importlib
import inspect
import subprocess

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information
project = "Daft"
copyright = "2023, Eventual"
author = "Eventual"
# html_logo = "_static/daft-logo.png"
html_favicon = "_static/daft-favicon.png"

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

# The name of a reST role (builtin or Sphinx extension) to use as the default role, that
# is, for text marked up `like this`. This can be set to 'py:obj' to make `filter` a
# cross-reference to the Python function “filter”. The default is None, which doesn’t
# reassign the default role.

default_role = "py:obj"

extensions = [
    "sphinx_reredirects",
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx.ext.autosummary",
    "sphinx.ext.linkcode",
    "IPython.sphinxext.ipython_console_highlighting",
    "myst_nb",
    "sphinx_copybutton",
]

templates_path = ["_templates"]


# -- Options for Notebook rendering
# https://myst-nb.readthedocs.io/en/latest/configuration.html?highlight=nb_execution_mode#execution

nb_execution_mode = "off"


# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "sphinx_book_theme"
html_static_path = ["_static"]
html_css_files = ["header.css", "custom-function-signatures.css"]
html_theme_options = {
    # This is the footer of the primary sidebar as HTML
    "extra_navbar": "",
    # This is how many levels are shown on the secondary sidebar
    "show_toc_level": 2,
    # Remove title under the logo on the left sidebar
    "logo_only": True,
}

# -- Copy button configuration

# This pattern matches:
# - Python Repl prompts (">>> ") and it's continuation ("... ")
# - Bash prompts ("$ ")
# - IPython prompts ("In []: ", "In [999]: ") and it's continuations
#   ("  ...: ", "     : ")
copybutton_prompt_text = r">>> |\.\.\. |\$ |In \[\d*\]: | {2,5}\.\.\.: | {5,8}: "
copybutton_prompt_is_regexp = True

# -- Options for redirecting URLs
redirects = {
    "learn/install": "../install.html",
    "learn/user_guides/dataframes": "intro-dataframes.html",
    "learn/user_guides/types_and_ops": "intro-dataframes.html",
    "learn/user_guides/remote_cluster_execution": "scaling-up.html",
    "learn/quickstart": "learn/10-min.html",
    "learn/10-min": "../10-min.html",
}

# Resolving code links to github
# Adapted from: https://github.com/aaugustin/websockets/blob/778a1ca6936ac67e7a3fe1bbe585db2eafeaa515/docs/conf.py#L100-L134


commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd="../..").strip().decode("utf-8")
code_url = f"https://github.com/Eventual-Inc/Daft/blob/{commit}"


def linkcode_resolve(domain, info):
    assert domain == "py", "expected only Python objects"
    mod = importlib.import_module(info["module"])
    if "." in info["fullname"]:
        objname, attrname = info["fullname"].split(".")
        obj = getattr(mod, objname)
        try:
            # object is a method of a class
            obj = getattr(obj, attrname)
        except AttributeError:
            # object is an attribute of a class
            return None
    else:
        obj = getattr(mod, info["fullname"])

    # Handle case where object is a decorated function
    while hasattr(obj, "__wrapped__"):
        obj = obj.__wrapped__

    try:
        file = inspect.getsourcefile(obj)
        lines = inspect.getsourcelines(obj)
    except (TypeError, OSError):
        # e.g. object is a typing.Union
        return None
    path_start = file.find("daft/")
    if path_start == -1:
        return None
    file = file[path_start:]
    start, end = lines[1], lines[1] + len(lines[0]) - 1

    return f"{code_url}/{file}#L{start}-L{end}"
