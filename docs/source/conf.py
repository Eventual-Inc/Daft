# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

from __future__ import annotations

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information
project = "Daft"
copyright = "2022, Eventual"
author = "Eventual"
# html_logo = "_static/daft-logo.png"
html_favicon = "_static/daft-favicon.png"

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx.ext.autosummary",
    "sphinx.ext.linkcode",
    "myst_nb",
]

templates_path = ["_templates"]
exclude_patterns = ["docs/release_notes/_template.rst"]


# -- Options for Notebook rendering
# https://myst-nb.readthedocs.io/en/latest/configuration.html?highlight=nb_execution_mode#execution

nb_execution_mode = "off"


# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "sphinx_book_theme"
html_static_path = ["_static"]
html_css_files = ["header.css", "landing-page.css"]
html_theme_options = {
    # This is the footer of the primary sidebar as HTML
    "extra_navbar": "",
    # This is how many levels are shown on the secondary sidebar
    "show_toc_level": 2,
    # Remove title under the logo on the left sidebar
    "logo_only": True,
}

# Resolving code links to github
# Adapted from: https://github.com/aaugustin/websockets/blob/778a1ca6936ac67e7a3fe1bbe585db2eafeaa515/docs/conf.py#L100-L134

import importlib
import inspect
import subprocess

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

    try:
        file = inspect.getsourcefile(obj)
        lines = inspect.getsourcelines(obj)
    except TypeError:
        # e.g. object is a typing.Union
        return None
    path_start = file.find("daft/")
    if path_start == -1:
        return None
    file = file[path_start:]
    start, end = lines[1], lines[1] + len(lines[0]) - 1

    return f"{code_url}/{file}#L{start}-L{end}"
