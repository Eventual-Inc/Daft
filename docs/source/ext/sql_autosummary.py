import inspect
import os

from sphinx.ext.autosummary import Autosummary
from sphinx.util import logging

logger = logging.getLogger(__name__)


TOCTREE = "doc_gen/sql_funcs"
SQL_MODULE_NAME = "daft.sql._sql_funcs"

STUB_TEMPLATE = """
.. currentmodule:: None

.. autofunction:: {module_name}.{name}
"""


class SQLAutosummary(Autosummary):
    def run(self):
        func_names = get_sql_func_names()
        # Run the normal autosummary stuff, override self.content
        self.content = [f"~{SQL_MODULE_NAME}.{f}" for f in func_names]
        nodes = super().run()
        return nodes

    def get_sql_module_name(self):
        return self.arguments[0]


def get_sql_func_names():
    # Import the SQL functions module
    module = __import__(SQL_MODULE_NAME, fromlist=[""])

    names = []
    for name, obj in inspect.getmembers(module):
        if inspect.isfunction(obj) and not name.startswith("_"):
            names.append(name)

    return names


def generate_stub(name: str):
    """Generates a stub string for a SQL function"""
    stub = name + "\n"
    stub += "=" * len(name) + "\n\n"
    stub += STUB_TEMPLATE.format(module_name=SQL_MODULE_NAME, name=name)
    return stub


def generate_files(app):
    # Determine where to write .rst files to
    output_dir = os.path.join(app.srcdir, "api_docs", TOCTREE)
    os.makedirs(output_dir, exist_ok=True)

    # Write stubfiles
    func_names = get_sql_func_names()
    for name in func_names:
        stub_content = generate_stub(name)
        filename = f"{SQL_MODULE_NAME}.{name}.rst"
        filepath = os.path.join(output_dir, filename)
        with open(filepath, "w") as f:
            f.write(stub_content)

        # HACK: Not sure if this is ok?
        app.env.found_docs.add(filepath)


def setup(app):
    app.add_directive("sql-autosummary", SQLAutosummary)

    # Generate and register files when the builder is initialized
    app.connect("builder-inited", generate_files)

    return {
        "version": "0.1",
        "parallel_read_safe": True,
        "parallel_write_safe": True,
    }
