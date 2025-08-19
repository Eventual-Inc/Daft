from __future__ import annotations

import sys


def test_daft_import_does_not_import_pyarrow():
    # Remove pyarrow and daft from sys.modules if they exist
    modules_to_remove = []
    for module_name in list(sys.modules.keys()):
        if module_name.startswith(("pyarrow", "daft")):
            modules_to_remove.append(module_name)

    for module_name in modules_to_remove:
        del sys.modules[module_name]

    # Import daft

    # Check that no pyarrow modules were imported (including nested ones)
    pyarrow_modules = [name for name in sys.modules.keys() if name.startswith("pyarrow")]
    assert len(pyarrow_modules) == 0, f"PyArrow modules were imported: {pyarrow_modules}"
