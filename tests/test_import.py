from __future__ import annotations

import subprocess
import sys
import textwrap


def test_daft_import_does_not_import_pyarrow():
    # Run the import in a separate process to ensure a clean sys.modules
    code = textwrap.dedent(
        """
        import sys
        # Remove pyarrow and daft from sys.modules if they exist
        modules_to_remove = []
        for module_name in list(sys.modules.keys()):
            if module_name.startswith(("pyarrow", "daft")):
                modules_to_remove.append(module_name)

        for module_name in modules_to_remove:
            del sys.modules[module_name]

        # Import daft
        import daft  # noqa: F401

        # Check that no pyarrow modules were imported (including nested ones)
        pyarrow_modules = [name for name in sys.modules.keys() if name.startswith("pyarrow")]
        if len(pyarrow_modules) > 0:
            raise RuntimeError(f"PyArrow modules were imported: {pyarrow_modules}")
        """
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise AssertionError(
            f"PyArrow modules were imported during daft import:\n" f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
