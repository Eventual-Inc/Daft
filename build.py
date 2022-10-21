from __future__ import annotations

import multiprocessing
import os
import sysconfig
import warnings
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow as pa
from Cython.Build import cythonize
from Cython.Distutils.build_ext import new_build_ext as cython_build_ext
from setuptools import Distribution, Extension

SOURCE_DIR = Path("daft")
BUILD_DIR = Path(".cython_build")


def get_extension_modules() -> list[Extension]:
    """Collect all .py files and turn them into Distutils/Setuptools
    Extensions"""

    extension_modules: list[Extension] = []

    for py_file in SOURCE_DIR.rglob("*.pyx"):
        # Get path (not just name) without .py extension
        module_path = py_file.with_suffix("")

        # Convert path to module name
        module_path = str(module_path).replace("/", ".")

        extension_module = Extension(
            name=module_path, sources=[str(py_file)], extra_compile_args=["-Wno-unused-variable"]
        )
        extension_module.include_dirs.append(np.get_include())
        extension_module.include_dirs.append(".")

        extension_module.include_dirs.append(pa.get_include())
        extension_module.libraries.extend(pa.get_libraries())

        extension_module.library_dirs.extend(pa.get_library_dirs())

        if not (
            os.path.exists(os.path.join(pa.get_library_dirs()[0], "libarrow.dylib"))
            or os.path.exists(os.path.join(pa.get_library_dirs()[0], "libarrow.so"))
        ):
            pa.create_library_symlinks()

        extension_module.extra_compile_args.append("-std=c++14")

        extension_modules.append(extension_module)

    return extension_modules


def cythonize_helper(extension_modules: list[Extension]) -> list[Extension]:
    """Cythonize all Python extensions"""
    num_threads = multiprocessing.cpu_count()
    if multiprocessing.get_start_method() == "spawn":
        num_threads = 0
        warnings.warn("multiprocessing start method is `spawn` which is incompatible with parallel builds")

    return cythonize(
        module_list=extension_modules,
        # Don't build in source tree (this leaves behind .c files)
        build_dir=BUILD_DIR,
        # Don't generate an .html output file. This will contain source.
        annotate=False,
        # Parallelize our build
        nthreads=num_threads,
        # Tell Cython we're using Python 3
        compiler_directives={"language_level": "3"},
        # (Optional) Always rebuild, even if files untouched
        force=True,
    )


def build(setup_kwargs: dict[str, Any]) -> None:
    extension_modules = cythonize_helper(get_extension_modules())
    print(f"Building on platform: {sysconfig.get_platform()}")

    setup_kwargs.update(
        {
            "ext_modules": extension_modules,
            "cmdclass": dict(build_ext=cython_build_ext),
            "zip_safe": False,
        }
    )


def build_inplace() -> None:
    setup = {}
    build(setup)
    distribution = Distribution(setup)
    distribution.run_command("build_ext")
    build_ext_cmd = distribution.get_command_obj("build_ext")
    build_ext_cmd.copy_extensions_to_source()


if __name__ == "__main__":
    build_inplace()
