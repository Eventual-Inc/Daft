import multiprocessing
import os
from pathlib import Path
from typing import Any, Dict, List

import numpy as np
import pyarrow as pa
from Cython.Build import cythonize
from Cython.Distutils.build_ext import new_build_ext as cython_build_ext
from setuptools import Extension

SOURCE_DIR = Path("daft")
BUILD_DIR = Path(".cython_build")


# def build() -> None:
#     # Collect and cythonize all files
#     extension_modules = cythonize_helper(get_extension_modules())

#     # Use Setuptools to collect files
#     distribution = Distribution({
#         "ext_modules": extension_modules,
#         "cmdclass": {
#             "build_ext": cython_build_ext,
#         },
#     })

#     # Grab the build_ext command and copy all files back to source dir. This is
#     # done so that Poetry grabs the files during the next step in its build.
#     distribution.run_command("build_ext")
#     build_ext_cmd = distribution.get_command_obj("build_ext")
#     build_ext_cmd.copy_extensions_to_source()


def get_extension_modules() -> List[Extension]:
    """Collect all .py files and turn them into Distutils/Setuptools
    Extensions"""

    extension_modules: List[Extension] = []

    for py_file in SOURCE_DIR.rglob("*.pyx"):
        # Get path (not just name) without .py extension
        module_path = py_file.with_suffix("")

        # Convert path to module name
        module_path = str(module_path).replace("/", ".")

        extension_module = Extension(name=module_path, sources=[str(py_file)])
        extension_module.include_dirs.append(np.get_include())
        extension_module.include_dirs.append(pa.get_include())
        extension_module.libraries.extend(pa.get_libraries())

        extension_module.library_dirs.extend(pa.get_library_dirs())

        if not (
            os.path.exists(os.path.join(pa.get_library_dirs()[0], "libarrow.dylib"))
            or os.path.exists(os.path.join(pa.get_library_dirs()[0], "libarrow.so"))
        ):
            pa.create_library_symlinks()

        if os.name == "posix":
            extension_module.extra_compile_args.append("-std=c++11")

        extension_modules.append(extension_module)

    return extension_modules


def cythonize_helper(extension_modules: List[Extension]) -> List[Extension]:
    """Cythonize all Python extensions"""
    return cythonize(
        module_list=extension_modules,
        # Don't build in source tree (this leaves behind .c files)
        build_dir=BUILD_DIR,
        # Don't generate an .html output file. This will contain source.
        annotate=False,
        # Parallelize our build
        nthreads=multiprocessing.cpu_count() * 2,
        # Tell Cython we're using Python 3
        compiler_directives={"language_level": "3"},
        # (Optional) Always rebuild, even if files untouched
        force=True,
    )


import os

import numpy as np
import pyarrow as pa
from Cython.Build import cythonize


def build(setup_kwargs: Dict[str, Any]) -> None:
    extension_modules = cythonize_helper(get_extension_modules())

    setup_kwargs.update(
        {
            "ext_modules": extension_modules,
            "cmdclass": dict(build_ext=cython_build_ext),
            "zip_safe": False,
        }
    )
    print(setup_kwargs)
