import os
import shutil
import subprocess


def make_api_docs(*args, **kwargs):
    env = os.environ.copy()
    env["PATH"] = f"{os.path.abspath('.venv/bin')}:{env['PATH']}"

    # Run sphinx-build directly instead of using make
    venv_path = os.getenv("READTHEDOCS_VIRTUALENV_PATH", ".venv")
    sphinx_build = os.path.join(os.path.abspath(f"{venv_path}/bin"), "sphinx-build")
    subprocess.run(
        [
            sphinx_build,
            "-b",
            "html",
            "docs/sphinx/source",  # source dir
            "docs/sphinx/_build",  # build dir
        ],
        check=True,
        env=env,
    )

    # Copy built docs to mkdocs directory
    shutil.copytree("docs/sphinx/_build", "docs/mkdocs/api_docs", dirs_exist_ok=True)
