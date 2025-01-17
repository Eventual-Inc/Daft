import shutil
import subprocess

def make_api_docs(*args, **kwargs):
    subprocess.run(["make", "html"])
    shutil.copytree("sphinx/_build/html", "mkdocs/api_docs", dirs_exist_ok=True)

