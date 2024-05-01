# [WIP] Building docs locally

1. Go to the `docs/` folder
2. `make clean`
3. `make html`
4. `open build/html/index.html`

To create a new directory level:

- create a folder under `source`
- add an `index.rst`, follow template from other `index.rst` files
- add new folder name to `toctree` at the end of `source/index.rst`
