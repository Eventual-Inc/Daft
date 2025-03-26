# Contributing to Docs

## Building the mkdocs

1. Go to the `/` folder (project root)
2. `make docs`
3. `python -m http.server`
3. open `localhost:8000/docs/site`

## Building sphinx docs locally

1. Go to the `docs/` folder
2. `make clean`
3. `make docs`
3. open `docs/site/index.html` to view static pages

## To create a new directory level:

- Create a folder under `sphinx/source`
- Add an `index.rst`, follow template from other `index.rst` files
- Add new folder name to `toctree` at the end of `sphinx/source/index.rst`

## To add a new page to User Guide:

- Create a `.md` file in `docs/mkdocs` or add to relevant folder in `docs/mkdocs`
- Add file to `docs/mkdocs.yml` navigation
