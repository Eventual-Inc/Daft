# Contributing to Docs

## Build Daft documentation

1. Go to the `/` folder (project root)
2. `make docs`
3. `python -m http.server`
3. open `localhost:8000/site`

## Run the build in development server

1. Go to the `/` folder (project root)
2. `make serve`
3. open `http://127.0.0.1:8000/projects/docs/en/stable/`

## Add a new page to User Guide:

1. Create a `.md` file in `docs` or add to relevant folder in `docs`
2. Add file to `mkdocs.yml` navigation under `Daft User Guide`

## Add a new page to API Docs:

1. Create a `.md` file in `docs/api_docs` or add to relevant folder in `docs/user_guide`
2. Add file to `mkdocs.yml` navigation under `API Docs`
