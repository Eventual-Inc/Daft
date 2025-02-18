# Daft Dashboard

## Structure

```txt
daft_dashboard/
    |- frontend/                   # nextjs web-application
    |- src/                        # rust server code
    |- daft_dashboard/             # python bindings to rust server code
        |- static-dashboard-assets # statically compiled HTML/CSS/JS files to be served to the browser
```

## Building for development

1. Run the nextjs development server:
```sh
cd daft_dashboard/frontend
bun dev
```
(you will need to have [`bun`](https://bun.sh/docs/installation) installed).
This will start a server on port `3000`.
You can verify this by going to your browser and navigating to `http://localhost:3000`.

2. Run the API/broadcast server:
```sh
cd daft_dashboard
cargo r
```
You can verify that the server is up and running by hitting a simple API endpoint which returns all the queries the server has observed thus far:
```sh
curl http://localhost:3238/api/queries
# should output `[]`
```

Since the `nextjs` and API server are both up, you can start running queries against them:

```py
import daft
daft.from_pydict({"nums": [1,2,3]}).collect()
```

## Building for release

```sh
# build the static html/css/js assets
make static-dashboard-assets

# compile the rust-server code and build the .so for the python package to link against
make daft-dashboard
```

The second `make` command will install the `daft-dashboard` library into your current virtual env.
You can now enter into a repl (or a script, for example) and experiment with it:

```py
from daft import dashboard
dashboard.launch(block=True)
```

Note that the first `make` command will create new auto-generated HTML/CSS/JS files with unique names and move them into the appropriate directory.
This will cause a `git diff` to appear (since at least the file names will be different).
