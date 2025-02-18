# Daft Dashboard

Daft dashboard is a locally-hosted web-application that allows users to view some statistics and metrics regarding their previously run queries in their browser.

Daft dashboard was designed specifically to be locally-hosted; the static HTML/CSS/JS files are included in the installed wheel (upon running `uv pip install "getdaft\[dashboard\]"` and are served via a simple local file-server.
Namely, no internet connection is required to run this feature.

## Usage

```py
from daft import dashboard
import daft

dashboard.launch()

df = daft.from_pydict({"nums": [1,2,3]})
df.agg(daft.col("nums").stddev()).show()
```

Or, if you already have a dashboard instance running:

```sh
DAFT_DASHBOARD=1 python my_daft_script.py
```

## Project structure

```txt
daft_dashboard/
    |- frontend/                   # nextjs web-application
    |- src/                        # rust server code
    |- daft_dashboard/             # python bindings to rust server code
        |- static-dashboard-assets # statically compiled HTML/CSS/JS files to be served to the browser
```

## Building

### Prerequisites

You'll need [`bun`](https://bun.sh/docs/installation) installed prior to running the below builds.

### Development

1. Run the NextJS development server:
```sh
cd daft_dashboard/frontend
bun dev
```
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

### Release

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

## Design decisions

### Reason for choosing NextJS

NextJS provides some advanced features such as SSR.
However, since we only want to run a simple file server in production (instead of a dynamic, server-side HTML generating server), we do *not* use the SSR features that NextJS provides.
Thus, we ensure that the NextJS application is void of all dynamic routes, dynamic props, etc..
(You can verify if your NextJS application is void of dynamic features by running `bun run build`; if that fails, you have some dynamic features in your source code).
Since the NextJS application is void of dynamic features, we can compile the application down to simple html/css/js, which can easily be served by a dummy file-server.

### The rust server's responsibilities in different modes

In dev-mode, we start two processes: the frontend NextJS server and the backend rust server.
The NextJS server provides a whole host of useful functions, the main one being hot-reloading upon save.
This helps increase development speed.

However, in release-mode, we only run the backend server process.
In release-mode, the backend server process *also* runs the html/css/js server, since we *don't* run a NextJS server in the background.
This is to keep the architecture as simple as possible.

### Reason for choosing rust for the backend application

Daft internals are written in rust, so daft-dashboard's internals being written in rust seems appropriate.
Rust also has some good support for writing http applications and file serving.
