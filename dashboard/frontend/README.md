# Daft Dashboard

## Installation

You will need the following installed prior to running this dashboard fully-featured.

- [Bun](https://bun.sh): `curl -fsSL https://bun.sh/install | bash`
- [Ray](https://www.ray.io): `uv v; source .venv/bin/activate; uv pip install ray[default]`
- [Prometheus](https://prometheus.io): `brew install prometheus`
- [Grafana](https://grafana.com): `brew install grafana`

## Getting Started

In order to get a fully functioning dashboard, you will need Daft Dashboard, Ray, Prometheus, and Grafana running concurrently.
Run the following commands to start them all:

```sh
# start grafana
grafana server ...

# start ray
source .venv/bin/activate
ray start --head --metrics-export-port=8080

# start prometheus
ray metrics launch-prometheus

# start daft dashboard
cd dashboard
bun dev
```

If all the above commands succeed, point your browser towards `http://localhost:3000` (if port 3000 is in use, then `bun` will try to bind to port 3001).
