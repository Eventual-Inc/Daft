# Daft Dashboard

The Daft Dashboard is a web UI for inspecting Daft query execution. Daft scripts push query metadata, plans, and execution stats to the dashboard server, which you can browse to investigate query behavior in real time.

The dashboard ships pre-built with the `daft` Python package — `pip install daft` is all you need, no separate install step or extra dependencies.

!!! note
    The dashboard is under active development. Some operational ergonomics (persistence, compatibility, authentication) are still evolving — feedback is welcome on the [Daft Slack](https://daft.ai/slack) or [GitHub](https://github.com/Eventual-Inc/Daft/issues).

## How It Works

The dashboard has two pieces:

1. A **dashboard server** that serves the web UI and accepts query-event reports over HTTP.
2. Your **Daft script**, which reports query events to the dashboard server when the `DAFT_DASHBOARD_URL` environment variable is set.

The relationship is push-based: your Daft script (and any Ray actors running on its behalf) sends events *to* the dashboard. The dashboard does not need to know where your script or cluster is — it only needs to be reachable over HTTP from anywhere that runs Daft code.

## Running Locally

Running the dashboard alongside a local Daft script is the quickest way to try it out.

In one terminal, start the dashboard server:

```bash
daft dashboard start
```

By default this listens on `0.0.0.0:3238`. You can override the address and port:

```bash
daft dashboard start -a 127.0.0.1 -p 3238
```

Useful flags:

- `-a, --addr <ADDR>`: address to bind to (default `0.0.0.0`).
- `-p, --port <PORT>`: port to bind to (default `3238`).
- `-v, --verbose`: log HTTP requests and responses.
- `-d, --daemon`: run in the background (Unix only). Logs go to `$DAFT_DASHBOARD_LOG_DIR/daft_dashboard.log` (defaults to your system's temp directory). Stop the daemon with `daft dashboard stop`.

In another terminal, point your Daft script at the dashboard via the `DAFT_DASHBOARD_URL` environment variable:

```bash
DAFT_DASHBOARD_URL=http://localhost:3238 python script.py
```

Then open `http://localhost:3238` in a browser to view query activity.

## Running in a Cluster Environment

When running Daft on a [Ray cluster](../distributed/ray.md) (whether self-managed or on a platform like [Kubernetes](../distributed/kubernetes.md)), three things need to be true for the dashboard to work:

1. The dashboard server is running somewhere reachable.
2. Both the **Python driver script** and the **Daft scheduler actor** (running on the Ray head) can make HTTP requests to the dashboard's `DAFT_DASHBOARD_URL`.
3. You can reach the dashboard from your browser (typically via port-forwarding, an ingress, or a `ray attach --port-forward`).

### Where to Run the Dashboard

The dashboard is a standalone HTTP server, so it can be deployed in several places. Pick one based on your topology:

- **On the Ray head node.** Simplest if your Python driver runs on the head node. You can forward a local port to view it (`ray attach --port-forward 3238`). Note that the dashboard will compete for resources on the head.
- **On a separate "runner" / driver pod.** If you have a topology where the Python driver runs in its own pod (e.g., an orchestration pod that talks to a Ray cluster via Ray Client), running the dashboard as a sidecar in that pod is usually the cleanest option. It keeps dashboard load off the Ray head and you can expose it via a Kubernetes Service or Ingress.
- **On a dedicated host or pod.** Any host reachable by both the driver and the Ray head works. Just be sure firewall rules / NetworkPolicies allow inbound HTTP on the dashboard port.

The dashboard does not need to be told where the Ray cluster is. It is a passive recipient — Daft queries push events to it.

### Configuring `DAFT_DASHBOARD_URL`

Set `DAFT_DASHBOARD_URL` on the Python driver process. Daft propagates it to the Ray actors that run query work, so you do not need to set it separately on the workers.

The URL must be **routable from every process that runs Daft code** — both the driver and the Ray head/workers. In particular:

- `http://localhost:3238` only works if the dashboard, driver, **and** Ray actors are all on the same host. On Ray, actors will resolve `localhost` to their own node, not the driver's, so this almost never works in a cluster.
- Use a hostname or address that is routable across the cluster — for example a Kubernetes Service DNS name (`http://daft-dashboard.my-namespace.svc:3238`), the runner pod's hostname, or the head node's IP.

Example for a Kubernetes deployment where the dashboard runs as a sidecar in the runner pod:

```bash
# In the runner pod, alongside the Python script:
daft dashboard start -p 3238 --daemon

# Run the script with DAFT_DASHBOARD_URL pointing at a hostname
# the Ray head and workers can resolve.
export DAFT_DASHBOARD_URL=http://runner-01.my-namespace.svc:3238
python script.py
```

To make the UI accessible to humans (not just Daft processes), expose the dashboard port through whatever mechanism your platform uses — a Kubernetes `Service` plus `Ingress`, an SSH tunnel, `ray attach --port-forward`, or a cloud load balancer.

### Production Readiness

- The dashboard has no built-in authentication. Do not expose it directly to the public internet. Place it behind an authenticating ingress or a VPN.
- Binding to `0.0.0.0` is convenient but means the dashboard accepts connections from any interface. Restrict access at the network layer (security groups, NetworkPolicies, firewall rules).
- Dashboard state currently lives in memory and is lost when the process restarts.
- The wire protocol between Daft and the dashboard is not yet stable across versions. A long-lived dashboard process may not correctly handle events from Daft scripts running on a different (older or newer) version than the dashboard itself was built from. For now, run the dashboard from the same Daft version as the scripts reporting to it, and restart it when you upgrade.
