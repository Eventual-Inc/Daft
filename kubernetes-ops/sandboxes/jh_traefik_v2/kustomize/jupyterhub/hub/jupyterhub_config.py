# flake8: noqa

# Jupyterhub configuration: https://jupyterhub.readthedocs.io/en/stable/getting-started/config-basics.html#generate-a-default-config-file

import os

# Networking
c.JupyterHub.bind_url = "http://:8081"
c.JupyterHub.hub_bind_url = "http://:8081"
c.JupyterHub.hub_connect_url = "https://jupyterhub:8081"

# Authentication
c.JupyterHub.authenticator_class = "jupyterhub.auth.DummyAuthenticator"

# Spawner
c.JupyterHub.spawner_class = "kubespawner.KubeSpawner"
c.KubeSpawner.start_timeout = 60 * 10

# Proxy
c.JupyterHub.proxy_class = "kubespawner.proxy.KubeIngressProxy"
c.Proxy.should_start = False
c.KubeIngressProxy.extra_annotations = {
    "kubernetes.io/ingress.class": "jupyterhub",
    "traefik.ingress.kubernetes.io/router.entrypoints": "https",
    "traefik.ingress.kubernetes.io/service.serversscheme": "https",
    "traefik.ingress.kubernetes.io/service.serverstransport": "jupyterhub-proxy@file",
}

# Miscellaneous
c.JupyterHub.cleanup_servers = False
c.JupyterHub.concurrent_spawn_limit = 1
c.JupyterHub.db_url = "postgresql://postgres@postgres:5432/postgres?sslmode=require"
c.JupyterHub.internal_ssl = True
c.JupyterHub.external_ssl_authorities = {
    name: {
        "cert": f"/srv/jupyterhub/certs/{name}/tls.crt",
        "key": f"/srv/jupyterhub/certs/{name}/tls.key",
    }
    for name in (
        "hub-ca",
        "notebooks-ca",
        "proxy-api-ca",
        "proxy-client-ca",
        "services-ca",
    )
}
c.JupyterHub.recreate_internal_certs = True
c.JupyterHub.trusted_alt_names = ["DNS:jupyterhub"]
