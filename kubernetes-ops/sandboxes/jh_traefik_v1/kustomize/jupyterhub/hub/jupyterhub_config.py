# flake8: noqa

# Jupyterhub configuration: https://jupyterhub.readthedocs.io/en/stable/getting-started/config-basics.html#generate-a-default-config-file

import os

# Networking
c.JupyterHub.proxy_class = "traefik_etcd"
c.TraefikEtcdProxy.should_start = False
c.TraefikEtcdProxy.kv_url = "http://jupyterhub-proxy-etcd-cluster-client:2379"
c.TraefikEtcdProxy.traefik_api_url = "http://jupyterhub-proxy:8001"
c.TraefikEtcdProxy.traefik_api_username = "jupyterhub"
c.TraefikEtcdProxy.traefik_api_password = os.environ["PROXY_API_PASSWORD"]
c.JupyterHub.bind_url = "http://:8081"
c.JupyterHub.hub_bind_url = "http://:8081"
c.JupyterHub.hub_connect_url = "http://jupyterhub:8081"

# Authentication
c.JupyterHub.authenticator_class = "jupyterhub.auth.DummyAuthenticator"

# Spawner
c.JupyterHub.spawner_class = "kubespawner.KubeSpawner"
c.KubeSpawner.start_timeout = 60 * 10

# Miscellaneous
c.JupyterHub.allow_named_servers = True
c.JupyterHub.cleanup_servers = False
c.JupyterHub.concurrent_spawn_limit = 1
c.JupyterHub.db_url = "postgresql://postgres@postgres:5432/postgres?sslmode=require"
