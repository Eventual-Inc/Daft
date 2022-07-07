FROM jupyterhub/jupyterhub:latest

RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*
RUN pip install git+https://github.com/jeevb/kubespawner.git@068af4493746b943e362cfcbf884cbf29bbcdaec#egg=jupyterhub-kubespawner psycopg2-binary
