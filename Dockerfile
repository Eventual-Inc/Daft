FROM python:3.9.5-slim-buster AS daft

WORKDIR /scratch

# Create a new user as jupyterhub expects running from user-space
RUN useradd -ms /bin/bash daftuser

# Adopted from jupyter/minimal-notebook
# Install commonly used utilities for development
RUN apt-get update --yes && \
    apt-get install --yes --no-install-recommends \
    # Common useful utilities
    git \
    nano-tiny \
    tzdata \
    unzip \
    vim-tiny \
    curl \
    # git-over-ssh
    openssh-client \
    # nbconvert dependencies
    # https://nbconvert.readthedocs.io/en/latest/install.html#installing-tex
    texlive-xetex \
    texlive-fonts-recommended \
    texlive-plain-generic && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Install AWS CLI
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" && \
    unzip awscliv2.zip && \
    ./aws/install && \
    rm -rf aws && \
    rm awscliv2.zip

# Install poetry and install Daft's dev python requirements
RUN curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py | python -
COPY poetry.lock /scratch/poetry.lock
COPY pyproject.toml /scratch/pyproject.toml
RUN /root/.poetry/bin/poetry config virtualenvs.create false
RUN --mount=type=cache,target=/root/.cache/pypoetry /root/.poetry/bin/poetry install
RUN /root/.poetry/bin/poetry export -f requirements.txt --output requirements.txt

USER daftuser
WORKDIR /home/daftuser

# HACK(jaychia): We should download the latest release and requirements of Daft instead of baking it into the image
COPY daft /opt/daft
USER root
RUN cp /scratch/requirements.txt /opt/daft/requirements.txt
USER daftuser

COPY scripts/jupyterhub-entrypoint.sh /app/entrypoint.sh

# Default entrypoint is set to be running Jupyterhub's single-user mode
ENTRYPOINT ["/app/entrypoint.sh", "jupyterhub-singleuser"]
