#!/bin/bash

# Symlink the daft release into $HOME
ln -s /opt/daft $HOME

# Add github to known_hosts so that jupyterlab-git doesn't fail mysteriously when cloning
if ! grep github.com ~/.ssh/known_hosts > /dev/null
then
    mkdir -p ~/.ssh
    ssh-keyscan github.com >> ~/.ssh/known_hosts
fi

# Demo:
cp /opt/notebooks/* $HOME

exec "$@"
