#!/bin/bash

# Symlink the daft release into $HOME
ln -s /opt/daft $HOME

# Demo:
cp /opt/notebooks/* $HOME

exec "$@"
