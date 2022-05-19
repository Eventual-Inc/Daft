#!/bin/bash

# Symlink the daft release into $HOME
ln -s /opt/daft $HOME/daft

exec "$@"
