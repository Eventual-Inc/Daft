#!/bin/bash

./gradle uberJar

python3 setup.py build sdist