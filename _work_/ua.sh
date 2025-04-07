#!/usr/bin/env bash

docker build -t ua-builder -f $(pwd)/_work_/Dockerfile .

docker run --rm -v "$(pwd)/src:/dx-toolkit/src" -w /dx-toolkit/src ua-builder bash -c "make ua"
