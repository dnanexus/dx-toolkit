#!/usr/bin/env bash

docker run --rm -v "$(pwd)/src:/src" -w /src ubuntu:24.04 bash -c "make ua"
