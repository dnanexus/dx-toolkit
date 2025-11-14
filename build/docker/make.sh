#!/usr/bin/env bash

# Script for building dx-toolkit components in builder container

MAKE_CMD="${1:-all}"

# Prepare the builder image
docker build -t dx-toolkit-builder -f "$(pwd)/build/docker/Dockerfile" .

# Build upload agent in the dx-toolkit-builder container
docker run --rm \
    -v "$(pwd):/dx-toolkit" \
    -w /dx-toolkit/src \
    dx-toolkit-builder \
    bash -c "make ${MAKE_CMD}"
