#!/usr/bin/env bash

# Script for building Upload Agent with builder image.
# This could be extended to support building other dx-toolkit components.

MAKE_CMD="make clean && make ua"

# Prepare the builder image
docker build -t upload-agent-builder -f "$(pwd)/build/docker/Dockerfile" .

# Build upload agent in the upload-agent-builder container
docker run --rm \
    -v "$(pwd):/dx-toolkit" \
    -w /dx-toolkit/src \
    upload-agent-builder \
    bash -c "${MAKE_CMD}"
