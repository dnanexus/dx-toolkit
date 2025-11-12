#!/usr/bin/env bash

# Script for building dx-toolkit components in builder container

# MAKE_CMD="dx-verify-file"
MAKE_CMD="${1:-all}"

# Prepare the builder image
docker build -t upload-agent-builder -f "$(pwd)/build/docker/Dockerfile" .

# Build upload agent in the upload-agent-builder container
docker run --rm \
    -v "$(pwd):/dx-toolkit" \
    -w /dx-toolkit/src \
    upload-agent-builder \
    bash -c "make ${MAKE_CMD}"
