#!/usr/bin/env bash

# Prepare Docker image
docker build -t ua-builder -f "$(pwd)/_work_/Dockerfile" .

# Build upload agent in the Docker container
docker run --rm \
    -v "$(pwd):/dx-toolkit" \
    -w /dx-toolkit/src \
    ua-builder \
    bash -c "make ua"
