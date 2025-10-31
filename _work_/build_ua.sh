#!/usr/bin/env bash

# Prepare the builder image
docker build -t upload-agent-builder -f "$(pwd)/_work_/Dockerfile" .

# Build upload agent in the upload-agent-builder container
docker run --rm \
    -v "$(pwd):/dx-toolkit" \
    -w /dx-toolkit/src \
    upload-agent-builder \
    bash -c "make ua_deps 2>&1 | tee ../_work_/build.log"
    # bash -c "make ua 2>&1 | tee ../_work_/build.log"

# Run interactive
# docker run -it -v "$(pwd):/dx-toolkit" -w /dx-toolkit/src upload-agent-builder
