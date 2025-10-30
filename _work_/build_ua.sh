#!/usr/bin/env bash

# Prepare the builder image
docker build -t ua-builder -f "$(pwd)/Dockerfile" .

# Build upload agent in the ua-builder container
docker run --rm \
    -v "$(pwd):/dx-toolkit" \
    -w /dx-toolkit \
    ua-builder \
    bash -c "make ua 2>&1 | tee make.log"

# Run interactive
# docker run -it -v "$(pwd):/dx-toolkit" -w /dx-toolkit ua-builder
