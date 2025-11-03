FROM ubuntu:24.04

# Docker image for building Upload Agent.
# This could be extended to support building other dx-toolkit components.

# Set environment variables to avoid interactive prompts during package installation
ENV DEBIAN_FRONTEND=noninteractive

# Update the package list and install development essentials
RUN apt-get update && apt-get install -y \
    curl \
    libbz2-dev \
    libicu-dev \
    libidn11-dev \
    libssl-dev \
    make \
    python3 \
    python3-pip \
    python3-venv \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

CMD ["/bin/bash"]
