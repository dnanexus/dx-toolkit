# Use the official Ubuntu base image
FROM ubuntu:20.04

# Set environment variables to avoid interactive prompts during package installation
ENV DEBIAN_FRONTEND=noninteractive

# Update the package list and install development essentials
RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    git \
    wget \
    curl \
    python3 \
    python3-pip \
    python3-venv \
    pkg-config \
    libssl-dev \
    libpsl-dev \
    zlib1g-dev \
    vim \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Set Python 3 as the default python command
RUN update-alternatives --install /usr/bin/python python /usr/bin/python3 1

# Verify installation of essential tools
RUN python --version && \
    make --version && \
    g++ --version && \
    cmake --version

# Default command to run when the container starts
CMD ["/bin/bash"]
