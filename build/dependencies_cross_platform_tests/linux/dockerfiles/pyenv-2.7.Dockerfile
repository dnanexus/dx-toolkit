FROM --platform=amd64 ubuntu:22.04

SHELL ["/bin/bash", "-c"]
ENV DEBIAN_FRONTEND=noninteractive
ENV DXPY_TEST_PYTHON_VERSION=2
ENV DXPY_TEST_USING_PYENV=true

RUN \
    apt-get update && \
    apt-get install -y curl build-essential git zlib1g-dev libssl-dev libbz2-dev libffi-dev libncurses-dev libreadline-dev libsqlite3-dev liblzma-dev python3-pip python3-venv && \
    curl https://pyenv.run | bash

RUN \
    python3 -m venv /pytest-env

ENV PYENV_ROOT=/root/.pyenv
ENV PATH="${PYENV_ROOT}/bin:${PATH}"
ENV PYENV_PYTHON_VERSION=2.7

RUN \
    eval "$(pyenv init -)" && \
    pyenv install ${PYENV_PYTHON_VERSION} && \
    pyenv global ${PYENV_PYTHON_VERSION}

RUN \
    source /pytest-env/bin/activate && \
    python3 -m pip install --quiet pytest pexpect

COPY run_tests.sh /

ENTRYPOINT [ "/run_tests.sh" ]
