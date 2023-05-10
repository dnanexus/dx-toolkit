FROM ubuntu:16.04

SHELL ["/bin/bash", "-c"]
ENV DEBIAN_FRONTEND=noninteractive
ENV DXPY_TEST_PYTHON_VERSION=2

RUN \
    apt-get update && \
    apt-get install -y python python-pip python3-pip python3-venv libffi-dev && \
    python3 -m pip install --quiet pip==20.3.4 setuptools==50.3.2 wheel==0.37.1 && \
    python2 -m pip install --quiet 'packaging<21.0' 'pyparsing<=2.4.5' six appdirs && \
    python2 -m pip install --quiet --upgrade 'pip<21.0' && \
    python2 -m pip install --quiet setuptools==10.2 && \
    python3 -m venv /pytest-env

RUN \
    source /pytest-env/bin/activate && \
    python3 -m pip install --quiet pip==20.3.4 setuptools==50.3.2 wheel==0.37.1 && \
    python3 -m pip install --quiet pytest==6.1.2 pexpect

COPY run_tests.sh /

ENTRYPOINT [ "/run_tests.sh" ]
