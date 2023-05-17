FROM --platform=amd64 ubuntu:20.04

SHELL ["/bin/bash", "-c"]
ENV DEBIAN_FRONTEND=noninteractive
ENV DXPY_TEST_PYTHON_VERSION=3

RUN \
    apt-get update && \
    apt-get install -y python3 python3-pip python3-venv && \
    python3 -m pip install --quiet pip==20.0.2 setuptools==46.1.3 wheel==0.37.1 requests==2.23.0 cryptography==36.0.2 pyOpenSSL==22.0.0 secretstorage==3.3.1 && \
    python3 -m venv /pytest-env

RUN \
    source /pytest-env/bin/activate && \
    python3 -m pip install --quiet pytest pexpect

COPY run_tests.sh /

ENTRYPOINT [ "/run_tests.sh" ]
