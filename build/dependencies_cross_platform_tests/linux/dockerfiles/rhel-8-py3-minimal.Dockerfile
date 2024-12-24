FROM --platform=amd64 almalinux:8

SHELL ["/bin/bash", "-c"]
ENV DXPY_TEST_PYTHON_VERSION=3

RUN \
    dnf install -y which diffutils python38 python38-pip && \
    python3 -m pip install --quiet --upgrade pip && \
    python3 -m venv /pytest-env

RUN \
    source /pytest-env/bin/activate && \
    python3 -m pip install --quiet pytest pexpect

COPY run_tests.sh /

ENTRYPOINT [ "/run_tests.sh" ]
