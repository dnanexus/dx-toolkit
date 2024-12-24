# dxpy dependencies cross-platform tests

## Usage

### Linux

1. Install Python and Docker: `sudo apt install --yes python3 docker.io`
2. Install Docker Python: `python3 -m pip install docker`
3. Run tests: `./run_linux.py -t <token> -d <dx-toolkit path> ...`

### Windows

1. Run PowerShell as an administrator
2. Prepare environment (Pythons, etc.): `powershell.exe -ExecutionPolicy Unrestricted windows\prepare.ps1`
3. Run tests: `python3.11 run_windows.py -t <token> -d <dx-toolkit path> ...`

### MacOS

1. Install dependencies: `bash macos/prepare.sh`
2. Run tests: `./run_macos.sh -t <token> -d <dx-toolkit path> ...`

## Where are libraries used

### argcomplete

Argument completion for `dx` commands in Unix shells. Tested usign `pexpect`.

### colorama

Colorized terminal output on Windows. Called directly in `dx` and `dx-app-wizard` scripts. Don't know how to test automatically.

### pyreadline and pyreadline3

Used for TAB completion in interactive commands `dx run` and `dx-app-wizard` on Windows.

### psutil

Information about process in `DXConfig` and memory information in `dx-download-all-inputs --parallel`.

### python-dateutil

Date parsing in `dxpy.utils.normalize_time_input` function.

### urllib3

Everything related to HTTP requests using their `PoolManager`/`ProxyManager`. Mostly used in `dxpy.__init__`.

### websocket-client

Used for streaming execution logs in `dx watch`.

## Deprecated/failing environments

### Linux

* `*-py2-*` - Python 2.7 is not supported by dx-toolkit
* `pyenv-3.6` - Python 3.6 is not supported by dx-toolkit
* `pyenv-3.7` - Python 3.6 is not supported by dx-toolkit
* `debian-10-py3-sysdeps` - problem with psutil installation
* `dx-aee-16.04-0` - dx-toolkit is installed using distribution tarball and thus the environment is not ready for installation from the source
* `ubuntu-18.04-py3-sysdeps` - problem with psutil installation

### Windows

* `*-2.7` - Python 2.7 is not supported by dx-toolkit
* `*-3.6` - Python 3.6 is not supported byt dx-toolkit
* `*-3.7` - Python 3.7 is not supported byt dx-toolkit

### MacOS

* `*-2.7` - Python 2.7 is not supported by dx-toolkit
* `*-3.6` - Python 3.6 is not supported byt dx-toolkit
* `*-3.7` - Python 3.7 is not supported byt dx-toolkit
