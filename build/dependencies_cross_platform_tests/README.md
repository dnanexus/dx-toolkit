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

### cryptography

Seems to be unused in the code and present because of historical issues with SSL implementation in Python.

### pyreadline and pyreadline3

Used for TAB completion in interactive commands `dx run` and `dx-app-wizard` on Windows.

### psutil

Information about process in `DXConfig` and memory information in `dx-download-all-inputs --parallel`.

### python-dateutil

Date parsing in `dxpy.utils.normalize_time_input` function.

### requests

Seems to be used only as a provider of exceptions, HTTP code mappings and helpers. Mostly used in `dxpy.__init__`. HTTP connections seem to be handled by `urllib3.PoolManager` directly.

### urllib3

Everything related to HTTP requests using their `PoolManager`/`ProxyManager`. Mostly used in `dxpy.__init__`.

### websocket-client

Used for streaming execution logs in `dx watch`.

## Deprecated/failing environments

### Linux

* `debian-10-py2-sysdeps` - problem with psutil installation
* `debian-10-py3-sysdeps` - problem with psutil installation
* `dx-aee-16.04-0` - dx-toolkit is installed using distribution tarball and thus the environment is not ready for installation from the source
* `rhel-7-py2-sysdeps` - problem with incompatible requests which cannot be uninstalled first
* `ubuntu-18.04-py2-sysdeps` - problem with psutil installation
* `ubuntu-18.04-py3-sysdeps` - problem with psutil installation

### Windows

* `official-3.6` - cannot install `psutil` as there are missing C++ build tools which cannot be installed by Chocolatey easily

### MacOS

* `brew-3.7` - only for `x86_64`, deprecated formula