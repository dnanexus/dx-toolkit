@echo off
REM Copyright (C) 2013-2016 DNAnexus, Inc.
REM
REM This file is part of dx-toolkit (DNAnexus platform client libraries).
REM
REM  Licensed under the Apache License, Version 2.0 (the "License"); you may not
REM  use this file except in compliance with the License. You may obtain a copy
REM  of the License at
REM
REM    http://www.apache.org/licenses/LICENSE-2.0
REM
REM  Unless required by applicable law or agreed to in writing, software
REM  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
REM  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
REM  License for the specific language governing permissions and limitations
REM  under the License.
REM
REM
REM Run this file in Command Prompt to initialize DNAnexus environment vars.

REM Set DNANEXUS_HOME to the location of this file
set "SOURCE_DIR=%~dp0"
set "DNANEXUS_HOME=%SOURCE_DIR%"

REM Add bin dir to PATH
set "PATH=%DNANEXUS_HOME%bin;%PATH%"

REM Check the registry for the Python27 path:
set PY27_KEY_NAME="HKEY_LOCAL_MACHINE\SOFTWARE\Wow6432Node\Python\PythonCore\2.7\InstallPath"
FOR /F "usebackq skip=2 tokens=1,2*" %%A IN (
 `REG QUERY %PY27_KEY_NAME% /ve 2^>nul`) DO (
    set PY_INSTALL_DIR=%%C
)
if defined PY_INSTALL_DIR echo PY_INSTALL_DIR = %PY_INSTALL_DIR%
if NOT defined PY_INSTALL_DIR (
    msg %USERNAME% Python installation dir not found!
    exit 1
)

REM Add Python27 and Python27\Scripts to PATH
set "PATH=%PY_INSTALL_DIR%;%PY_INSTALL_DIR%Scripts;%PATH%"

REM Set PYTHONPATH so the dx-*.exe wrappers can locate dxpy
set "PYTHONPATH=%DNANEXUS_HOME%share\dnanexus\lib\python2.7\site-packages"

REM Regenerate the dxpy console script .exe wrappers, so the .exes can
REM locate python.exe on this machine
REM python -m wheel install-scripts dxpy

REM Bring up the interactive shell
start cmd.exe /u /k echo DNAnexus CLI initialized. For help, run: 'dx help'
