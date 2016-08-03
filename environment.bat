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
REM Run this file in Command Prompt to initialize DNAnexus environment
REM variables:
REM >environment.bat

REM Resolve the location of this file
set "SOURCE_DIR=%~dp0"
set "DNANEXUS_HOME=%SOURCE_DIR%"

REM Place DNANEXUS_HOME bin dir in PATH
set "PATH=%DNANEXUS_HOME%bin;%PATH%"
REM Place C:\Python27 and its Scripts dir in PATH
REM TODO - check whether they're already there or not
REM set "PATH=C:\Python27;C:\Python27\Scripts;%PATH%"

REM Enable Python to locate dxpy and other dependencies
set "PYTHONPATH=%DNANEXUS_HOME%share\dnanexus\lib\python2.7\site-packages;%PYTHONPATH%"
