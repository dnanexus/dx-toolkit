# Copyright (C) 2013-2019 DNAnexus, Inc.
#
# This file is part of dx-toolkit (DNAnexus platform client libraries).
#
#   Licensed under the Apache License, Version 2.0 (the "License"); you may not
#   use this file except in compliance with the License. You may obtain a copy
#   of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#   WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#   License for the specific language governing permissions and limitations
#   under the License.
import dxpy
from functools import wraps

from processing import run_cmd, chain_cmds
from transfers import (
    Uploader,
    Downloader,
    upload_file,
    tar_and_upload_files,
    download_file,
)

def requires_worker_context(func):
    """This decorator checks that a given function is running within a DNAnexus job context"""
    @wraps(func)
    def check_job_id(*args, **kwargs):
        if dxpy.JOB_ID is None:
            raise dxpy.DXError("Illegal function call, must be called from within DNAnexus job context.")
        else:
            return func(*args, **kwargs)

    return check_job_id
