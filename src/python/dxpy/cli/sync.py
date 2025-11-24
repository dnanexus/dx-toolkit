# Copyright (C) 2013-2016 DNAnexus, Inc.
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

from ..api import bulk_new_symlinks
from ..utils.loader import run_with_loader

class SyncCommand:
    def __init__(self, args):
        self.args = args

    def validate_args(self):
        project_id, project_path = self.args.project.split(":", 1)
        drive_id, drive_path = self.args.drive.split(":", 1)
        if not project_id or not isinstance(project_id, str):
            raise ValueError("project_id must be a non-empty string")
        if not drive_id or not isinstance(drive_id, str):
            raise ValueError("drive_id must be a non-empty string")

        return project_id, project_path, drive_id, drive_path

    def sync(self):
        result = run_with_loader(self.__sync_objects, text="Syncing objects...")
        if not self.args.quiet:
            print(result)

    def __sync_objects(self):
        project_id, project_path, drive_id, drive_path = self.validate_args()
        input_params = {
            "project": project_id,
            "drive": drive_id,
            "sourcePath": drive_path,
            "destinationPath": project_path,
        }

        result = []
        continuation_token = None

        while True:
            if continuation_token:
                input_params["continuationToken"] = continuation_token
            response = bulk_new_symlinks(input_params)
            result.extend(response.get("objects", []))
            continuation_token = response.get("continuationToken")
            if not continuation_token:
                break

        return result
