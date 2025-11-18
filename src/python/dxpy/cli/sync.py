from ..api import bulk_new_symlinks

class SyncCommand:
    def __init__(self, args):
        self.args = args

    def validate_args(self):
        project_id, project_path = self.args.project.split(":", 1)
        drive_id, drive_path = self.args.drive.split(":", 1)
        return project_id, project_path, drive_id, drive_path

    def sync(self):
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
            try:
                if continuation_token:
                    input_params["continuationToken"] = continuation_token
                response = bulk_new_symlinks(input_params)
                for obj in response.get("objects", []):
                    result.append(obj)
                continuation_token = response.get("continuationToken")
                if not continuation_token:
                    break
            except Exception as e:
                print(f"API call failed: {e}")
                break

        print(result)