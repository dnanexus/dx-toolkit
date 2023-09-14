from dxpy import DXHTTPRequest


class Dataset:
    def __init__(self, record_id, project_id):
        self.record_id = record_id
        self.project_id = project_id
        self.vizualise_info = None

    def get_vizualise_info(self):
        if self.vizualise_info is None:
            self.vizualise_info = DXHTTPRequest(
                "/" + self.record_id + "/visualize",
                {"project": self.project_id, "cohortBrowser": False},
            )
        return self.vizualise_info

    def __getattr__(self, key_name):
        if key_name in self.get_vizualise_info():
            return self.get_vizualise_info().get(key_name)

    @property
    def cohort_flag(self):
        return (
            True
            if "CohortBrowser" in self.get_vizualise_info().get("recordTypes")
            else False
        )
