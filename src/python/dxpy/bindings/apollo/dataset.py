from dxpy import DXHTTPRequest


class Dataset:
    def __init__(self, record_id, project_id):
        self.record_id = record_id
        self.project_id = project_id
        self.visualize_info = None
        self.descriptor = None

    def get_visualize_info(self):
        if self.visualize_info is None:
            self.visualize_info = DXHTTPRequest(
                "/" + self.record_id + "/visualize",
                {"project": self.project_id, "cohortBrowser": False},
            )
        return self.visualize_info

    def __getattr__(self, key_name):
        if key_name in self.get_visualize_info():
            return self.get_visualize_info().get(key_name)

    @property
    def cohort_flag(self):
        return (
            True
            if "CohortBrowser" in self.get_visualize_info().get("recordTypes")
            else False
        )

    def populate_descriptor(self, descriptor):
        # for key_name, value in vars(descriptor).items():
        #     self.key_name = value
        self.descriptor = vars(descriptor)

    def list_assays(self, assay_type):
        selected_type_assays = []
        for a in self.descriptor.get("assays"):
            if a["generalized_assay_model"] == assay_type:
                selected_type_assays.append(a)
        return selected_type_assays

    def list_assay_names(self, assay_type):
        list_assay = self.list_assays(assay_type)
        list_assay_names = []
        for a in list_assay:
            list_assay_names.append(a.get("name"))
        return list_assay_names
