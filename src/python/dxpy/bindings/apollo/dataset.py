from dxpy import DXHTTPRequest
from dxpy.bindings import DXRecord


class Dataset(DXRecord):
    def __init__(self, record_id, project_id):
        self.record_id = record_id
        self.project_id = project_id
        self.visualize_info = None
        self.descriptor = None
        self.assays_list = None
        self.assay_names_list = None

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

    def get_assay_list(self, assay_type):
        if self.assays_list is None:
            selected_type_assays = []
            for a in self.descriptor.get("assays"):
                if a["generalized_assay_model"] == assay_type:
                    selected_type_assays.append(a)
            self.assays_list = selected_type_assays
        return self.assays_list

    def get_assay_names(self, assay_type):
        if self.assay_names_list is None:
            list_assay = self.get_assay_list(assay_type)
            list_assay_names = []
            for a in list_assay:
                list_assay_names.append(a.get("name"))
            self.assay_names_list = list_assay_names
        return self.assay_names_list

    def get_assay_indice_in_list(self, assay_names_list, assay_name):
        index = self.assay_names_list.index(assay_name)
        return index

    def get_assay_reference(self, assay_index):
        reference = (
            self.descriptor.get("assays")[assay_index].get("reference").get("name")
        )
        return reference
