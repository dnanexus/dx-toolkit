from dxpy import DXHTTPRequest
from dxpy.bindings import DXRecord


class Dataset(DXRecord):
    def __init__(self, record_id, project_id):
        self.record_id = record_id
        self.project_id = project_id
        self.visualize_info = None
        self._dx_record_obj = None
        self.dx_dataset_descriptor = None
        self.assays_list = None
        self.assay_names_list = None

    def get_visualize_info(self):
        if self.visualize_info is None:
            self.visualize_info = DXHTTPRequest(
                "/" + self.record_id + "/visualize",
                {"project": self.project_id, "cohortBrowser": False},
            )
        return self.visualize_info

    @property
    def vizserver_url(self):
        vis_info = self.get_visualize_info()
        return vis_info.get("url")

    def get_dx_record(self):
        if self._dx_record_obj is None:
            self._dx_record_obj = DXRecord(self.record_id)
        return self._dx_record_obj

    @property
    def dx_record_obj(self):
        return self.get_dx_record()

    @property
    def record_details(self):
        return self.dx_record_obj.get_details()

    @property
    def record_descriptor(self):
        return self.dx_record_obj.describe()

    @property
    def is_cohort(self):
        if "CohortBrowser" in self.record_descriptor.get("types"):
            return True
        else:
            return False

    @property
    def dataset_id(self):
        if self.is_cohort:
            return self.record_details.get("dataset").get("$dnanexus_link")
        else:
            return self.record_descriptor.get("id")

    @property
    def dataset_project_id(self):
        if self.is_cohort:
            dataset_dx_obj = DXRecord(self.dataset_id)
            return dataset_dx_obj.describe().get("project")
        else:
            return self.record_descriptor.get("project")

    @property
    def base_sql(self):
        return self.record_details.get("baseSql")

    @property
    def filters(self):
        return self.record_details.get("filters")

    def populate_dx_dataset_descriptor(self, descriptor):
        self.dx_dataset_descriptor = vars(descriptor)

    def get_assay_list(self, assay_type):
        if self.assays_list is None:
            selected_type_assays = []
            for a in self.dx_dataset_descriptor.get("assays"):
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

    def get_assay_uuid(self, assay_index):
        uuid = self.dx_dataset_descriptor.get("assays")[assay_index].get("uuid")
        return uuid

    def get_assay_name(self, assay_index):
        name = self.dx_dataset_descriptor.get("assays")[assay_index].get("name")
        return name

    def get_assay_reference(self, assay_index):
        reference = (
            self.dx_dataset_descriptor.get("assays")[assay_index]
            .get("reference")
            .get("name")
        )
        return reference

    def get_assay_generalized_assay_model(self, assay_index):
        generalized_assay_model = self.dx_dataset_descriptor.get("assays")[
            assay_index
        ].get("generalized_assay_model")
        return generalized_assay_model
