from dxpy import DXHTTPRequest
from dxpy.bindings import DXRecord


class Dataset(DXRecord):
    def __init__(self, dataset_id):
        super().__init__(dataset_id)
        self.dataset_id = dataset_id
        self._detail_describe = None
        # self.project_id = project_id
        self.visualize_info = None
        self._dx_record_obj = None
        self.dx_dataset_descriptor = None
        self.assays_list = None
        self.assay_names_list = None

    @staticmethod
    def cohort_object(record_id):
        return DXRecord(record_id)

    @staticmethod
    def cohort_object_describe(record_id):
        record_obj = Dataset.cohort_object(record_id)
        return record_obj.describe(
            default_fields=True, fields={"properties", "details"}
        )

    @staticmethod
    def cohort_object_information(record_id):
        cohort_object_describe = Dataset.cohort_object_describe(record_id)

        cohort_information = {}

        if "CohortBrowser" in cohort_object_describe.get("types"):
            cohort_information["is_cohort"] = True
            cohort_information["record_id"] = True
            cohort_information["is_cohort"] = True
            # reminder: not all cohorts have base sql
            cohort_information["base_sql"] = cohort_object_describe.get("details").get(
                "baseSql"
            )
            cohort_information["filters"] = cohort_object_describe.get("details").get(
                "filters"
            )
            cohort_information["dataset_id"] = (
                cohort_object_describe.get("details")
                .get("dataset")
                .get("$dnanexus_link")
            )

        else:
            cohort_information["is_cohort"] = False
            cohort_information["base_sql"] = None
            cohort_information["filters"] = None
            cohort_information["dataset_id"] = record_id

        return cohort_information

    def get_visualize_info(self):
        if self.visualize_info is None:
            self.visualize_info = DXHTTPRequest(
                "/" + self.dataset_id + "/visualize",
                {"project": self.project_id, "cohortBrowser": False},
            )
        return self.visualize_info

    @property
    def vizserver_url(self):
        vis_info = self.get_visualize_info()
        return vis_info.get("url")

    @property
    def project_id(self):
        return self.detail_describe.get("project")

    @property
    def detail_describe(self):
        if self._detail_describe is None:
            self._detail_describe = self.describe(
                default_fields=True, fields={"properties", "details"}
            )
        return self._detail_describe

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
