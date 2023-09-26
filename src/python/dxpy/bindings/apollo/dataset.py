from dxpy import DXHTTPRequest
from dxpy.bindings import DXRecord


class Dataset(DXRecord):
    def __init__(self, dataset_id):
        super(Dataset, self).__init__(dataset_id)
        self.dataset_id = dataset_id
        self._detail_describe = None
        self._visualize_info = None
        self._dx_record_obj = None
        self.dx_dataset_descriptor = None

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

    @property
    def visualize_info(self):
        if self._visualize_info is None:
            self._visualize_info = DXHTTPRequest(
                "/" + self.dataset_id + "/visualize",
                {"project": self.project_id, "cohortBrowser": False},
            )
        return self._visualize_info

    @property
    def vizserver_url(self):
        vis_info = self.visualize_info
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

    @property
    def assays_info_dict(self):
        assays = self.dx_dataset_descriptor.get("assays")
        assays_info_dict = {}

        for index in range(len(assays)):
            model = assays[index]["generalized_assay_model"]
            assay_dict = {
                "name": assays[index]["name"],
                "index": index,
                "uuid": assays[index]["uuid"],
                "referece": assays[index]["reference"]["name"],
            }

            if model not in assays_info_dict.keys():
                assays_info_dict[model] = []

            assays_info_dict[model].append(assay_dict)

        return assays_info_dict

    def assay_names_list(self, assay_type):
        assay_names_list = []
        for assay in self.assays_info_dict[assay_type]:
            assay_names_list.append(assay["name"])
        return assay_names_list

    def is_assay_name_valid(self, assay_name, assay_type):
        return True if assay_name in self.assay_names_list(assay_type) else False

    def assay_index(self, assay_name):
        assay_lists_per_model = self.assays_info_dict.values()
        for model_assays in assay_lists_per_model:
            for assay in model_assays:
                if assay["name"] == assay_name:
                    return assay["index"]
