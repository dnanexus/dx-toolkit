import sys
import gzip
from dxpy import DXHTTPRequest
from dxpy.bindings import DXRecord, DXFile
import json


class Dataset(DXRecord):
    def __init__(self, dataset_id, detail_describe_dict=None):
        super(DXRecord, self).__init__(dataset_id)
        self.dataset_id = dataset_id
        self._detail_describe = detail_describe_dict
        self._visualize_info = None

        if detail_describe_dict:
            if "details" not in detail_describe_dict:
                raise ValueError("detail is expected key in detail_describe_dict")

    @staticmethod
    def resolve_cohort_to_dataset(record_obj):
        record_obj_desc = record_obj.describe(
            default_fields=True, fields={"properties", "details"}
        )
        cohort_info = None
        is_cohort = "CohortBrowser" in record_obj_desc["types"]

        if is_cohort:
            cohort_info = record_obj_desc
            dataset_id = record_obj_desc["details"]["dataset"]["$dnanexus_link"]
            dataset_obj = Dataset(dataset_id)
        else:
            dataset_obj = Dataset(record_obj.id, record_obj_desc)

        return dataset_obj, cohort_info

    @property
    def descriptor_file(self):
        return self.detail_describe["details"]["descriptor"]["$dnanexus_link"]

    @property
    def descriptor_file_dict(self):
        is_python2 = sys.version_info.major == 2
        content = DXFile(
            self.descriptor_file, mode="rb", project=self.project_id
        ).read()

        if is_python2:
            import StringIO

            x = StringIO.StringIO(content)
            file_obj = gzip.GzipFile(fileobj=x)
            content = file_obj.read()

        else:
            content = gzip.decompress(content)

        return json.loads(content)

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
    def version(self):
        return self.detail_describe.get("details").get("version")
    
    @property
    def detail_describe(self):
        if self._detail_describe is None:
            self._detail_describe = self.describe(
                default_fields=True, fields={"properties", "details"}
            )
        return self._detail_describe

    @property
    def assays_info_dict(self):
        assays = self.descriptor_file_dict["assays"]
        assays_info_dict = {}

        for index in range(len(assays)):
            model = assays[index]["generalized_assay_model"]
            assay_dict = {
                "name": assays[index]["name"],
                "index": index,
                "uuid": assays[index]["uuid"],
                "reference": assays[index].get("reference"),
            }

            if model not in assays_info_dict.keys():
                assays_info_dict[model] = []

            assays_info_dict[model].append(assay_dict)

        return assays_info_dict

    def assay_names_list(self, assay_type):
        assay_names_list = []
        if self.assays_info_dict.get(assay_type):
            for assay in self.assays_info_dict.get(assay_type):
                assay_names_list.append(assay["name"])
        return assay_names_list

    def is_assay_name_valid(self, assay_name, assay_type):
        return assay_name in self.assay_names_list(assay_type)

    def assay_index(self, assay_name):
        assay_lists_per_model = self.assays_info_dict.values()
        for model_assays in assay_lists_per_model:
            for assay in model_assays:
                if assay["name"] == assay_name:
                    return assay["index"]
