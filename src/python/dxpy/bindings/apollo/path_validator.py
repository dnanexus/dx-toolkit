from __future__ import print_function
from dxpy import DXHTTPRequest
from dxpy.exceptions import (
    InvalidState,
    InvalidInput,
)


class PathValidator:
    """
    PathValidator class checks for invalid object inputs and its combination with passed arguments.
    """

    def __init__(self, input_dict, project, entity_describe, error_handler=print):
        self.input_dict = input_dict
        self.project = project
        self.entity_describe = entity_describe
        self.error_handler = error_handler

        self.record_http_request_info = None

    def throw_error(self, message):
        self.error_handler(message)

    def try_populate_record_http_request_info(self):
        # record_http_request_info contains crucial information for records
        try:
            self.record_http_request_info = DXHTTPRequest(
                "/" + self.entity_describe["id"] + "/visualize",
                {"project": self.project, "cohortBrowser": False},
            )
        except (InvalidInput, InvalidState):
            self.throw_error(
                "Invalid cohort or dataset: {}".format(self.entity_describe["id"]),
            )
        except Exception as details:
            self.throw_error(str(details))

    def is_object_in_current_project(self):
        # for object in a different project:
        if self.project != self.entity_describe["project"]:
            self.throw_error(
                'Unable to resolve "{}" to a data object or folder name in {}. Please make sure your object is in your selected project.'.format(
                    self.input_dict.get("path"), self.project
                )
            )

    def is_cohort_or_dataset(self):
        # resolving non record/cohort type
        if self.entity_describe["class"] != "record":
            self.throw_error(
                "Invalid path. The path must point to a record type of cohort or dataset and not a {} object.".format(
                    self.entity_describe["class"]
                )
            )

        # since object is record:
        self.try_populate_record_http_request_info()
        if not (
            ("Dataset" in self.record_http_request_info["recordTypes"])
            or ("CohortBrowser" in self.record_http_request_info["recordTypes"])
        ):
            self.throw_error(
                "Invalid path. The path must point to a record type of cohort or dataset and not a {} object."
            ).format(self.record_http_request_info["recordTypes"])

    def assert_dataset_version(self, expected_min_dataset_version=3.0):
        # checking cohort/dataset version
        dataset_version = float(self.record_http_request_info["datasetVersion"])
        if dataset_version < expected_min_dataset_version:
            self.throw_error(
                "{}: Version of the cohort or dataset is too old. Version must be at least {}.".format(
                    dataset_version, expected_min_dataset_version
                )
            )

    def cohort_list_assays_invalid_combination(self):
        invalid_combination = "CohortBrowser" in self.record_http_request_info[
            "recordTypes"
        ] and (self.input_dict.get("list_assays") or self.input_dict.get("assay_name"))

        if invalid_combination:
            self.throw_error(
                'Currently "--assay-name" and "--list-assays" may not be used with a CohortBrowser record (Cohort Object) as input. To select a specific assay or to list assays, please use a Dataset Object as input.'
            )

    def validate(
        self,
        expected_min_dataset_version=3.0,
        check_list_assays_invalid_combination=False,
    ):
        self.is_object_in_current_project()
        self.is_cohort_or_dataset()
        self.assert_dataset_version(expected_min_dataset_version)

        if check_list_assays_invalid_combination:
            self.cohort_list_assays_invalid_combination()