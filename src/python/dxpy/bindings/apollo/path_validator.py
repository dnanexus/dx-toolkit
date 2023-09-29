from __future__ import print_function


class PathValidator:
    """
    PathValidator class checks for invalid object inputs and its combination with passed arguments.

    entity_describe must be a dict from /describe call containing properties and details

    e.g. ```f.describe(default_fields=True, fields={"properties", "details"})``` where f is a DXRecord object
    """

    def __init__(self, input_dict, project, entity_describe, error_handler=print):
        self.input_dict = input_dict
        self.project = project
        self.entity_describe = entity_describe
        self.error_handler = error_handler

        assert "path" in input_dict
        assert "class" in entity_describe

    def throw_error(self, message):
        self.error_handler(message)

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

        EXPECTED_TYPES = ["Dataset", "CohortBrowser"]
        _record_types = self.get_record_types()

        if all(x not in _record_types for x in EXPECTED_TYPES):
            self.throw_error(
                "{} Invalid path. The path must point to a record type of cohort or dataset and not a {} object.".format(
                    self.entity_describe["id"],
                    _record_types
                )
            )

    def assert_dataset_version(self, expected_min_dataset_version=3.0):
        # checking cohort/dataset version
        dataset_version = float(self.entity_describe.get("details").get("version"))
        if dataset_version < expected_min_dataset_version:
            self.throw_error(
                "{}: Version of the cohort or dataset is too old. Version must be at least {}.".format(
                    dataset_version, expected_min_dataset_version
                )
            )

    def get_record_types(self):
        return self.entity_describe.get("types")

    def cohort_list_assays_invalid_combination(self):
        invalid_combination = "CohortBrowser" in self.get_record_types() and (
            self.input_dict.get("list_assays") or self.input_dict.get("assay_name")
        )

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
