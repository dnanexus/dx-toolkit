# TODO: maybe check number of arguments and type manually

from __future__ import print_function
from dxpy import DXHTTPRequest
from dxpy.exceptions import (
    PermissionDenied,
    InvalidState,
    InvalidInput,
    ResourceNotFound,
)


class InputsValidator:
    """
    InputsValidator class for extract_assay expresion. Checks for invalid input combinations set by a JSON schema.

    The schema is a dictionary with the following structure:

    {
    "schema_version": "1.0",
    "parser_args":[
                  "path"
                  "...",
                  ]
    "condition_1": {
        "properties": {
            "items": ["path", "json_help"],
        },
        "condition": "at_least_one_required",
        "error_message": {
            "message": "..."
        },
    },
    "condition_2": {
        "properties": {
            "main_key": "path",
            "items": ["output","delim"]
        },
        "condition": "with_at_least_one_required",
        "error_message": {
            "message": "...",
            "type": "warning"
            },

    """

    def __init__(
        self,
        parser_dict,
        schema,
        error_handler=print,
        warning_handler=print,
    ):
        self.parser_dict = parser_dict
        self.schema = schema
        self.error_handler = error_handler
        self.warning_handler = warning_handler

        self.schema_version = schema.get("schema_version")
        self.conditions_funcs = {
            "exclusive": "interpret_exclusive",
            "exclusive_with_exceptions": "interpret_exclusive_with_exceptions",
            "required": "interpret_required",
            "at_least_one_required": "interpret_at_least_one_required",
            "with_at_least_one_required": "interpret_with_at_least_one_required",
            "with_none_of": "interpret_with_none_of",
            "mutually_exclusive_group": "interpret_mutually_exclusive_group",
        }

    ### Schema methods ###
    def validate_schema_conditions(self):
        # Checking if all conditions exist
        present_conditions = [
            value.get("condition")
            for key, value in self.schema.items()
            if key != "schema_version" and key != "parser_args"
        ]
        not_found = set(present_conditions) - set(self.conditions_funcs)
        if len(not_found) != 0:
            self.error_handler("{} schema condition is not defined".format(not_found))

    ### Checking general methods ###
    def interpret_conditions(self):
        for key, value in self.schema.items():
            if key != "schema_version" and key != "parser_args":
                condition = value.get("condition")
                method_to_call = self.conditions_funcs.get(condition)
                getattr(self, method_to_call)(key)

    def throw_message(self, check):
        type = self.schema.get(check).get("error_message").get("type")
        if type == "warning":
            self.throw_warning(check)
        elif (type == None) or (type == "error"):
            self.throw_exit_error(check)
        else:
            self.error_handler(
                'Unkown error message in schema: "{}" for key "{}"'.format(type, check)
            )

    def throw_exit_error(self, check):
        self.error_handler(self.schema.get(check).get("error_message").get("message"))

    def throw_warning(self, check):
        self.warning_handler(self.schema.get(check).get("error_message").get("message"))

    def get_parser_values(self, params):
        values = [self.parser_dict.get(p) for p in params]
        return values

    def get_main_key(self, check):
        return self.schema.get(check).get("properties").get("main_key")

    def get_items(self, check):
        return self.schema.get(check).get("properties").get("items")

    def get_items_values(self, check):
        items = self.get_items(check)
        return [self.parser_dict[i] for i in items]

    def get_exceptions(self, check):
        return self.schema.get(check).get("properties").get("exceptions")

    def remove_exceptions_from_list(self, check, list):
        exceptions_list = self.get_exceptions(check)
        for e in exceptions_list:
            list.remove(e)
        return list

    ### Checking specific methods ###
    def interpret_exclusive(self, check):
        self.interpret_exclusive_with_exceptions(check, exception_present=False)

    def interpret_exclusive_with_exceptions(self, check, exception_present=True):
        main_key = self.get_main_key(check)

        # Defining args to check and its values
        args_to_check = self.schema.get("parser_args")[:]
        args_to_check.remove(main_key)
        if exception_present:
            args_to_check = self.remove_exceptions_from_list(check, args_to_check)
        args_to_check_values = self.get_parser_values(args_to_check)

        # True check
        if self.parser_dict.get(main_key) and any(args_to_check_values):
            self.throw_message(check)

    def interpret_with_none_of(self, check):
        main_key = self.get_main_key(check)
        args_to_check_values = self.get_items_values(check)
        if self.parser_dict.get(main_key) and any(args_to_check_values):
            self.throw_message(check)

    def interpret_required(self, check):
        self.interpret_at_least_one_required(check)

    def interpret_at_least_one_required(self, check, main_key=None):
        args_to_check_values = self.get_items_values(check)

        if main_key:
            if self.parser_dict.get(main_key) and not any(args_to_check_values):
                self.throw_message(check)
        else:
            if not any(args_to_check_values):
                self.throw_message(check)

    def interpret_with_at_least_one_required(self, check):
        main_key = self.get_main_key(check)
        self.interpret_at_least_one_required(check, main_key)

    def interpret_mutually_exclusive_group(self, check):
        args_to_check_values = self.get_items_values(check)
        present_args_count = 0

        for arg in args_to_check_values:
            if arg is not None and not self.parser_dict.get(arg):
                present_args_count += 1

        if present_args_count > 1:
            self.throw_message(check)

    # VALIDATION
    def validate_input_combination(self):
        self.validate_schema_conditions()
        self.interpret_conditions()


class PathValidator:
    def __init__(self, parser_dict, project, entity_result, error_handler=print):
        # is it ok to leave err_exit as default? should I do the same for inputvalidator?
        self.parser_dict = parser_dict
        self.project = project
        self.entity_result = entity_result
        self.error_handler = error_handler

        self.record_http_request_info = None

    def throw_error(self, message):
        self.error_handler(message)

    def try_populate_record_http_request_info(self):
        try:
            self.record_http_request_info = DXHTTPRequest(
                "/" + self.entity_result["id"] + "/visualize",
                {"project": self.project, "cohortBrowser": False},
            )
        except (InvalidInput, InvalidState):
            self.throw_error(
                "Invalid cohort or dataset: {}".format(self.entity_result["id"]),
            )
        except Exception as details:
            self.throw_error(str(details))

    def resolve_project(self):
        # object in a different project
        if self.project != self.entity_result["describe"]["project"]:
            self.throw_error(
                'Unable to resolve "{}" to a data object or folder name in {}. Please make sure your object is in your selected project.'.format(
                    self.parser_dict.get("path"), self.project
                )
            )

    def assure_cohort_or_dataset(self):
        # resolving non record/cohort type
        if self.entity_result is None:
            self.throw_error(
                "The path must point to a record type of cohort or dataset, not a folder."
            )

        if self.entity_result["describe"]["class"] != "record":
            self.throw_error(
                "Invalid path. The path must point to a record type of cohort or dataset and not a {} object.".format(
                    self.entity_result["describe"]["class"]
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

    def assure_dataset_version(self):
        # checking cohort/dataset version
        dataset_version = float(self.record_http_request_info["datasetVersion"])
        if dataset_version < 3.0:
            self.throw_error(
                "Invalid version of cohort or dataset. Version must be 3.0 and not {}.".format(
                    dataset_version
                )
            )

    def cohort_list_assays_invalid_combination(self):
        invalid_combination = "CohortBrowser" in self.record_http_request_info[
            "recordTypes"
        ] and (
            self.parser_dict.get("list_assays") or self.parser_dict.get("assay_name")
        )

        if invalid_combination:
            self.throw_error(
                'Currently "--assay-name" and "--list-assays" may not be used with a CohortBrowser record (Cohort Object) as input. To select a specific assay or to list assays, please use a Dataset Object as input.'
            )
