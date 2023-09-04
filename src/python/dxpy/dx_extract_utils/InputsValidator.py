# TODO: check number of arguments and type manually
# TODO: maybe remove exclusive condition
# TODO: schema versioning handling?

from __future__ import print_function


class InputsValidator:
    """
    InputsValidator class for extract_assay expresion. Checks for invalid input combinations set by a JSON schema.

    The schema is a dictionary with the following structure:

    {
    "schema_version": "1.0",
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
        built_in_args=None,
    ):
        self.parser_dict = parser_dict
        self.schema = schema
        self.error_handler = error_handler
        self.warning_handler = warning_handler
        self.built_in_args = built_in_args

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
    def populate_schema_version(self):
        self.schema_version = self.schema.get("schema_version")

    def validate_schema_conditions(self):
        # Checking if all conditions exist
        present_conditions = [
            value.get("condition")
            for key, value in self.schema.items()
            if key != "schema_version"
        ]
        not_found = set(present_conditions) - set(self.conditions_funcs)
        if len(not_found) != 0:
            self.error_handler("{} schema condition is not defined".format(not_found))

    # TODO: maybe remove the whole function
    def populate_arguments_list(self):
        if self.built_in_args == None:
            self.built_in_args = [
                "apiserver_host",
                "apiserver_port",
                "apiserver_protocol",
                "project_context_id",
                "workspace_id",
                "security_context",
                "auth_token",
                "env_help",
                "version",
                "command",
                "func",
            ]
        parser_dict_keys = self.parser_dict.keys()

        self.arguments_list = list(set(parser_dict_keys) - set(self.built_in_args))

    ### Checking general methods ###
    def interpret_conditions(self):
        for key, value in self.schema.items():
            if key != "schema_version":
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
        args_to_check = self.arguments_list.copy()
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
    def validate(self):
        # TODO only necessary if exclusive methods are used (maybe remove later)
        self.populate_arguments_list()
        # we're not really using this but I guess we can keep it here?
        self.populate_schema_version()
        self.validate_schema_conditions()

        self.interpret_conditions()


# class PathValidator:
#     def __init__(self, path) -> None:
#         self.path = path
#         self.project = None
#         self.folder_path = None
#         self.entity_result = None
#         self.http_request = None
#         self.dataset_project = None
#         self.which_json = None

#         self.variable_assigning()
#         self.resolve_project()
#         self.assure_record_type()
#         self.resolve_permission()
#         self.assure_dataset_version()
#         self.resolve_dataset_project()

#     def variable_assigning(self):
#         # Assigning platform information
#         self.project, self.folder_path, self.entity_result = resolve_existing_path(
#             self.path
#         )
#         # print(f"{self.project}, pths {self.folder_path}, ent {self.entity_result}")

#     def resolve_project(self):
#         # resolving project issues
#         if self.project is None:
#             raise ResolutionError(
#                 'Unable to resolve "'
#                 + self.path
#                 + '" to a data object or folder name in a project'
#             )
#         elif self.project != self.entity_result["describe"]["project"]:
#             raise ResolutionError(
#                 'Unable to resolve "'
#                 + self.path
#                 + "\" to a data object or folder name in '"
#                 + self.project
#                 + "'"
#             )

#     def assure_record_type(self):
#         # resolving non record/cohort and permission issues
#         if self.entity_result["describe"]["class"] != "record":
#             err_exit(
#                 "%s : Invalid path. The path must point to a record type of cohort or dataset"
#                 % self.entity_result["describe"]["class"]
#             )

#     def resolve_permission(self):
#         try:
#             self.http_request = dxpy.DXHTTPRequest(
#                 "/" + self.entity_result["id"] + "/visualize",
#                 {"project": self.project, "cohortBrowser": False},
#             )
#         except PermissionDenied:
#             err_exit(
#                 "Insufficient permissions", expected_exceptions=(PermissionDenied,)
#             )
#         except (InvalidInput, InvalidState):
#             err_exit(
#                 "%s : Invalid cohort or dataset" % self.entity_result["id"],
#                 expected_exceptions=(
#                     InvalidInput,
#                     InvalidState,
#                 ),
#             )
#         except Exception as details:
#             err_exit(str(details))

#     def assure_dataset_version(self):
#         # checking cohort/dataset version
#         if self.http_request["datasetVersion"] != "3.0":
#             err_exit(
#                 "%s : Invalid version of cohort or dataset. Version must be 3.0"
#                 % self.http_request["datasetVersion"]
#             )

#     def resolve_dataset_project(self):
#         # Defining dataset project
#         if ("Dataset" in self.http_request["recordTypes"]) or (
#             "CohortBrowser" in self.http_request["recordTypes"]
#         ):
#             self.dataset_project = self.http_request["datasetRecordProject"]
#         else:
#             err_exit(
#                 "%s : Invalid path. The path must point to a record type of cohort or dataset"
#                 % self.http_request["recordTypes"]
#             )

#     def get_http_request_info(self):
#         return self.http_request
