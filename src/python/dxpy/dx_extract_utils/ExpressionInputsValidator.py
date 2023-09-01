# TODO: check number of arguments and type manually
# TODO: warning error handler
# TODO: maybe remove exclusive condition
# TODO: schema versioning handling?

schema = {
    "schema_version": "1.0",
    "1_path_or_json_help-at_least_one_required": {
        "properties": {
            "items": ["path", "json_help"],
        },
        "condition": "at_least_one_required",
        "error_message": {
            "message": 'At least one of the following arguments is required: "Path", "--json-help"'
        },
    },
    "2_path_with_no_args-with_at_least_one_required": {
        "properties": {
            "main_key": "path",
            "items": [
                "list_assays",
                "retrieve_expression",
                "additional_fields_help",
                "json_help",
            ],
        },
        "condition": "with_at_least_one_required",
        "error_message": {
            "message": 'One of the arguments "--retrieve-expression", "--list-assays", "--additional-fields-help", "--json-help" is required.'
        },
    },
    # "3_list_assays_exclusive": {
    #     "properties": {
    #         "main_key": "list_assays",
    #         "exceptions": ["path"],
    #     },
    #     "condition": "exclusive_with_exceptions",
    #     "error_message": {
    #         "message": '"--list-assays" cannot be presented with other options'
    #     },
    # },
    "3_list_assays_with_none_of": {
        "properties": {
            "main_key": "list_assays",
            "items": [
                "assay_name",
                "output",
                "retrieve_expression",
                "additional_fields",
                "additional_fields_help",
                "delim",
                "input_json_file",
                "sql",
                "expression_matrix",
                "json_help",
                "input_json",
            ],
        },
        "condition": "with_none_of",
        "error_message": {
            "message": '"--list-assays" cannot be presented with other options'
        },
    },
    "4_retrieve_expression_with_at_least_one_required": {
        "properties": {
            "main_key": "retrieve_expression",
            "items": [
                "input_json",
                "input_json_file",
                "json_help",
                "additional_fields_help",
            ],
        },
        "condition": "with_at_least_one_required",
        "error_message": {
            "message": 'The flag "--retrieve_expression" must be followed by "--input-json", "--input-json-file", "--json-help", or "--additional-fields-help".'
        },
    },
    "5_json_help_with_none_of": {
        "properties": {
            "main_key": "json_help",
            "items": [
                "assay_name",
                "output",
                "list_assays",
                "additional_fields",
                "additional_fields_help",
                "delim",
                "input_json_file",
                "sql",
                "expression_matrix",
                "json_help",
                "input_json",
            ],
        },
        "condition": "with_none_of",
        "error_message": {
            "message": '"--json-help" cannot be passed with any option other than "--retrieve-expression".'
        },
    },
    "6_additional_fields_help": {
        "properties": {
            "main_key": "additional_fields_help",
            "exceptions": ["path", "retrieve_expression"],
        },
        "condition": "exclusive_with_exceptions",
        "error_message": {
            "message": '"--additional-fields-help" cannot be passed with any option other than "--retrieve-expression".'
        },
    },
    "7_json_inputs-mutually_exclusive": {
        "properties": {
            "items": ["input_json", "input_json_file"],
        },
        "condition": "mutually_exclusive_group",
        "error_message": {
            "message": 'The arguments "--input-json" and "--input-json-file" are not allowed together.'
        },
    },
    "8_expression_matrix-with_at_least_one_required": {
        "properties": {
            "main_key": "expression_matrix",
            "items": ["retrieve_expression"],
        },
        "condition": "with_at_least_one_required",
        "error_message": {
            "message": '“--expression-matrix" cannot be passed with any argument other than "--retrieve-expression”'
        },
    },
}


class ExpressionInputsValidator:
    """InputsValidator class for extract_assay expresion. Checks for invalid input combinations"""

    conditions_funcs = [
        "exclusive",
        "exclusive_with_exceptions",
        "required",
        "at_least_one_required",
        "with_at_least_one_required",
        "with_none_of",
        "mutually_exclusive_group",
    ]

    def __init__(
        self,
        parser_dict,
        schema,
        error_handler=print,
        built_in_args=None,
    ):
        self.parser_dict = parser_dict
        self.schema = schema
        self.error_handler = error_handler
        self.built_in_args = built_in_args

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
        not_found = set(present_conditions) - set(
            ExpressionInputsValidator.conditions_funcs
        )
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
                method_to_call = value.get("condition")
                getattr(self, method_to_call)(key)

    def throw_exit_error(self, check):
        self.error_handler(self.schema.get(check).get("error_message").get("message"))

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
    def exclusive(self, check):
        self.exclusive_with_exceptions(check, exception_present=False)

    def exclusive_with_exceptions(self, check, exception_present=True):
        main_key = self.get_main_key(check)

        # Defining args to check and its values
        args_to_check = self.arguments_list.copy()
        args_to_check.remove(main_key)
        if exception_present:
            args_to_check = self.remove_exceptions_from_list(check, args_to_check)
        args_to_check_values = self.get_parser_values(args_to_check)

        # True check
        if self.parser_dict.get(main_key) and any(args_to_check_values):
            self.throw_exit_error(check)

    def with_none_of(self, check):
        main_key = self.get_main_key(check)
        args_to_check_values = self.get_items_values(check)
        if self.parser_dict.get(main_key) and any(args_to_check_values):
            self.throw_exit_error(check)

    def required(self, check):
        self.at_least_one_required(check)

    def at_least_one_required(self, check, main_key=None):
        args_to_check_values = self.get_items_values(check)

        if main_key:
            if self.parser_dict.get(main_key) and not any(args_to_check_values):
                self.throw_exit_error(check)
        else:
            if not any(args_to_check_values):
                self.throw_exit_error(check)

    def with_at_least_one_required(self, check):
        main_key = self.get_main_key(check)
        self.at_least_one_required(check, main_key)

    def mutually_exclusive_group(self, check):
        args_to_check_values = self.get_items_values(check)
        present_args_count = 0

        for arg in args_to_check_values:
            if arg is not None and not self.parser_dict.get(arg):
                present_args_count += 1

        if present_args_count > 1:
            self.throw_exit_error(check)

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
