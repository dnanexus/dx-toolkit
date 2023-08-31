# TODO: check number of arguments and type manually
class ExpressionInputsValidator:
    """InputsValidator class for extract_assay expresion. Checks for invalid input combinations"""

    conditions_funcs = ["exclusive", "exclusive_with_exceptions"]

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

    def get_arguments_list(self):
        # Do I leave this here or in dataset_utilities? I guess this is not exclusive from expression but I'd leave it mutable
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

    def validate(self):
        self.get_arguments_list()
        # TODO: deal with this
        self.schema_version = self.schema.get("schema_version")

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
            self.error_handler("at least one of the schemas condition is not defined")

        # Calling method according to condition
        for key, value in self.schema.items():
            if key != "schema_version":
                method_to_call = value.get("condition")
                getattr(self, method_to_call)(key)

    def exclusive(self, check):
        self.exclusive_with_exceptions(check, no_exception=True)

    def exclusive_with_exceptions(self, check, no_exception=False):
        # Defining if exceptions exist
        if no_exception:
            exception_list = []
        else:
            exception_list = self.schema[check]["items"]["exceptions"]

        # Defining args to check and its values
        args_to_check = self.arguments_list.copy()

        args_to_check.remove(self.schema[check].get("items").get("main_key"))

        for e in exception_list:
            args_to_check.remove(e)
        args_to_check_values = [self.parser_dict[arg] for arg in args_to_check]

        # True check
        if self.parser_dict[self.schema[check].get("items").get("main_key")] and any(
            args_to_check_values
        ):
            # TODO configurate warning when set
            self.error_handler(self.schema[check].get("error_message").get("message"))

    # def __init__(self, args, error_handler=print):
    #     self.args = args
    #     self.error_handler = error_handler

    # def validate(self):
    #     # Checking exclusive arguments
    #     exclusive_arguments_definition = {
    #         "list_assays": {"exclusive_arg": "list_assays", "exceptions": []},
    #         "additional_fields_help": {
    #             "exclusive_arg": "additional_fields_help",
    #             "exceptions": ["retrieve_expression"],
    #         },
    #         "json_help": {
    #             "exclusive_arg": "json_help",
    #             "exceptions": ["retrieve_expression"],
    #         },
    #     }
    #     self.exclusive_argument_check(exclusive_arguments_definition)

    #     # Checking multiple exclusive groups
    #     mutually_exclusive_groups_definition = {
    #         "json_inputs": {
    #             "arguments": ["input_json", "input_json_file"],
    #             "required": False,
    #         },
    #     }
    #     self.mutually_exclusive_groups_check(mutually_exclusive_groups_definition)

    #     # Checking invalid combinations
    #     invalid_combinations_definition = {
    #         "no_args": {
    #             "invalid_combination": self.args["command"] == "extract_assay"
    #             and not any(
    #                 [
    #                     self.args["list_assays"],
    #                     self.args["retrieve_expression"],
    #                     self.args["additional_fields_help"],
    #                     self.args["json_help"],
    #                 ]
    #             ),
    #             "error_message": 'One of the arguments "--retrieve-expression", "--list-assays", "--additional-fields-help", "--json-help" is required.',
    #         },
    #         "retrieve_expression": {
    #             "invalid_combination": self.args["retrieve_expression"]
    #             and not any(
    #                 [
    #                     self.args["input_json"],
    #                     self.args["json_help"],
    #                     self.args["additional_fields_help"],
    #                 ]
    #             ),
    #             "error_message": 'The flag "--retrieve_expression" must be followed by "--input-json", "--json-help", "--json-help", or "--additional-fields-help".',
    #         },
    #         "expression_matrix_out_of_context": {
    #             "invalid_combination": self.args["expression_matrix"] and not self.args["retrieve_expression"],
    #             "error_message": '“--expression-matrix" cannot be passed with any argument other than "--retrieve-expression”',
    #         },
    #         "empty_json": {
    #             "invalid_combination": self.args["input_json"] == "{}",
    #             "error_message": 'JSON for "--retrieve-expression" does not contain valid filter information.',
    #         },
    #     }
    #     self.invalid_combinations_check(invalid_combinations_definition)

    # def exclusive_argument_check(self, exclusive_arguments_definition):
    #     for key, value in exclusive_arguments_definition.items():
    #         args_to_check = [
    #             "list_assays",
    #             "retrieve_expression",
    #             "assay_name",
    #             "input_json",
    #             "input_json_file",
    #             "json_help",
    #             "sql",
    #             "additional_fields",
    #             "additional_fields_help",
    #             "expression_matrix",
    #             "delim",
    #             "output",
    #         ]
    #         exception_str = []

    #         for exception in value.get("exceptions"):
    #             args_to_check.remove(exception)
    #             exception_str.append("--{}".format(exception).replace("_", "-"))

    #         exception_str = (
    #             str(exception_str).replace("[", "").replace("]", "").replace("'", '"')
    #         )
    #         args_to_check_values = [
    #             self.args[argument]
    #             for argument in args_to_check
    #             if argument != value.get("exclusive_arg")
    #         ]
    #         invalid_combination = self.args[
    #             value.get("exclusive_arg")
    #         ] and any(args_to_check_values)

    #         if invalid_combination:
    #             if value.get("exceptions") == []:
    #                 self.error_handler(
    #                     '"--{}" cannot be passed with other options.'.format(
    #                         value.get("exclusive_arg").replace("_", "-")
    #                     )
    #                 )
    #             else:
    #                 self.error_handler(
    #                     '"--{}" cannot be passed with any option other than {}.'.format(
    #                         value.get("exclusive_arg").replace("_", "-"), (exception_str))
    #                     )

    # def invalid_combinations_check(self, invalid_combinations_definition):
    #     for key, value in invalid_combinations_definition.items():
    #         if value.get("invalid_combination"):
    #             self.error_handler(value.get("error_message"))

    # def mutually_exclusive_groups_check(self, mutually_exclusive_groups_definition):
    #     for key in mutually_exclusive_groups_definition.keys():
    #         present_args = 0
    #         arguments_str = []

    #         # for arg in mutually_exclusive_groups_definition[key]["arguments"]:
    #         for arg in mutually_exclusive_groups_definition[key]["arguments"]:
    #             if self.args[arg] is not None and self.args[arg] is not False:
    #                 present_args += 1
    #             arguments_str.append("--{}".format(arg).replace("_", "-"))

    #         arguments_str = (
    #             str(arguments_str).replace("[", "").replace("]", "").replace("'", '"')
    #         )

    #         if present_args > 1:
    #             self.error_handler(
    #                 "The arguments {} are not allowed together.".format(arguments_str)
    #             )

    #         if (
    #             present_args < 1
    #             and mutually_exclusive_groups_definition[key]["required"]
    #         ):
    #             self.error_handler(
    #                 "Missing one of the following required arguments: {}.".format(
    #                     arguments_str
    #                 )
    #             )


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
