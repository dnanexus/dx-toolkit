# TODO: check number of arguments and type manually
class ExpressionInputsValidator:
    """InputsValidator class for extract_assay expresion. Checks for invalid input combinations"""

    def __init__(self, args, error_handler=print):
        self.args = args
        self.error_handler = error_handler

    def validate(self):
        # Checking exclusive arguments
        # TODO pass as dict later on
        self.exclusive_argument_check("list_assays", [])
        self.exclusive_argument_check("additional_fields_help", ["retrieve_expression"])
        self.exclusive_argument_check("json_help", ["retrieve_expression"])
        self.exclusive_argument_check(
            "expression_matrix",
            ["retrieve_expression", "input_json", "input_json_file"],
        )

        # Checking multiple exclusive groups
        mutually_exclusive_groups_definition = {
            "json_inputs": {
                "arguments": ["input_json", "input_json_file"],
                "required": False,
            },
        }
        self.mutually_exclusive_groups_check(mutually_exclusive_groups_definition)

        # Checking invalid combinations
        invalid_combinations_definition = {
            "no_args": {
                "invalid_combination": self.args["command"] == "extract_assay"
                and not any(
                    [
                        self.args["list_assays"],
                        self.args["retrieve_expression"],
                        self.args["additional_fields_help"],
                        self.args["json_help"],
                    ]
                ),
                "error_message": 'One of the arguments "--retrieve-expression", "--list_assays", "--additional-fields-help", "--json-help" is required.',
            },
            "retrieve_expression": {
                "invalid_combination": self.args["retrieve_expression"]
                and not any(
                    [
                        self.args["input_json"],
                        self.args["json_help"],
                        self.args["additional_fields_help"],
                    ]
                ),
                "error_message": 'The flag "--retrieve_expression" must be followed by "--input-json", "--json-help", "--json-help", or "--additional-fields-help".',
            },
            "empty_json": {
                "invalid_combination": self.args["input_json"] == "{}",
                "error_message": 'JSON for "--retrieve-expression" does not contain valid filter information.',
            },
        }
        self.invalid_combinations_check(invalid_combinations_definition)

    def exclusive_argument_check(self, exclusive_arg, exceptions):
        args_to_check = [
            "list_assays",
            "retrieve_expression",
            "assay_name",
            "input_json",
            "input_json_file",
            "json_help",
            "sql",
            "additional_fields",
            "additional_fields_help",
            "expression_matrix",
            "delim",
            "output",
        ]
        exception_str = []
        for e in exceptions:
            args_to_check.remove(e)
            exception_str.append("--{}".format(e).replace("_", "-"))
        exception_str = (
            str(exception_str).replace("[", "").replace("]", "").replace("'", '"')
        )
        args_specific_list = [self.args[e] for e in args_to_check if e != exclusive_arg]
        invalid_combination = self.args[exclusive_arg] and any(args_specific_list)

        if invalid_combination:
            if exceptions == []:
                self.error_handler(
                    '"--{}" cannot be presented with other options.'.format(
                        exclusive_arg.replace("_", "-")
                    )
                )
            else:
                self.error_handler(
                    '"--{}" cannot be passed with any option other than {}'.format(
                        (exclusive_arg.replace("_", "-")), (exception_str)
                    )
                )

    def invalid_combinations_check(self, invalid_combinations_definition):
        for key, value in invalid_combinations_definition.items():
            if value.get("invalid_combination"):
                self.error_handler(value.get("error_message"))

    def mutually_exclusive_groups_check(self, mutually_exclusive_groups_definition):
        for key in mutually_exclusive_groups_definition.keys():
            present_args = 0
            arguments_str = []

            # for arg in mutually_exclusive_groups_definition[key]["arguments"]:
            for arg in mutually_exclusive_groups_definition[key]["arguments"]:
                if self.args[arg] is not None and self.args[arg] is not False:
                    present_args += 1
                arguments_str.append("--{}".format(arg).replace("_", "-"))

            arguments_str = (
                str(arguments_str).replace("[", "").replace("]", "").replace("'", '"')
            )

            if present_args > 1:
                self.error_handler(
                    "The arguments {} are not allowed together.".format(arguments_str)
                )

            if (
                present_args < 1
                and mutually_exclusive_groups_definition[key]["required"]
            ):
                self.error_handler(
                    "Missing one of the following required arguments: {}.".format(
                        arguments_str
                    )
                )


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
