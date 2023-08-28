class ExpressionInputsValidator:
    """InputsValidator class for extract_assay expresion. Checks for invalid input combinations"""

    def __init__(self, args, error_handler=print):
        self.args = args
        self.error_handler = error_handler

    def validate(self):
        self.end_arguments_check("list_assays")
        self.end_arguments_check("additional_fields_help")

        invalid_combination_infos = {
            "json_help_unique": {
                "invalid_combination": self.args["json_help"]
                and any(
                    [
                        self.args["assay_name"],
                        self.args["sql"],
                        self.args["additional_fields"],
                        self.args["expression_matrix"],
                        self.args["output"],
                    ]
                ),
                "error_message": '"--json-help" cannot be passed with any of "--assay-name", "--sql", "--additional-fields", "--expression-matrix", or "--output"',
            },
            "retrieve_expression_json": {
                "invalid_combination": self.args["retrieve_expression"] and not any(
            [self.args["input_json"], self.args["json_help"]]),
                "error_message": 'The flag "--retrieve_expression" must be followed by a json input or json help.',
            },
            "empty_json": {
                "invalid_combination": self.args["input_json"] == "{}",
                "error_message": 'JSON for "--retrieve-expression" does not contain valid filter information.',
            },
        }
        self.invalid_combinations_check(invalid_combination_infos)

    def end_arguments_check(self, end_arg):
        args_to_check = [
            "list_assays",
            "retrieve_expression",
            "assay_name",
            "input_json",
            "input_json_file",
            "json_help",
            "sql",
            "additional_fields",
            "expression_matrix",
            "delim",
            "output",
        ]
        args_specific_list = [self.args[e] for e in args_to_check if e != end_arg]
        invalid_combination = self.args[end_arg] and any(args_specific_list)

        if invalid_combination:
            self.error_handler(
                '"--{}" cannot be presented with other options.'.format(
                    end_arg.replace("_", "-")
                )
            )

    def invalid_combinations_check(self, invalid_combinations_info):
        for e in invalid_combinations_info:
            if invalid_combinations_info[e]["invalid_combination"]:
                self.error_handler(invalid_combinations_info[e]["error_message"])


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
