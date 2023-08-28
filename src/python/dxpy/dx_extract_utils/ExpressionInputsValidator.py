from ..exceptions import err_exit


class ExpressionInputsValidator:
    """InputsValidator class for extract_assay expresion. Checks for invalid input combinations"""

    def __init__(self, args, error_handler):
        self.list_assays = args.list_assays
        self.retrieve_expression_flag = args.retrieve_expression
        self.assay_name = args.assay_name
        self.input_json = args.input_json
        self.input_json_file = args.input_json_file
        self.json_help = args.json_help
        self.sql = args.sql
        self.additional_fields = args.additional_fields
        self.additional_fields_help = args.additional_fields_help
        self.expression_matrix = args.expression_matrix
        self.delim = args.delim
        self.output = args.output

        self.error_handler = error_handler

    def run_validations(self):
        self.end_arguments_check("list_assays")
        self.end_arguments_check("additional_fields_help")

        if self.json_help and any(
            [
                self.assay_name,
                self.sql,
                self.additional_fields,
                self.expression_matrix,
                self.output,
            ]
        ):
            self.error_handler = (
                '"--json-help" cannot be passed with any of "--assay-name", "--sql", "--additional-fields", "--expression-matrix", or "--output"',
            )

        if self.retrieve_expression_flag and not any([self.input_json, self.json_help]):
            self.error_handler = (
                'The flag "--retrieve_expression" must be followed by a json input or json help.',
            )

        if self.input_json == "{}":
            self.error_handler = (
                'JSON for "--retrieve-expression" does not contain valid filter information.',
            )

    def end_arguments_check(self, end_arg):
        args_specific_list = [
            getattr(self, arg)
            for arg in dir(self)
            if not callable(getattr(self, arg))
            and not arg.startswith("__")
            and arg != end_arg
        ]

        invalid_combination = getattr(self, end_arg) and any(args_specific_list)

        if invalid_combination:
            self.error_handler = (
                '"--{}" cannot be presented with other options.'.format(
                    end_arg.replace("_", "-")
                )
            )

    def error_exit(self):
        if self.error_handler:
            err_exit(self.error_handler[0])


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
