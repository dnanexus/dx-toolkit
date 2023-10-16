# -*- coding: utf-8 -*-

EXTRACT_ASSAY_EXPRESSION_INPUT_ARGS_SCHEMA = {
    "schema_version": "1.0",
    "parser_args": [
        "path",
        "assay_name",
        "list_assays",
        "output",
        "retrieve_expression",
        "additional_fields",
        "additional_fields_help",
        "delim",
        "filter_json_file",
        "sql",
        "expression_matrix",
        "json_help",
        "filter_json",
    ],
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
    "3_list_assays_exclusive_with_exceptions": {
        "properties": {
            "main_key": "list_assays",
            "exceptions": ["path"],
        },
        "condition": "exclusive_with_exceptions",
        "error_message": {
            "message": '"--list-assays" cannot be presented with other options'
        },
    },
    "4_retrieve_expression_with_at_least_one_required": {
        "properties": {
            "main_key": "retrieve_expression",
            "items": [
                "filter_json",
                "filter_json_file",
                "json_help",
                "additional_fields_help",
            ],
        },
        "condition": "with_at_least_one_required",
        "error_message": {
            "message": 'The flag "--retrieve_expression" must be followed by "--filter-json", "--filter-json-file", "--json-help", or "--additional-fields-help".'
        },
    },
    "5_json_help_exclusive_with_exceptions": {
        "properties": {
            "main_key": "json_help",
            "exceptions": [
                "path",
                "retrieve_expression",
            ],
        },
        "condition": "exclusive_with_exceptions",
        "error_message": {
            "message": '"--json-help" cannot be passed with any option other than "--retrieve-expression".'
        },
    },
    "6_additional_fields_help_exclusive_with_exceptions": {
        "properties": {
            "main_key": "additional_fields_help",
            "exceptions": [
                "path",
                "retrieve_expression",
            ],
        },
        "condition": "exclusive_with_exceptions",
        "error_message": {
            "message": '"--additional-fields-help" cannot be passed with any option other than "--retrieve-expression".'
        },
    },
    "7_json_inputs-mutually_exclusive": {
        "properties": {
            "items": ["filter_json", "filter_json_file"],
        },
        "condition": "mutually_exclusive_group",
        "error_message": {
            "message": 'The arguments "--filter-json" and "--filter-json-file" are not allowed together.'
        },
    },
    "8_expression_matrix-with_at_least_one_required": {
        "properties": {
            "main_key": "expression_matrix",
            "items": ["retrieve_expression"],
        },
        "condition": "with_at_least_one_required",
        "error_message": {
            "message": '"--expression-matrix" cannot be passed with any argument other than "--retrieve-expression".'
        },
    },
    "9_em_sql-mutually_exclusive": {
        "properties": {
            "items": ["expression_matrix", "sql"],
        },
        "condition": "mutually_exclusive_group",
        "error_message": {
            "message": '"--expression-matrix"/"-em" cannot be passed with the flag, "--sql".'
        },
    },
}
