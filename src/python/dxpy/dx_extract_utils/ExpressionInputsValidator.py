from ..exceptions import (
    err_exit,
    PermissionDenied,
    InvalidInput,
    InvalidState,
    ResourceNotFound,
    default_expected_exceptions,
)

# import sys
# import collections
# import json
# import os
# import re
# import csv
# import dxpy
# import codecs
# import subprocess


class ExpressionInputsValidator:
    """InputsValidator class for extract_assay expresion"""

    def __init__(self, retrieve_expression, input_json) -> None:
        self.retrieve_expression_flag = retrieve_expression
        self.input_json_flag = input_json

    def validate_input_combination(self):
        invalid_combo_1 = self.retrieve_expression_flag and not self.input_json_flag
        if invalid_combo_1:
            err_exit(
                "The flag, --retrieve_expression must be followed by a json input."
            )

