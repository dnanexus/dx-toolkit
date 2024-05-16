from __future__ import annotations
import sys
import csv
import os
import json
from ..exceptions import err_exit


def write_expression_output(
    arg_output,
    arg_delim,
    arg_sql,
    output_listdict_or_string,
    save_uncommon_delim_to_txt=True,
    output_file_name=None,
    error_handler=err_exit,
    colnames=None,
):
    """
    arg_output: str
    A string representing the output file path.
    When it's "-", output is written to stdout.
    This can be directly set as args.output from argparse when calling the function (e.g within dataset_utilities)

    arg_delim: str
    A string representing the delimiter. Defaults to "," when not specified.
    It also determines the file suffix when writing to file.
    This can be set to args.delimiter from argparse when the method is called

    arg_sql: bool
    A boolean representing whether the output_listdict_or_string is a SQL query (string) or not.
    This can be args.sql from argparse

    output_listdict_or_string: 'list of dicts' or 'str' depending on whether arg_sql is False or True, respectively
    This is expected to be the response from vizserver
    if arg_sql is True, this is expected to be a string representing the SQL query
    if arg_sql is False, this is expected to be a list of dicts representing the output of a SQL query
    if output_listdict_or_string is a list of dicts, all dicts must have the same keys which will be used as column names

    save_uncommon_delim_to_txt: bool
    Set this to False if you want to error out when any delimiter other than "," or "\t" is specified

    output_file_name: str
    This is expected to be a record_name which will be used when arg_output is not specified
    Do not append a suffix to this string
    output_file_name is mandatory when arg_output is not specified

    By default delimiter is set "," and file suffix is csv (when writing to file)

    None values are written as empty strings by default (csv.DictWriter behavior)

    """

    IS_OS_WINDOWS = os.name == "nt"
    OS_SPECIFIC_LINE_SEPARATOR = os.linesep
    IS_PYTHON_2 = sys.version_info.major == 2
    IS_PYTHON_3 = sys.version_info.major == 3

    if arg_sql:
        SUFFIX = ".sql"
        if not isinstance(output_listdict_or_string, str):
            error_handler("Expected SQL query to be a string")
    elif arg_delim:
        if arg_delim == ",":
            SUFFIX = ".csv"
        elif arg_delim == "\t":
            SUFFIX = ".tsv"
        else:
            if save_uncommon_delim_to_txt:
                SUFFIX = ".txt"
            else:
                error_handler("Unsupported delimiter: {}".format(arg_delim))
    else:
        SUFFIX = ".csv"

    if arg_output:
        if arg_output == "-":
            WRITE_METHOD = "STDOUT"
        else:
            WRITE_METHOD = "FILE"
            output_file_name = arg_output

    else:
        OUTPUT_DIR = os.getcwd()

        if output_file_name is None:
            error_handler(
                "No output filename specified"
            )  # Developer expected to provide record_name upstream when calling this function

        else:
            WRITE_METHOD = "FILE"
            output_file_name = os.path.join(OUTPUT_DIR, output_file_name + SUFFIX)

    if WRITE_METHOD == "FILE":
        # error out if file already exists or output_file_name is a directory
        if os.path.exists(output_file_name):
            if os.path.isfile(output_file_name):
                error_handler(
                    "{} already exists. Please specify a new file path".format(
                        output_file_name
                    )
                )
            if os.path.isdir(output_file_name):
                error_handler(
                    "{} is a directory. Please specify a new file path".format(
                        output_file_name
                    )
                )

    if arg_sql:
        if WRITE_METHOD == "STDOUT":
            print(output_listdict_or_string)
        elif WRITE_METHOD == "FILE":
            with open(output_file_name, "w") as f:
                f.write(output_listdict_or_string)
        else:
            error_handler("Unexpected error occurred while writing SQL query output")

    else:
        if colnames:
            COLUMN_NAMES = colnames
        else:
            COLUMN_NAMES = output_listdict_or_string[0].keys()

        if not all(
            set(i.keys()) == set(COLUMN_NAMES) for i in output_listdict_or_string
        ):
            error_handler("All rows must have the same column names")

        WRITE_MODE = "wb" if IS_PYTHON_2 or IS_OS_WINDOWS else "w"
        NEWLINE = "" if IS_PYTHON_3 else None
        DELIMITER = str(arg_delim) if arg_delim else ","
        QUOTING = csv.QUOTE_MINIMAL
        QUOTE_CHAR = '"'

        write_args = {
            "mode": WRITE_MODE,
        }

        if IS_PYTHON_3:
            write_args["newline"] = NEWLINE

        dictwriter_params = {
            "fieldnames": COLUMN_NAMES,
            "delimiter": DELIMITER,
            "lineterminator": OS_SPECIFIC_LINE_SEPARATOR,
            "quoting": QUOTING,
            "quotechar": QUOTE_CHAR,
        }

        if WRITE_METHOD == "FILE":
            with open(output_file_name, **write_args) as f:
                w = csv.DictWriter(f, **dictwriter_params)
                w.writeheader()
                w.writerows(output_listdict_or_string)

        elif WRITE_METHOD == "STDOUT":
            w = csv.DictWriter(sys.stdout, **dictwriter_params)
            w.writeheader()
            w.writerows(output_listdict_or_string)

        else:
            error_handler("Unexpected error occurred while writing output")



def pretty_print_json(json_dict: dict) -> str:
    """Pretty-prints the provided JSON object.

    Args:
        json_dict: A string containing valid JSON data.

    Returns:
        Returns a string with formatted JSON or None if there's an error.
    """
    if isinstance(json_dict, dict):
        formatted_json = json.dumps(json_dict, sort_keys=True, indent=4)
        return formatted_json
    else:
        print("WARNING: Invalid JSON provided.", file=sys.stderr)
        return None
