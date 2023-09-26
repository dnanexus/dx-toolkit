import sys
import csv
import os
from ..exceptions import err_exit


def write_expression_output(
    arg_output,
    arg_delim,
    arg_sql,
    output_listdict_or_string,
    save_uncommon_delim_to_txt=True,
    output_file_name=None,
):
    """
    arg_output: str
    This is is expected to be args.output from argparse

    arg_delim: str
    This is expected to be args.delimiter from argparse

    arg_sql: bool
    This is expected to be args.sql from argparse

    output_listdict_or_string: 'list of dicts' or 'str' depending on whether arg_sql is False or True, respectively
    This is expected to be the response from vizserver

    save_uncommon_delim_to_txt: bool
    Set this to False if you want to error out when any delimiter other than "," or "\t" is specified

    output_file_name: str
    This is expected to be a record_name which will be used when arg_output is not specified
    Do not append a suffix to this string

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
            err_exit("Expected SQL query to be a string")
    elif arg_delim:
        if arg_delim == ",":
            SUFFIX = ".csv"
        elif arg_delim == "\t":
            SUFFIX = ".tsv"
        else:
            if save_uncommon_delim_to_txt:
                SUFFIX = ".txt"
            else:
                err_exit("Unsupported delimiter: ".format(arg_delim))
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
            err_exit(
                "No output filename specified"
            )  # Developer expected to provide record_name upstream when calling this function

        else:
            WRITE_METHOD = "FILE"
            output_file_name = os.path.join(OUTPUT_DIR, output_file_name + SUFFIX)

    if WRITE_METHOD == "FILE":
        # error out if file already exists or output_file_name is a directory
        if os.path.exists(output_file_name):
            if os.path.isfile(output_file_name):
                err_exit(
                    "{} already exists. Please specify a new file path".format(
                        output_file_name
                    )
                )
            if os.path.isdir(output_file_name):
                err_exit(
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
            err_exit("Unexpected error occurred while writing SQL query output")

    else:
        COLUMN_NAMES = output_listdict_or_string[0].keys()

        if not all(
            set(i.keys()) == set(COLUMN_NAMES) for i in output_listdict_or_string
        ):
            err_exit("All rows must have the same column names")

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

        if WRITE_METHOD == "FILE":
            with open(output_file_name, **write_args) as f:
                w = csv.DictWriter(
                    f,
                    COLUMN_NAMES,
                    delimiter=DELIMITER,
                    lineterminator=OS_SPECIFIC_LINE_SEPARATOR,
                    quoting=QUOTING,
                    quotechar=QUOTE_CHAR,
                )
                w.writeheader()
                w.writerows(output_listdict_or_string)

        elif WRITE_METHOD == "STDOUT":
            w = csv.DictWriter(
                sys.stdout,
                COLUMN_NAMES,
                delimiter=DELIMITER,
                lineterminator=OS_SPECIFIC_LINE_SEPARATOR,
                quoting=QUOTING,
                quotechar=QUOTE_CHAR,
            )
            w.writeheader()
            w.writerows(output_listdict_or_string)

        else:
            err_exit("Unexpected error occurred while writing output")
