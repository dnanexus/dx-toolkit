import sys
import csv
import os
from ..exceptions import err_exit

IS_OS_WINDOWS = os.name == "nt"
OS_SPECIFIC_LINE_SEPARATOR = os.linesep
IS_PYTHON_2 = sys.version_info.major == 2
IS_PYTHON_3 = sys.version_info.major == 3


def write_expression_output(
    arg_output,
    arg_delim,
    arg_sql,
    output_listdict_or_string,
    save_uncommon_delim_to_txt=True,
    output_file_name=None,
):
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
        WRITE_MODE = "wb" if IS_PYTHON_2 or IS_OS_WINDOWS else "w"
        NEWLINE = "" if IS_PYTHON_3 else None
        DELIMITER = str(arg_delim) if arg_delim else ","
        QUOTING = csv.QUOTE_MINIMAL
        QUOTE_CHAR = '"'

        if WRITE_METHOD == "FILE":
            with open(output_file_name, WRITE_MODE, newline=NEWLINE) as f:
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
