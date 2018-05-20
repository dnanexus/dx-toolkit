# Copyright (C) 2013-2018 DNAnexus, Inc.
#
# This file is part of dx-toolkit (DNAnexus platform client libraries).
#
#   Licensed under the Apache License, Version 2.0 (the "License"); you may not
#   use this file except in compliance with the License. You may obtain a copy
#   of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#   WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#   License for the specific language governing permissions and limitations
#   under the License.

"""
Utilities used when running executables in a batch. For example, running an analysis on
a set of data samples.
"""

from __future__ import print_function, unicode_literals, division, absolute_import

from collections import defaultdict
import csv
import dxpy
import json

from ..compat import open
from ..exceptions import err_exit, DXError


# Informational columns in the TSV file, which we want to ignore
# TODO: are these the correct names?
BATCH_ID = "batch ID"

# Figure out the type for each input, by describing the
# executable
def _get_types_for_inputs(executable):
    exec_desc = executable.describe()
    input_spec = []
    if ('inputs' in exec_desc and
        exec_desc['inputs'] is not None):
        # workflow-level inputs were defined for the workflow
        input_spec = exec_desc['inputs']
    elif ('inputSpec' in exec_desc and
          exec_desc['inputSpec'] is not None):
        input_spec = exec_desc['inputSpec']
    else:
        raise Exception("executable {} does not have an input specification".format(executable.get_id()))

    input_key_classes={}
    for arg_desc in input_spec:
        input_key_classes[arg_desc['name']] = arg_desc['class']

    return input_key_classes

# val: a value of type string, that needs to be converted
# col_class: the class of the executable argument
#
# return:
#   - the value in the correct type
#   - any platform files referenced in the type
def _type_convert_primitive(val, klass):
    retval = None
    ref_files = []
    if klass == 'string':
        retval = val
    elif klass == 'int':
        retval = int(val)
    elif klass == "boolean":
        retval = bool(val)
    elif klass == 'float':
        retval = float(val)
    elif klass == 'hash':
        retval = json.loads(val)
    elif klass == 'file':
        if not val.startswith("file-"):
            raise Exception("Malformed file {}, must start with 'file-'".format(val))
        retval = dxpy.dxlink(val)
        ref_files.append(retval)
    else:
        raise Exception("class {} not currently supported".format(klass))
    return retval, ref_files

# An array type, such as array:file
def _type_convert_array(val, klass):
    inner_type = klass.split(":")[1]
    len_val = len(val)
    if (len_val < 2 or
        val[0] != '[' or
        val[len_val-1] != ']'):
        raise Exception("Malformed array {}".format(val))
    val_strip_brackets = val[1:(len_val-1)]
    retval = []
    ref_files = []
    elements = [e.strip() for e in val_strip_brackets.split(',')]
    for e in elements:
        e_with_type,files = _type_convert_primitive(e, inner_type)
        retval.append(e_with_type)
        ref_files += files
    return retval, ref_files

def _type_convert(val, klass):
    try:
        if klass.startswith("array:"):
            return _type_convert_array(val, klass)
        else:
            # A primitive type, like {int, float, file, hash}
            return _type_convert_primitive(val, klass)
    except Exception:
        raise Exception("value={} cannot be converted into class {}".format(val, klass))

# For a column that represents files, assume it is named "pair", look for
# the index of column "pair ID".
def _search_column_id(col_name, header_line):
    for i, col_name2 in enumerate(header_line):
        if col_name2 == (col_name + " ID"):
            return i
    raise Exception("Could not find a column with file IDs for {}".format(col_name))

# Parse the TSV file. Create a dictionary with the input arguments for
# each invocation. Return an array of dictionaries.
#
# For each line, except the header:
#   remove informational columns
#   create a dictionary of inputs we can pass to the executable
#
# Example TSV input:
# batch ID, pair1,       pair1 ID, pair2,       pair2 ID
# 23,       SRR123_1.gz, file-XXX, SRR223_2.gz, file-YYY
#
def batch_launch_args(executable, input_json, batch_tsv_file):
    header_line = []
    lines = []
    with open(batch_tsv_file, "rb") as f:
        reader = csv.reader(f, delimiter=str(u'\t'))
        header_line = next(reader)
        lines = list(reader)

    # Get the classes for the executable inputs
    input_classes = _get_types_for_inputs(executable)

    # which columns to use. Note that a file input uses two columns.
    # For example, argument 'pair1' exposes two columns {'pair1, 'pair1 ID'}.
    batch_index = None
    index_2_column = {}
    for i, col_name in enumerate(header_line):
        if col_name == BATCH_ID:
            batch_index = i
        elif (col_name in input_classes and
              input_classes[col_name] != 'file'):
            index_2_column[i] = col_name
        elif (col_name in input_classes and
              input_classes[col_name] == 'file'):
            idx = _search_column_id(col_name, header_line)
            index_2_column[idx] = col_name
    if batch_index is None:
        raise Exception("Could not find column {}".format(BATCH_ID))

    # A dictionary of inputs. Each column in the TSV file is mapped to a row.
    # {
    #    "a": [{dnanexus_link: "file-xxxx"}, {dnanexus_link: "file-yyyy"}, ....],
    #    "b": [1,null, ...]
    # }
    columns=defaultdict(list)
    all_files=[]
    for line in lines:
        for i, val in enumerate(line):
            if i not in index_2_column:
                continue
            col_name = index_2_column[i]
            klass = input_classes[col_name]
            val_w_correct_type, ref_files = _type_convert(val.strip(), klass)
            all_files += ref_files
            columns[col_name].append(val_w_correct_type)

    # Create an array of batch_ids
    batch_ids = [line[batch_index].strip() for line in lines]

    # call validate
    #
    # Output: list of dictionaries, each dictionary corresponds to expanded batch call
    expanded_args = dxpy.api.applet_validate_batch(executable.get_id(),
                                                   { "batchInput": columns,
                                                     "commonInput": input_json,
                                                     "files": all_files,
                                                     "instanceTypes": [] })

    ## future proofing
    if isinstance(expanded_args, dict):
        assert('expandedBatch' in expanded_args)
        launch_args = expanded_args['expandedBatch']
    else:
        launch_args = expanded_args
    if len(launch_args) != len(batch_ids):
        raise Exception("Mismatch in number of launch_args vs. batch_ids ({} != {})"
                        .format(len(launch_args), len(batch_ids)))

    return { "launch_args": launch_args,
             "batch_ids": batch_ids }

#
# executable: applet, app, or workflow
# launch_args: array of dictionaries, each of which contains all arguments needed to
#     invoke the executable.
#
def batch_run(executable, b_args, run_kwargs):
    run_args = run_kwargs.copy()
    exec_name = executable.describe()["name"]
    launch_args = b_args["launch_args"]
    batch_ids = b_args["batch_ids"]
    executions = []
    for idx, input_json in enumerate(launch_args):
        batch_id = batch_ids[idx]
        name = "{}-{}".format(exec_name, batch_id)
        properties = {
            'batch-id': batch_id,
            'batch-name': name
        }
        run_args['name'] = name
        if run_args.get('properties') is not None:
            run_args['properties'].update(properties)
        else:
            run_args['properties'] = properties
        try:
            dxexecution = executable.run(input_json, **run_args)
            executions.append(dxexecution)
        except Exception:
            err_exit()
    return executions
