#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (C) 2022 DNAnexus, Inc.
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

from __future__ import print_function, unicode_literals, division, absolute_import, annotations

import sys
import collections
import json
import os
import re
import csv
import dxpy
import codecs
import subprocess
from functools import reduce
from ..utils.printing import fill
from ..bindings import DXRecord
from ..bindings.dxdataobject_functions import is_dxlink, describe
from ..bindings.dxfile import DXFile
from ..utils.resolver import resolve_existing_path, is_hashid, ResolutionError, resolve_path, check_folder_exists
from ..utils.file_handle import as_handle
from ..utils.describe import print_desc
from ..compat import USING_PYTHON2
from ..exceptions import (
    err_exit,
    PermissionDenied,
    InvalidInput,
    InvalidState,
    ResourceNotFound,
    default_expected_exceptions,
)

from ..dx_extract_utils.filter_to_payload import validate_JSON, final_payload
from ..dx_extract_utils.germline_utils import (
    get_genotype_only_types,
    add_germline_base_sql,
    sort_germline_variant,
    harmonize_germline_sql,
    harmonize_germline_results,
    get_germline_ref_payload,
    get_germline_loci_payload,
    update_genotype_only_ref,
    get_genotype_types,
    infer_genotype_type,
    get_types_to_filter_out_when_infering,
    filter_results
)
from ..dx_extract_utils.input_validation import inference_validation
from ..dx_extract_utils.input_validation_somatic import validate_somatic_filter
from ..dx_extract_utils.somatic_filter_payload import somatic_final_payload
from ..dx_extract_utils.cohort_filter_payload import cohort_filter_payload, cohort_final_payload

from ..bindings.apollo.dataset import Dataset

from ..bindings.apollo.cmd_line_options_validator import ArgsValidator
from ..bindings.apollo.json_validation_by_schema import JSONValidator

from ..bindings.apollo.schemas.input_arguments_validation_schemas import EXTRACT_ASSAY_EXPRESSION_INPUT_ARGS_SCHEMA
from ..bindings.apollo.schemas.assay_filtering_json_schemas import EXTRACT_ASSAY_EXPRESSION_JSON_SCHEMA
from ..bindings.apollo.schemas.assay_filtering_conditions import EXTRACT_ASSAY_EXPRESSION_FILTERING_CONDITIONS

from ..bindings.apollo.vizserver_filters_from_json_parser import JSONFiltersValidator
from ..bindings.apollo.vizserver_payload_builder import VizPayloadBuilder
from ..bindings.apollo.vizclient import VizClient

from ..bindings.apollo.data_transformations import transform_to_expression_matrix
from .output_handling import write_expression_output, pretty_print_json

from .help_messages import EXTRACT_ASSAY_EXPRESSION_JSON_HELP, EXTRACT_ASSAY_EXPRESSION_ADDITIONAL_FIELDS_HELP

database_unique_name_regex = re.compile("^database_\w{24}__\w+$")
database_id_regex = re.compile("^database-\\w{24}$")


def resolve_validate_record_path(path):

    project, folder_path, entity_result = resolve_existing_path(path)

    if project is None:
        raise ResolutionError(
            'Unable to resolve "'
            + path
            + '" to a data object or folder name in a project'
        )
    elif project != entity_result["describe"]["project"]:
        raise ResolutionError(
            'Unable to resolve "'
            + path
            + "\" to a data object or folder name in '"
            + project
            + "'"
        )

    if entity_result["describe"]["class"] != "record":
        err_exit(
            "{}: Invalid path. The path must point to a record type of cohort or dataset".format(path)
            
        )

    try:
        resp = dxpy.DXHTTPRequest(
            "/" + entity_result["id"] + "/visualize",
            {"project": project, "cohortBrowser": False},
        )
    except PermissionDenied:
        err_exit("Insufficient permissions", expected_exceptions=(PermissionDenied,))
    except (InvalidInput, InvalidState):
        err_exit(
            "%s : Invalid cohort or dataset" % entity_result["id"],
            expected_exceptions=(
                InvalidInput,
                InvalidState,
            ),
        )
    except Exception as details:
        err_exit(str(details))

    if resp["datasetVersion"] != "3.0":
        err_exit(
            "%s : Invalid version of cohort or dataset. Version must be 3.0"
            % resp["datasetVersion"]
        )

    if ("Dataset" in resp["recordTypes"]) or ("CohortBrowser" in resp["recordTypes"]):
        dataset_project = resp["datasetRecordProject"]
    else:
        err_exit(
            "%s : Invalid path. The path must point to a record type of cohort or dataset"
            % resp["recordTypes"]
        )

    return project, entity_result, resp, dataset_project


def viz_query_api_call(resp, payload, route):
    resource_val = resp["url"] + "/viz-query/3.0/" + resp["dataset"] + "/" + route
    try:
        resp_query = dxpy.DXHTTPRequest(
            resource=resource_val, data=payload, prepend_srv=False
        )

    except Exception as details:
        err_exit(str(details))
    sql_results = resp_query["sql"] + ";"
    return sql_results


def raw_query_api_call(resp, payload):
    return viz_query_api_call(resp, payload, 'raw-query')


def raw_cohort_query_api_call(resp, payload):
    return viz_query_api_call(resp, payload, 'raw-cohort-query')


def raw_api_call(resp, payload, sql_message=True):
    resource_val = resp["url"] + "/data/3.0/" + resp["dataset"] + "/raw"
    try:
        resp_raw = dxpy.DXHTTPRequest(
            resource=resource_val, data=payload, prepend_srv=False
        )
        if "error" in resp_raw.keys():
            if resp_raw["error"]["type"] == "InvalidInput":
                err_message = "Insufficient permissions due to the project policy.\n" + resp_raw["error"]["message"]
            elif sql_message and resp_raw["error"]["type"] == "QueryTimeOut":
                err_message = "Please consider using `--sql` option to generate the SQL query and query via a private compute cluster.\n" + resp_raw["error"]["message"]        
            elif resp_raw["error"]["type"] == "QueryBuilderError" and resp_raw["error"]["details"] == "rsid exists in request filters without rsid entries in rsid_lookup_table.":
                err_message = "At least one rsID provided in the filter is not present in the provided dataset or cohort"
            elif resp_raw["error"]["type"] == "DxApiError":
                err_message = resp_raw["error"]["message"]
            else:
                err_message = resp_raw["error"]
            err_exit(str(err_message))
    except Exception as details:
        err_exit(str(details))
    return resp_raw


def extract_dataset(args):
    """
    Retrieves the data or generates SQL to retrieve the data from a dataset or cohort for a set of entity.fields. Additionally, the dataset's dictionary can be extracted independently or in conjunction with data.
    """
    if (
        not args.dump_dataset_dictionary
        and not args.list_fields
        and not args.list_entities
        and args.fields is None
        and args.fields_file is None
    ):
        err_exit(
            "Must provide at least one of the following options: --fields, --fields-file, --dump-dataset-dictionary, --list-fields, --list-entities"
        )

    if (
        args.fields is not None
        and args.fields_file is not None
    ):
        err_exit(
            "dx extract_dataset: error: only one of the arguments, --fields or --fields-file, may be supplied at a given time"
        )

    listing_restricted = {
        "dump_dataset_dictionary": False,
        "sql": False,
        "fields": None,
        "fields_file": None,
        "output": None,
        "delim": ",",
    }

    def check_options(args, restricted):
        error_list = []
        for option, value in restricted.items():
            if args.__dict__[option] != value:
                error_list.append("--{}".format(option.replace("_", "-")))
        return error_list

    if args.list_fields:
        listing_restricted["list_entities"] = False
        error_list = check_options(args, listing_restricted)
        if error_list:
            err_exit("--list-fields cannot be specified with: {}".format(error_list))

    if args.list_entities:
        listing_restricted["list_fields"] = False
        listing_restricted["entities"] = None
        error_list = check_options(args, listing_restricted)
        if error_list:
            err_exit("--list-entities cannot be specified with: {}".format(error_list))

    delimiter = str(codecs.decode(args.delim, "unicode_escape"))
    if len(delimiter) == 1 and delimiter != '"':
        if delimiter == ",":
            out_extension = ".csv"
        elif delimiter == "\t":
            out_extension = ".tsv"
        else:
            out_extension = ".txt"
    else:
        err_exit("Invalid delimiter specified")

    project, entity_result, resp, dataset_project = resolve_validate_record_path(args.path)

    dataset_id = resp["dataset"]
    out_directory = ""
    out_file_field = ""
    print_to_stdout = False
    files_to_check = []
    file_already_exist = []

    def _check_system_python_version():
        python_version = sys.version_info[:3]
        # Set python version range
        # python_range = 0 for python_version>="3.7"
        # python_range = 1 for python_version>="3.5.3" and python_version<"3.7"
        # python_range = 2 for python_version<"3.5.3"
        if python_version >= (3, 7):
            python_range = "0"
        elif python_version >= (3, 5, 3):
            python_range = "1"
        else:
            python_range = "2"
        return python_range

    def _check_pandas_version(
        python_range, current_pandas_version, pandas_version_range
    ):
        # Valid pandas versions based on python versions
        # python_range = 0; python_version>="3.7"; Valid pandas version: pandas==1.3.5
        # python_range = 1; python_version>="3.5.3" and python_version<"3.7"; Valid pandas version: pandas>=0.23.3,<=0.25.3
        # python_range = 2; python_version<"3.5.3"; Valid pandas version: pandas>=0.23.3,< 0.25.0
        system_pandas_version = tuple(map(int, current_pandas_version.split(".")))
        if (
            (python_range == "0" and system_pandas_version == (1, 3, 5))
            or (
                python_range == "1"
                and ((0, 25, 3) >= system_pandas_version >= (0, 23, 3))
            )
            or (
                python_range == "2"
                and ((0, 25, 0) > system_pandas_version >= (0, 23, 3))
            )
        ):
            pass
        else:
            print(
                "Warning: For '-ddd' usage, the recommended pandas version is {}. The installed version of pandas is {}. It is recommended to update pandas. For example, 'pip/pip3 install -I pandas==X.X.X' where X.X.X is {}.".format(
                    pandas_version_range, current_pandas_version, pandas_version_range
                )
            )

    if args.dump_dataset_dictionary:
        global pd
        pandas_version_dictionary = {
            "0": "'1.3.5'",
            "1": ">= '0.23.3' and <= '0.25.3'",
            "2": ">= '0.23.3' and < '0.25.0'",
        }
        python_range = _check_system_python_version()
        try:
            import pandas as pd

            current_pandas_version = pd.__version__
            _check_pandas_version(
                python_range,
                current_pandas_version,
                pandas_version_dictionary[python_range],
            )
        except ImportError as e:
            err_exit(
                "'-ddd' requires the use of pandas, which is not currently installed. Please install pandas to a version {}. For example, 'pip/pip3 install -I pandas==X.X.X' where X.X.X is {}.".format(
                    pandas_version_dictionary[python_range],
                    pandas_version_dictionary[python_range],
                )
            )

        if args.output is None:
            out_directory = os.getcwd()
        elif args.output == "-":
            print_to_stdout = True
        elif os.path.exists(args.output):
            if os.path.isdir(args.output):
                out_directory = args.output
            else:
                err_exit(
                    "Error: When using -ddd, --output must be an existing directory"
                )
        else:
            err_exit("Error: When using -ddd, --output must be an existing directory")

        if print_to_stdout:
            output_file_data = sys.stdout
            output_file_coding = sys.stdout
            output_file_entity = sys.stdout
        else:
            output_file_data = os.path.join(
                out_directory, resp["recordName"] + ".data_dictionary" + out_extension
            )
            output_file_coding = os.path.join(
                out_directory, resp["recordName"] + ".codings" + out_extension
            )
            output_file_entity = os.path.join(
                out_directory, resp["recordName"] + ".entity_dictionary" + out_extension
            )
            files_to_check = [output_file_data, output_file_coding, output_file_entity]

    if args.fields or args.fields_file:
        if args.sql:
            file_name_suffix = ".data.sql"
        else:
            file_name_suffix = out_extension

        if args.output is None:
            out_directory = os.getcwd()
            out_file_field = os.path.join(
                out_directory, resp["recordName"] + file_name_suffix
            )
            files_to_check.append(out_file_field)
        elif args.output == "-":
            print_to_stdout = True
        elif os.path.exists(args.output):
            if os.path.isdir(args.output):
                out_directory = args.output
                out_file_field = os.path.join(
                    out_directory, resp["recordName"] + file_name_suffix
                )
                files_to_check.append(out_file_field)
            else:
                file_already_exist.append(args.output)
        elif os.path.exists(os.path.dirname(args.output)) or not os.path.dirname(
            args.output
        ):
            out_file_field = args.output
        else:
            err_exit(
                "Error: {path} could not be found".format(
                    path=os.path.dirname(args.output)
                )
            )

    for file in files_to_check:
        if os.path.exists(file):
            file_already_exist.append(file)

    if file_already_exist:
        err_exit("Error: path already exists {path}".format(path=file_already_exist))

    rec_descriptor = DXDataset(dataset_id, project=dataset_project).get_descriptor()
    
    fields_list = []
    if args.fields is not None:
        fields_list = [field.strip() for field in args.fields.split(",")]
    elif args.fields_file is not None:
        if os.path.isfile(args.fields_file):
            with open(args.fields_file, "r") as infile:
                for line in infile:
                    fields_list.append(line.strip("\n"))
        else:
            err_exit(
                "The file, {input_fields_file}, supplied using --fields-file could not be found".format(
                    input_fields_file=args.fields_file
                )
            )

    if fields_list:
        error_list = []
        for entry in fields_list:
            entity_field = entry.split(".")
            if len(entity_field) < 2:
                error_list.append(entry)
            elif (
                entity_field[0] not in rec_descriptor.model["entities"].keys()
                or entity_field[1]
                not in rec_descriptor.model["entities"][entity_field[0]][
                    "fields"
                ].keys()
            ):
                error_list.append(entry)

        if error_list:
            err_exit("The following fields cannot be found: %s" % error_list)

        payload = {
            "project_context": project,
            "fields": [{item: "$".join(item.split("."))} for item in fields_list],
        }
        if "CohortBrowser" in resp["recordTypes"]:
            if resp.get("baseSql"):
                payload["base_sql"] = resp.get("baseSql")
            payload["filters"] = resp["filters"]

        if args.sql:
            sql_results = raw_query_api_call(resp, payload)
            if print_to_stdout:
                print(sql_results)
            else:
                with open(out_file_field, "w") as f:
                    print(sql_results, file=f)
        else:
            resp_raw = raw_api_call(resp, payload)
            csv_from_json(
                out_file_name=out_file_field,
                print_to_stdout=print_to_stdout,
                sep=delimiter,
                raw_results=resp_raw["results"],
                column_names=fields_list,
            )

    elif args.sql:
        err_exit("`--sql` passed without `--fields` or `--fields-file")

    if args.dump_dataset_dictionary:
        rec_dict = rec_descriptor.get_dictionary()
        write_ot = rec_dict.write(
            output_file_data=output_file_data,
            output_file_entity=output_file_entity,
            output_file_coding=output_file_coding,
            sep=delimiter,
        )

    # Listing section
    if args.list_entities or args.list_fields:
        # Retrieve entity names, titles and main entity
        entity_names_and_titles, _main_entity = retrieve_entities(rec_descriptor.model)
        # List entities
        if args.list_entities:
            print("\n".join(entity_names_and_titles))
        # List fields
        if args.list_fields:
            list_fields(rec_descriptor.model, _main_entity, args)


def get_assay_info(rec_descriptor, assay_type):
    assay_list = rec_descriptor.assays
    selected_type_assays = []
    other_assays = []
    if not assay_list:
        err_exit("No valid assays in the dataset.")
    else:
        for a in assay_list:
            if a["generalized_assay_model"] == assay_type:
                selected_type_assays.append(a)
            else:
                other_assays.append(a)
    return (selected_type_assays, other_assays)


#### Validate json filters ####
def json_validation_function(filter_type, args):
    filter_arg = "args.retrieve_" + filter_type
    filter_value = str(vars(args)["retrieve_" + filter_type])
    filter = {}
    if filter_value.endswith(".json"):
        if os.path.isfile(filter_value):
            if os.stat(filter_value).st_size == 0:
                err_exit(
                    'No filter given for --retrieve-{filter_type} or JSON for "--retrieve-{filter_type}" does not contain valid filter information.'.format(
                        filter_type=filter_type
                    )
                )
            else:
                with open(filter_value, "r") as json_file:
                    try:
                        filter = json.load(json_file)
                    except Exception as json_error:
                        err_exit(
                            "JSON for variant filters is malformatted.",
                            expected_exceptions=default_expected_exceptions,
                        )
        else:
            err_exit(
                "JSON file {filter_json} provided does not exist".format(
                    filter_json=filter_value
                )
            )
    else:
        if filter_value == "{}":
            err_exit(
                'No filter given for --retrieve-{filter_type} or JSON for "--retrieve-{filter_type}" does not contain valid filter information.'.format(
                    filter_type=filter_type
                )
            )
        else:
            try:
                filter = json.loads(filter_value)
            except Exception as json_error:
                err_exit(
                    "JSON for variant filters is malformatted.",
                    expected_exceptions=default_expected_exceptions,
                )

    if filter_type in ["allele", "annotation", "genotype"]:
        validate_JSON(filter, filter_type)
    elif filter_type in ["variant"]:
        validate_somatic_filter(filter, filter_type)

    return filter
   

def retrieve_meta_info(resp, project_id, assay_id, assay_name, print_to_stdout, out_file_name):
    table = "vcf_meta_information_unique"
    columns = ["Field", "ID", "Type", "Number", "Description"]
    payload = {
        "project_context": project_id,
        "fields": [
            {column: "$".join((table, column))} for column in columns
        ],
        "is_cohort": False,
        "variant_browser": {
            "name": assay_name,
            "id": assay_id,
        },
    }
    resp_raw = raw_api_call(resp, payload, sql_message=False)

    csv_from_json(
        out_file_name=out_file_name,
        print_to_stdout=print_to_stdout,
        sep="\t",
        raw_results=resp_raw["results"],
        column_names=columns,
    )


def assign_output_method(args, record_name, friendly_assay_type):
    #### Decide output method based on --output and --sql ####
    if args.sql:
        file_name_suffix = ".data.sql"
    elif friendly_assay_type == 'somatic' and args.retrieve_meta_info:
        file_name_suffix = ".vcf_meta_info.txt"
    else:
        file_name_suffix = ".tsv"
    file_already_exist = []
    files_to_check = []
    out_file = ""

    print_to_stdout = False
    if args.output is None:
        out_directory = os.getcwd()
        out_file = os.path.join(out_directory, record_name + file_name_suffix)
        files_to_check.append(out_file)
    elif args.output == "-":
        print_to_stdout = True
    elif os.path.exists(args.output):
        if os.path.isdir(args.output):
            err_exit("--output should be a file, not a directory.")
        else:
            file_already_exist.append(args.output)
    elif os.path.exists(os.path.dirname(args.output)) or not os.path.dirname(
        args.output
    ):
        out_file = args.output
    else:
        err_exit(
            "Error: {path} could not be found".format(path=os.path.dirname(args.output))
        )

    for file in files_to_check:
        if os.path.exists(file):
            file_already_exist.append(file)

    if file_already_exist:
        err_exit("Cannot specify the output to be an existing file.")
    return out_file, print_to_stdout

def get_assay_name_info(
    list_assays, assay_name, path, friendly_assay_type, rec_descriptor
):
    """
    Generalized function for determining assay name and reference genome
    """
    if friendly_assay_type == "germline":
        assay_type = "genetic_variant"
    elif friendly_assay_type == "somatic":
        assay_type = "somatic_variant"

    #### Get names of genetic assays ####
    if list_assays:
        (target_assays, other_assays) = get_assay_info(
            rec_descriptor, assay_type=assay_type
        )
        if not target_assays:
            err_exit("There's no {} assay in the dataset provided.".format(friendly_assay_type))
        else:
            for a in target_assays:
                print(a["name"])
            sys.exit(0)

    #### Decide which assay is to be queried and which ref genome is to be used ####
    (target_assays, other_assays) = get_assay_info(
        rec_descriptor, assay_type=assay_type
    )

    target_assay_names = [ga["name"] for ga in target_assays]
    target_assay_ids = [ga["uuid"] for ga in target_assays]
    other_assay_names = [oa["name"] for oa in other_assays]
    # other_assay_ids = [oa["uuid"] for oa in other_assays]

    if target_assay_names and target_assay_ids:
        selected_assay_name = target_assay_names[0]
        selected_assay_id = target_assay_ids[0]
    else:
        err_exit("There's no {} assay in the dataset provided.".format(friendly_assay_type))
    if assay_name:
        if assay_name not in list(target_assay_names):
            if assay_name in list(other_assay_names):
                err_exit(
                    "This is not a valid assay. For valid assays accepted by the function, `extract_assay {}`, please use the --list-assays flag.".format(
                        friendly_assay_type
                    )
                )
            else:
                err_exit(
                    "Assay {assay_name} does not exist in the {path}.".format(
                        assay_name=assay_name, path=path
                    )
                )
        else:
            selected_assay_name = assay_name
            for ga in target_assays:
                if ga["name"] == assay_name:
                    selected_assay_id = ga["uuid"]
    
    additional_descriptor_info = {}

    if friendly_assay_type == "germline":
        selected_ref_genome = "GRCh38.92"
        for a in target_assays:
            if a["name"] == selected_assay_name:
                if a["reference_genome"]:
                    selected_ref_genome = a["reference_genome"]["name"].split(".", 1)[1]
                additional_descriptor_info["genotype_type_table"] = a["entities"]["genotype"]["fields"]["type"]["mapping"]["table"]
                for exclude_genotype in ("exclude_refdata", "exclude_halfref", "exclude_nocall"):
                    if exclude_genotype in a:
                        additional_descriptor_info[exclude_genotype] = a[exclude_genotype]
    elif friendly_assay_type == "somatic":
        selected_ref_genome = ""
        for a in target_assays:
            if a["name"] == selected_assay_name and a["reference"]:
                selected_ref_genome = a["reference"]["name"] + "." + a["reference"]["annotation_source_version"]

    return(selected_assay_name, selected_assay_id, selected_ref_genome, additional_descriptor_info)


def comment_fill(string, comment_string='#  ', **kwargs):
    width_adjustment = kwargs.pop('width_adjustment', 0) - len(comment_string)
    return re.sub('^', comment_string, fill(string, width_adjustment=width_adjustment, **kwargs), flags=re.MULTILINE)


def retrieve_samples(resp: dict, assay_name: str, assay_id: str) -> list:
    """
    Get the list of sample_ids from the sample table for the selected assay.
    """
    sample_payload = {
        "project_context": resp["datasetRecordProject"],
        "fields": [{"sample_id": "sample$sample_id"}],
        "raw_filters": {"assay_filters": {"name": assay_name, "id": assay_id}},
    }
    return [_["sample_id"] for _ in raw_api_call(resp, sample_payload)["results"]]


def extract_assay_germline(args):
    """
    Retrieve the selected data or generate SQL to retrieve the data from an genetic variant assay in a dataset or cohort based on provided rules.
    """
    ######## Input combination validation ########
    filter_given = False
    if args.retrieve_allele or args.retrieve_annotation or args.retrieve_genotype:
        filter_given = True
    #### Check if valid options are passed with the --json-help flag ####
    if args.json_help:
        if not filter_given:
            err_exit(
                'Please specify one of the following: --retrieve-allele, --retrieve-genotype or --retrieve-annotation" for details on the corresponding JSON template and filter definition.'
            )
        elif args.list_assays or args.assay_name or args.sql or args.output:
            err_exit(
                "Please check to make sure the parameters are set properly. --json-help cannot be specified with options other than --retrieve-annotation/--retrieve-allele/--retrieve-genotype."
            )
    #### Validate that other arguments are not passed with --list-assays ####
    if args.list_assays:
        if args.sql:
            err_exit("The flag, --sql, cannot be used with --list-assays.")
        elif args.output:
            err_exit(
                'When --list-assays is specified, output is to STDOUT. "--output" may not be supplied.'
            )
        elif filter_given:
            err_exit("--list-assays cannot be presented with other options.")

    #### Validate that a retrieve options infer_ref or infer_nocall are not passed with retrieve_allele or retrieve_annotation ####
    if args.infer_ref or args.infer_nocall:
        if args.retrieve_allele or args.retrieve_annotation or args.sql or args.list_assays:
            err_exit(
                "The flags, --infer-ref and --infer-nocall, can only be used with --retrieve-genotype."
            )

    #### Check if the retrieve options are passed correctly, print help if needed ####
    if args.retrieve_allele:
        if args.json_help:
            print(
                comment_fill('Filters and respective definitions', comment_string='# ') + '\n#\n' +
                comment_fill('rsid: rsID associated with an allele or set of alleles. If multiple values are provided, the conditional search will be, "OR." For example, ["rs1111", "rs2222"], will search for alleles which match either "rs1111" or "rs2222". String match is case sensitive. Duplicate values are permitted and will be handled silently.') + '\n#\n' +
                comment_fill('type: Type of allele. Accepted values are "SNP", "Ins", "Del", "Mixed". If multiple values are provided, the conditional search will be, "OR." For example, ["SNP", "Ins"], will search for variants which match either "SNP" or "Ins". String match is case sensitive.') + '\n#\n' +
                comment_fill('dataset_alt_af: Dataset alternate allele frequency, a json object with empty content or two sets of key/value pair: {min: 0.1, max:0.5}. Accepted numeric value for each key is between and including 0 and 1.  If a user does not want to apply this filter but still wants this information in the output, an empty json object should be provided.') + '\n#\n' +
                comment_fill('gnomad_alt_af: gnomAD alternate allele frequency. a json object with empty content or two sets of key/value pair: {min: 0.1, max:0.5}. Accepted value for each key is between 0 and 1. If a user does not want to apply this filter but still wants this information in the output, an empty json object should be provided.') + '\n#\n' +
                comment_fill('location: Genomic range in the reference genome where the starting position of alleles fall into. If multiple values are provided in the list, the conditional search will be, "OR." String match is case sensitive.') + '\n#\n' +
                comment_fill('JSON filter template for --retrieve-allele', comment_string='# ') + '\n' +
'{\n  "rsid": ["rs11111", "rs22222"],\n  "type": ["SNP", "Del", "Ins"],\n  "dataset_alt_af": {"min": 0.001, "max": 0.05},\n  "gnomad_alt_af": {"min": 0.001, "max": 0.05},\n  "location": [\n    {\n      "chromosome": "1",\n      "starting_position": "10000",\n      "ending_position": "20000"\n    },\n    {\n      "chromosome": "X",\n      "starting_position": "500",\n      "ending_position": "1700"\n    }\n  ]\n}'
            )
            sys.exit(0)
    elif args.retrieve_annotation:
        if args.json_help:
            print(
                comment_fill('Filters and respective definitions', comment_string='# ') + '\n#\n' +
                comment_fill('allele_id: ID of an allele for which annotations should be returned. If multiple values are provided, annotations for any alleles that match one of the values specified will be listed. For example, ["1_1000_A_T", "1_1010_C_T"], will search for annotations of alleles which match either "1_1000_A_T" or ""1_1010_C_T". String match is case insensitive.') + '\n#\n' +
                comment_fill('gene_name: Gene name of the annotation. A list of gene names whose annotations will be returned. If multiple values are provided, the conditional search will be, "OR." For example, ["BRCA2", "ASPM"], will search for annotations which match either "BRCA2" or "ASPM". String match is case insensitive.') + '\n#\n' +
                comment_fill('gene_id: Ensembl gene ID (ENSG) of the annotation. If multiple values are provided, the conditional search will be, "OR." For example, ["ENSG00000302118", "ENSG00004000504"], will search for annotations which match either "ENSG00000302118" or "ENSG00004000504". String match is case insensitive.') + '\n#\n' +
                comment_fill('feature_id: Ensembl feature id (ENST) where the range overlaps with the variant. Currently, only  coding transcript IDs are searched. If multiple values are provided, the conditional search will be, "OR." For example, ["ENST00000302118.5", "ENST00004000504.1"], will search for annotations which match either "ENST00000302118.5" or "ENST00004000504.1". String match is case insensitive.') + '\n#\n' +
                comment_fill('consequences: Consequence as recorded in the annotation. If multiple values are provided, the conditional search will be, "OR." For example, ["5_prime_UTR_variant", "3_prime_UTR_variant"], will search for annotations which match either "5 prime UTR variant" or "3 prime UTR variant". String match is case sensitive. For all supported consequences terms, please refer to snpeff: http://pcingola.github.io/SnpEff/se_inputoutput/#effect-prediction-details (Effect Seq. Ontology column). This filter cannot be specified by itself, and must be included with at least one of the following filters: "gene_id", "gene_name",or "feature_id".') + '\n#\n' +
                comment_fill('putative_impact: Putative impact as recorded in the annotation. Possible values are [ "HIGH", "MODERATE", "LOW", "MODIFIER"]. If multiple values are provided, the conditional search will be, "OR." For example, ["MODIFIER", "HIGH"], will search for annotations which match either "MODIFIER" or "HIGH". String match is case insensitive. For all supported terms, please refer to snpeff: http://pcingola.github.io/SnpEff/se_inputoutput/#impact-prediction. This filter cannot be specified by itself, and must be included with at least one of the following filters: "gene_id", "gene_name", or "transcript_id".') + '\n#\n' +
                comment_fill('hgvs_c: HGVS (DNA) code of the annotation. If multiple values are provided, the conditional search will be, "OR." For example, ["c.-49A>G", "c.-20T>G"], will search for annotations which match either "c.-49A>G" or "c.-20T>G". String match is case sensitive.') + '\n#\n' +
                comment_fill('hgvs_p: HGVS (Protein) code of the annotation. If multiple values are provided, the conditional search will be, "OR." For example, ["p.Gly2Asp", "p.Aps2Gly"], will search for annotations which match either "p.Gly2Asp" or "p.Aps2Gly". String match is case sensitive.') + '\n#\n' +
                comment_fill('JSON filter template for --retrieve-annotation', comment_string='# ') + '\n' +
                '{\n  "allele_id":["1_1000_A_T","2_1000_G_C"],\n  "gene_name": ["BRCA2"],\n  "gene_id": ["ENST00000302118"],\n  "feature_id": ["ENST00000302118.5"],\n  "consequences": ["5 prime UTR variant"],\n  "putative_impact": ["MODIFIER"],\n  "hgvs_c": ["c.-49A>G"],\n  "hgvs_p": ["p.Gly2Asp"]\n}'
            )
            sys.exit(0)
    elif args.retrieve_genotype:
        if args.json_help:
            print(
                comment_fill('Filters and respective definitions', comment_string='# ') + '\n#\n' +
                comment_fill('allele_id: ID(s) of one or more alleles for which sample genotypes will be returned. If multiple values are provided, any samples having at least one allele that match any of the values specified will be listed. For example, ["1_1000_A_T", "1_1010_C_T"], will search for samples with at least one allele matching either "1_1000_A_T" or "1_1010_C_T". String match is case insensitive.') + '\n#\n' +
                comment_fill('location: Genomic position in the reference genome of the starting position of the alleles.  If multiple values are provided in the list, the conditional search will be, "OR." String match is case sensitive.') + '\n#\n' +
                comment_fill('allele_id and location are mutually exclusive filters.') + '\n#\n' +
                comment_fill('sample_id: Optional, one or more sample IDs for which sample genotypes will be returned. If the provided object is a cohort, this further intersects the sample ids. If a user has a list of samples more than 1,000, it is recommended to use a cohort id containing all the samples.') + '\n#\n' +
                comment_fill('genotype_type: Optional, one or more genotype types for which sample genotype types will be returned.') + '\n' +
                comment_fill('One of:') + '\n' +
                comment_fill('\tref\t(homozygous for the reference allele\t\t\te.g. 0/0)') + '\n' +
                comment_fill('\thet-ref\t(heterozygous for the ref allele and alt allele\t\te.g. 0/1)') + '\n' +
                comment_fill('\thom\t(homozygous for the non-ref allele\t\t\te.g. 1/1)') + '\n' +
                comment_fill('\thet-alt\t(heterozygous with two distinct alt alleles\t\te.g. 1/2)') + '\n' +
                comment_fill('\thalf\t(only one allele is known, second allele is unknown\te.g. ./1)') + '\n' +
                comment_fill('\tno-call\t(both alleles are unknown\t\t\t\te.g. ./.)') + '\n#\n' +
                comment_fill('JSON filter templates for --retrieve-genotype', comment_string='# ') + '\n#\n' +
                comment_fill('Example using location:', comment_string='# ') + '\n' +
                pretty_print_json(
                    {
                        "sample_id": ["s1", "s2"],
                        "location": [
                            {"chromosome": "1", "starting_position": "10000"},
                            {"chromosome": "X", "starting_position": "500"},
                        ],
                        "genotype_type": ["ref", "het-ref", "hom", "het-alt", "half", "no-call"],
                    }
                ) + '\n' +
                comment_fill('Example using allele_id:', comment_string='# ') + '\n' +
                pretty_print_json({
                    "sample_id": ["s1", "s2"],
                    "allele_id": ["1_1000_A_T", "2_1000_G_C"],
                    "genotype_type": ["het-ref", "hom", "het-alt", "half"],
                }
                )
            )
            sys.exit(0)

    if args.retrieve_allele:
        filter_dict = json_validation_function("allele", args)
    elif args.retrieve_annotation:
        filter_dict = json_validation_function("annotation", args)
    elif args.retrieve_genotype:
        filter_dict = json_validation_function("genotype", args)

    #### Validate that a retrieve option is passed with --assay-name ####
    if args.assay_name:
        if not filter_given:
            err_exit(
                "--assay-name must be used with one of --retrieve-allele,--retrieve-annotation, --retrieve-genotype."
            )

    #### Validate that a retrieve option is passed with --sql ####
    if args.sql:
        if not filter_given:
            err_exit(
                "When --sql provided, must also provide at least one of the three options: --retrieve-allele <JSON>; --retrieve-genotype <JSON>; --retrieve-annotation <JSON>."
            )

    ######## Data Processing ########
    project, entity_result, resp, dataset_project = resolve_validate_record_path(args.path)

    if "CohortBrowser" in resp["recordTypes"] and any(
        [args.list_assays, args.assay_name]
    ):
        err_exit(
            "Currently --assay-name and --list-assays may not be used with a CohortBrowser record (Cohort Object) as input. To select a specific assay or to list assays, please use a Dataset Object as input."
        )
    dataset_id = resp["dataset"]
    rec_descriptor = DXDataset(dataset_id, project=dataset_project).get_descriptor()

    selected_assay_name, selected_assay_id, selected_ref_genome, additional_descriptor_info = get_assay_name_info(
        args.list_assays, args.assay_name, args.path, "germline", rec_descriptor
    )

    out_file, print_to_stdout = assign_output_method(args, resp["recordName"], "germline")

    filter_type = None
    if args.retrieve_allele:
        filter_type = "allele"
    elif args.retrieve_annotation:
        filter_type = "annotation"

    if filter_type and filter_given:
        payload, fields_list = final_payload(
            full_input_dict=filter_dict,
            name=selected_assay_name,
            id=selected_assay_id,
            project_context=project,
            genome_reference=selected_ref_genome,
            filter_type=filter_type,
        )

        add_germline_base_sql(resp, payload)

        if args.sql:
            sql_results = raw_query_api_call(resp, payload)

            if print_to_stdout:
                print(sql_results)
            else:
                with open(out_file, "w") as sql_file:
                    print(sql_results, file=sql_file)
        else:
            resp_raw = raw_api_call(resp, payload)
            ordered_results = sorted(resp_raw["results"], key=sort_germline_variant)

            csv_from_json(
                out_file_name=out_file,
                print_to_stdout=print_to_stdout,
                sep="\t",
                raw_results=ordered_results,
                column_names=fields_list,
                quote_char=str("|"),
            )

    if args.retrieve_genotype and filter_given:
        exclude_refdata: bool = additional_descriptor_info.get("exclude_refdata")
        exclude_halfref: bool = additional_descriptor_info.get("exclude_halfref")
        exclude_nocall: bool = additional_descriptor_info.get("exclude_nocall")
        inference_validation(
            args.infer_nocall,
            args.infer_ref,
            filter_dict,
            exclude_nocall,
            exclude_refdata,
            exclude_halfref
            )
        # in case of infer flags, we query all the genotypes and do the filtering post query
        if args.infer_ref or args.infer_nocall:
            types_to_filter_out = get_types_to_filter_out_when_infering(filter_dict.get("genotype_type", []))
            filter_dict["genotype_type"] = []

        # get a list of requested genotype types for the genotype table only queries
        if "allele_id" in filter_dict:
            genotype_only_types = []
        else:
            genotype_only_types = get_genotype_only_types(filter_dict,
                                                          exclude_refdata, exclude_halfref, exclude_nocall)
            
        # get the payload for the genotype/allele table query for alternate genotype types
        genotype_payload, fields_list = final_payload(
            full_input_dict=filter_dict,
            name=selected_assay_name,
            id=selected_assay_id,
            project_context=project,
            genome_reference=selected_ref_genome,
            filter_type="genotype",
            order=not genotype_only_types,
        )

        add_germline_base_sql(resp, genotype_payload)

        genotype_only_payloads = []
        if genotype_only_types:
            # get the payloads for the genotype table only query
            # assay_filter does not support "or" so there is a separate query for each partition
            for i, genotype_only_type in enumerate(genotype_only_types):
                genotype_only_filter_dict = filter_dict.copy()
                if genotype_only_type == "ref":
                    genotype_only_filter_dict["ref_yn"] = True
                elif genotype_only_type == "half":
                    genotype_only_filter_dict["halfref_yn"] = True
                elif genotype_only_type == "no-call":
                    genotype_only_filter_dict["nocall_yn"] = True

                genotype_only_payload, _ = final_payload(
                    full_input_dict=genotype_only_filter_dict,
                    name=selected_assay_name,
                    id=selected_assay_id,
                    project_context=project,
                    genome_reference=selected_ref_genome,
                    filter_type="genotype_only",
                    exclude_refdata=genotype_only_type != "ref",
                    exclude_halfref=genotype_only_type != "half",
                    exclude_nocall=genotype_only_type != "no-call",
                    order=i == len(genotype_only_types) - 1,
                )

                add_germline_base_sql(resp, genotype_only_payload)

                genotype_only_payloads.append(genotype_only_payload)

        # get the list of requested genotype types for the genotype/allele table query
        genotype_types = get_genotype_types(filter_dict)

        if args.sql:
            sql_queries = []
            if genotype_types:
                # get the genotype/allele table query
                genotype_sql_query = raw_query_api_call(resp, genotype_payload)[:-1]
                try:
                    geno_table_regex = r"\b" + additional_descriptor_info["genotype_type_table"] + r"\w+"
                    re.search(geno_table_regex, genotype_sql_query).group()
                except Exception:
                    err_exit("Failed to find the table, {}, in the generated SQL".format(
                        additional_descriptor_info["genotype_type_table"]), expected_exceptions=(AttributeError,))
                sql_queries.append(genotype_sql_query)

            # get the genotype table only query
            # assay_filter does not support "or" so there is a separate query for each partition
            for genotype_only_payload in genotype_only_payloads:
                # join the allele table to get the ref, will join on locus_id
                genotype_only_payload["fields"].append({"ref": "allele$ref"})
                genotype_only_sql_query = raw_query_api_call(resp, genotype_only_payload)[:-1]
                # update the query to add column in the genotype/allele table query and join on locus_id
                sql_queries.append(harmonize_germline_sql(genotype_only_sql_query))

            # combine the queries into a single query
            sql_results = " UNION ".join(sql_queries) + ";"

            if print_to_stdout:
                print(sql_results)
            else:
                with open(out_file, "w") as sql_file:
                    print(sql_results, file=sql_file)
        else:
            # get the list of dictionary results for the genotype/allele table query
            ordered_results = []
            if genotype_types:
                genotype_resp_raw = raw_api_call(resp, genotype_payload)
                ordered_results.extend(genotype_resp_raw["results"])

            # get the list of dictionary results for each genotype table only query
            for genotype_only_payload in genotype_only_payloads:
                genotype_only_resp_raw = raw_api_call(resp, genotype_only_payload)
                # add missing keys that are in the allele table part of the genotype/allele table query
                ordered_results.extend(harmonize_germline_results(genotype_only_resp_raw["results"], fields_list))

            if genotype_only_types:
                # get the ref value from the allele table using locus ids
                # ingestion of VCFs lines missing ALT is unsupported so the locus_id will exist in the allele table
                # normalized ref values in the locus_id will match the ref value for missing ALT lines if they
                # were ingested and locus_id could be parsed for the ref value
                ref_payload = get_germline_ref_payload(ordered_results, genotype_payload)
                if ref_payload:
                    locus_id_refs = raw_api_call(resp, ref_payload)
                    update_genotype_only_ref(ordered_results, locus_id_refs)

            if args.infer_ref or args.infer_nocall:
                samples = retrieve_samples(resp, selected_assay_name, selected_assay_id)
                selected_samples = set(filter_dict.get("sample_id", []))
                if selected_samples:
                    samples = list(selected_samples.intersection(samples))
                loci_payload = get_germline_loci_payload(filter_dict["location"], genotype_payload)
                loci = [locus for locus in raw_api_call(resp, loci_payload)["results"]]
                type_to_infer = "ref" if args.infer_ref else "no-call"
                ordered_results = infer_genotype_type(samples, loci, ordered_results, type_to_infer)
                # Filter out not requested genotypes
                if len(types_to_filter_out) > 0:
                    ordered_results = filter_results(ordered_results, "genotype_type", types_to_filter_out)

            ordered_results.sort(key=sort_germline_variant)

            csv_from_json(
                out_file_name=out_file,
                print_to_stdout=print_to_stdout,
                sep="\t",
                raw_results=ordered_results,
                column_names=fields_list,
                quote_char=str("|"),
            )


def retrieve_entities(model):
    """
    Retrieves the entities in form of <entity_name>\t<entity_title> and identifies main entity
    """
    entity_names_and_titles = []
    for entity in sorted(model["entities"].keys()):
        entity_names_and_titles.append(
            "{}\t{}".format(entity, model["entities"][entity]["entity_title"])
        )
        if model["entities"][entity]["is_main_entity"] is True:
            main_entity = entity
    return entity_names_and_titles, main_entity


def list_fields(model, main_entity, args):
    """
    Listing fileds in the model in form at <entity>.<field_name>\t<field_title> for specified list of entities
    """
    present_entities = model["entities"].keys()
    entities_to_list_fields = [model["entities"][main_entity]]
    if args.entities:
        entities_to_list_fields = []
        error_list = []
        for entity in sorted(args.entities.split(",")):
            if entity in present_entities:
                entities_to_list_fields.append(model["entities"][entity])
            else:
                error_list.append(entity)
        if error_list:
            err_exit("The following entity/entities cannot be found: %s" % error_list)
    fields = []
    for entity in entities_to_list_fields:
        for field in sorted(entity["fields"].keys()):
            fields.append(
                "{}.{}\t{}".format(
                    entity["name"], field, entity["fields"][field]["title"]
                )
            )
    print("\n".join(fields))


def csv_from_json(
    out_file_name="",
    print_to_stdout=False,
    sep=",",
    raw_results=[],
    column_names=[],
    quote_char=str('"'),
    quoting=csv.QUOTE_MINIMAL,
):
    if print_to_stdout:
        fields_output = sys.stdout
    else:
        fields_output = open(out_file_name, "w")

    csv_writer = csv.DictWriter(
        fields_output,
        delimiter=str(sep),
        doublequote=True,
        escapechar=None,
        lineterminator="\n",
        quotechar=quote_char,
        quoting=quoting,
        skipinitialspace=False,
        strict=False,
        fieldnames=column_names,
    )
    csv_writer.writeheader()
    for entry in raw_results:
        csv_writer.writerow(entry)

    if not print_to_stdout:
        fields_output.close()


def extract_assay_somatic(args):
    """
    Retrieve the selected data or generate SQL to retrieve the data from an somatic variant assay in a dataset or cohort based on provided rules.
    """
    
    ######## Input combination validation and print help########
    invalid_combo_args = any([args.include_normal_sample, args.additional_fields, args.json_help, args.sql])

    if args.retrieve_meta_info and invalid_combo_args:
        err_exit(
            "The flag, --retrieve-meta-info cannot be used with arguments other than --assay-name, --output."
        )

    if args.list_assays and any([args.assay_name, args.output, invalid_combo_args]):
        err_exit(
            '--list-assays cannot be presented with other options.'
        )

    if args.json_help:
        if any([args.assay_name, args.output, args.include_normal_sample, args.additional_fields, args.sql]):
            err_exit(
                "--json-help cannot be passed with any of --assay-name, --sql, --additional-fields, --additional-fields-help, --output."
            )
        elif args.retrieve_variant is None:
            err_exit("--json-help cannot be passed without --retrieve-variant.")
        else:
            print(
                comment_fill('Filters and respective definitions', comment_string='# ') + '\n#\n' +
                comment_fill('location: "location" filters variants based on having an allele_id which has a corresponding annotation row which matches the supplied "chromosome" with CHROM and where the start position (POS) of the allele_id is between and including the supplied "starting_position" and "ending_position". If multiple values are provided in the list, the conditional search will be, "OR". String match is case sensitive.') + '\n#\n' +
               comment_fill('symbol: "symbol" filters variants based on having an allele_id which has a corresponding annotation row which has a matching symbol (gene) name. If multiple values are provided, the conditional search will be, "OR". For example, ["BRCA2", "ASPM"], will search for variants which match either "BRCA2" or "ASPM". String match is case sensitive.') + '\n#\n' +
               comment_fill('gene: "gene" filters variants based on having an allele_id which has a corresponding annotation row which has a matching gene ID of the variant. If multiple values are provided, the conditional search will be, "OR". For example, ["ENSG00000302118", "ENSG00004000504"], will search for variants which match either "ENSG00000302118" or "ENSG00004000504". String match is case insensitive.') + '\n#\n' +
               comment_fill('feature: "feature" filters variants based on having an allele_id which has a corresponding annotation row which has a matching feature ID. The most common Feature ID is a transcript_id. If multiple values are provided, the conditional search will be, "OR". For example, ["ENST00000302118", "ENST00004000504"], will search for variants which match either "ENST00000302118" or "ENST00004000504". String match is case insensitive.') + '\n#\n' +
               comment_fill('hgvsc: "hgvsc" filters variants based on having an allele_id which has a corresponding annotation row which has a matching HGVSc. If multiple values are provided, the conditional search will be, "OR". For example, ["c.-49A>G", "c.-20T>G"], will search for alleles which match either "c.-49A>G" or "c.-20T>G". String match is case sensitive.') + '\n#\n' +
               comment_fill('hgvsp: "hgvsp" filters variants based on having an allele_id which has a corresponding annotation row which has a matching HGVSp. If multiple values are provided, the conditional search will be, "OR". For example, ["p.Gly2Asp", "p.Aps2Gly"], will search for variants which match either "p.Gly2Asp" or "p.Aps2Gly". String match is case sensitive.') + '\n#\n' +
               comment_fill('allele_id: "allele_id" filters variants based on allele_id match. If multiple values are provided, anymatch will be returned. For example, ["1_1000_A_T", "1_1010_C_T"], will search for allele_ids which match either "1_1000_A_T" or ""1_1010_C_T". String match is case sensitive/exact match.') + '\n#\n' +
               comment_fill('variant_type: Type of allele. Accepted values are "SNP", "INS", "DEL", "DUP", "INV", "CNV", "CNV:TR", "BND", "DUP:TANDEM", "DEL:ME", "INS:ME", "MISSING", "MISSING:DEL", "UNSPECIFIED", "REF" or "OTHER". If multiple values are provided, the conditional search will be, "OR". For example, ["SNP", "INS"], will search for variants which match either "SNP" or ""INS". String match is case insensitive.') + '\n#\n' +
               comment_fill('sample_id: "sample_id" filters either a pair of tumor-normal samples based on having sample_id which has a corresponding sample row which has a matching sample_id. If a user has more than 500 IDs, it is recommended to either retrieve multiple times, or use a cohort id containing all desired individuals, providing the full set of sample_ids.') + '\n#\n' +
               comment_fill('assay_sample_id: "assay_sample_id" filters either a tumor or normal sample based on having an assay_sample_id which has a corresponding sample row which has a matching assay_sample_id. If a user has a list of more than 1,000 IDs, it is recommended to either retrieve multiple times, or use a cohort id containing all desired individuals, providing the full set of assay_sample_ids.') + '\n#\n' +
               comment_fill('JSON filter template for --retrieve-variant', comment_string='# ') + '\n' +
                               '{\n  "location": [\n    {\n      "chromosome": "1",\n      "starting_position": "10000",\n      "ending_position": "20000"\n    },\n    {\n      "chromosome": "X",\n      "starting_position": "500",\n      "ending_position": "1700"\n    }\n  ],\n  "annotation": {\n    "symbol": ["BRCA2"],\n    "gene": ["ENST00000302118"],\n    "feature": ["ENST00000302118.5"],\n    "hgvsc": ["c.-49A>G"],\n    "hgvsp": ["p.Gly2Asp"]\n  },\n  "allele" : {\n    "allele_id":["1_1000_A_T","2_1000_G_C"],\n    "variant_type" : ["SNP", "INS"]\n  },\n  "sample": {\n    "sample_id": ["Sample1", "Sample2"],\n    "assay_sample_id" : ["Sample1_tumt", "Sample1_nor"]\n  }\n}'
            )
            sys.exit(0)

    if args.additional_fields_help:
        if any([args.assay_name, args.output, invalid_combo_args]):
            err_exit(
                '--additional-fields-help cannot be presented with other options.'
            )
        else:
            def print_fields(fields):
                for row in fields:
                    fields_string = "{: <22} {: <22} ".format(*row[:2])
                    width = len(fields_string)
                    fields_string += re.sub('\n', '\n' + ' ' * width, fill(row[2], width_adjustment=-width))
                    print(fields_string)
            print(fill('The following fields will always be returned by default:') + '\n')
            fixed_fields = [['NAME', 'TITLE', 'DESCRIPTION'], 
                            ['assay_sample_id', 'Assay Sample ID', 'A unique identifier for the tumor or normal sample. Populated from the sample columns of the VCF header.'], 
                            ['allele_id', 'Allele ID', 'An unique identification of the allele'], 
                            ['CHROM', 'Chromosome', 'Chromosome of variant, verbatim from original VCF'], 
                            ['POS', 'Position', 'Starting position of variant, verbatim from original VCF'], 
                            ['REF', 'Reference Allele', 'Reference allele of locus, verbatim from original VCF'], 
                            ['allele', 'Allele', 'Sequence of the allele']]
            print_fields(fixed_fields)
            print('\n' + fill('The following fields may be added to the output by using option --additional-fields. If multiple fields are specified, use a comma to separate each entry. For example, "sample_id,tumor_normal"') + '\n')
            additional_fields = [['NAME', 'TITLE', 'DESCRIPTION'], 
                                 ['sample_id', 'Sample ID', 'Unique ID of the pair of tumor-normal samples'], 
                                 ['tumor_normal', 'Tumor-Normal', 'One of ["tumor", "normal"] to describe source sample type'], 
                                 ['ID', 'ID', 'Comma separated list of associated IDs for the variant from the original VCF'], 
                                 ['QUAL', 'QUAL', 'Quality of locus, verbatim from original VCF'], 
                                 ['FILTER', 'FILTER', 'Comma separated list of filters for locus from the original VCF'], 
                                 ['reference_source', 'Reference Source', 'One of ["GRCh37", "GRCh38"] or the allele_sample_id of the respective normal sample'], 
                                 ['variant_type', 'Variant Type', 'The type of allele, with respect to reference'], 
                                 ['symbolic_type', 'Symbolic Type', 'One of ["precise", "imprecise"]. Non-symbolic alleles are always "precise"'], 
                                 ['file_id', 'Source File ID', 'DNAnexus platform file-id of original source file'], 
                                 ['INFO', 'INFO', 'INFO section, verbatim from original VCF'], 
                                 ['FORMAT', 'FORMAT', 'FORMAT section, verbatim from original VCF'], 
                                 ['SYMBOL', 'Symbol', 'A list of gene name associated with the variant'], 
                                 ['GENOTYPE', 'GENOTYPE', 'GENOTYPE section, as described by FORMAT section, verbatim from original VCF'],
                                 ['normal_assay_sample_id', 'Normal Assay Sample ID', 'Assay Sample ID of respective "normal" sample, if exists'],
                                 ['normal_allele_ids', 'Normal Allele IDs', 'Allele ID(s) of respective "normal" sample, if exists'],
                                 ['Gene', 'Gene ID', 'A list of gene IDs, associated with the variant'], 
                                 ['Feature', 'Feature ID', 'A list of feature IDs, associated with the variant'], 
                                 ['HGVSc', 'HGVSc', 'A list of sequence variants in HGVS nomenclature, for DNA'], 
                                 ['HGVSp', 'HGVSp', 'A list of sequence variants in HGVS nomenclature, for protein'], 
                                 ['CLIN_SIG', 'Clinical Significance', 'A list of allele specific clinical significance terms'],
                                 ['ALT', 'ALT', 'Alternate allele(s) at locus, comma separated if more than one, verbatim from original VCF'],
                                 ['alt_index', 'ALT allele_index', 'Order of the allele, as represented in the ALT field. If the allele is missing (i.e, "./0",  "0/." or "./.") then the alt_index will be empty']]
            print_fields(additional_fields)
            sys.exit(0)

    # Validate additional fields

    if args.additional_fields is not None:
        additional_fields_input = [additional_field.strip() for additional_field in args.additional_fields.split(",")]
        accepted_additional_fields = ['sample_id', 'tumor_normal', 'ID', 'QUAL', 'FILTER', 'reference_source', 'variant_type', 'symbolic_type', 'file_id', 'INFO', 'FORMAT', 'SYMBOL', 'GENOTYPE', 'normal_assay_sample_id', 'normal_allele_ids', 'Gene', 'Feature', 'HGVSc', 'HGVSp', 'CLIN_SIG', 'ALT', 'alt_index']
        for field in additional_fields_input:
            if field not in accepted_additional_fields:
                err_exit("One or more of the supplied fields using --additional-fields are invalid. Please run --additional-fields-help for a list of valid fields")
            
    ######## Data Processing ########
    project, entity_result, resp, dataset_project = resolve_validate_record_path(args.path)
    if "CohortBrowser" in resp["recordTypes"] and any([args.list_assays,args.assay_name]):
        err_exit(
            "Currently --assay-name and --list-assays may not be used with a CohortBrowser record (Cohort Object) as input. To select a specific assay or to list assays, please use a Dataset Object as input."
        )
    dataset_id = resp["dataset"]
    rec_descriptor = DXDataset(dataset_id, project=dataset_project).get_descriptor()

    selected_assay_name, selected_assay_id, selected_ref_genome, additional_descriptor_info = get_assay_name_info(
        args.list_assays, args.assay_name, args.path, "somatic", rec_descriptor
    )

    out_file, print_to_stdout = assign_output_method(args, resp["recordName"], "somatic")

    if args.retrieve_meta_info:
        retrieve_meta_info(resp, project, selected_assay_id, selected_assay_name, print_to_stdout, out_file)
        sys.exit(0)

    if args.retrieve_variant:
        filter_dict = json_validation_function("variant", args)
       
        if args.additional_fields:
            payload, fields_list = somatic_final_payload(
                full_input_dict=filter_dict,
                name=selected_assay_name,
                id=selected_assay_id,
                project_context=project,
                genome_reference=selected_ref_genome,
                additional_fields=additional_fields_input,
                include_normal=args.include_normal_sample,
            )
        else:
            payload, fields_list = somatic_final_payload(
                full_input_dict=filter_dict,
                name=selected_assay_name,
                id=selected_assay_id,
                project_context=project,
                genome_reference=selected_ref_genome,
                include_normal=args.include_normal_sample,
            )

        if "CohortBrowser" in resp["recordTypes"]:
            if resp.get("baseSql"):
                payload["base_sql"] = resp.get("baseSql")
            payload["filters"] = resp["filters"]

        #### Run api call to get sql or extract data ####

        if args.sql:
            sql_results = raw_query_api_call(resp, payload)
            if print_to_stdout:
                print(sql_results)
            else:
                with open(out_file, "w") as sql_file:
                    print(sql_results, file=sql_file)
        else:
            resp_raw = raw_api_call(resp, payload)

            csv_from_json(
                out_file_name=out_file,
                print_to_stdout=print_to_stdout,
                sep="\t",
                raw_results=resp_raw["results"],
                column_names=fields_list,
                quote_char=str("\t"),
                quoting=csv.QUOTE_NONE,
            )

def extract_assay_expression(args):
    """
    `dx extract_assay expression`

    Retrieve the selected data or generate SQL to retrieve the data from an expression assay in a dataset or cohort based on provided rules.
    """

    # Validating input arguments (argparse) combinations
    parser_dict = vars(args)
    input_validator = ArgsValidator(
        parser_dict=parser_dict,
        schema=EXTRACT_ASSAY_EXPRESSION_INPUT_ARGS_SCHEMA,
        error_handler=err_exit,
    )
    input_validator.validate_input_combination()

    if args.json_help:
        print(EXTRACT_ASSAY_EXPRESSION_JSON_HELP)
        sys.exit(0)

    if args.additional_fields_help:
        print(EXTRACT_ASSAY_EXPRESSION_ADDITIONAL_FIELDS_HELP)
        sys.exit(0)

    # Resolving `path` argument
    if args.path:
        # entity_result contains `id` and `describe`
        project, folder_path, entity_result = resolve_existing_path(args.path)
        if entity_result is None:
            err_exit(
                'Unable to resolve "{}" to a data object in {}.'.format(
                    args.path, project
                )
            )
        else:
            entity_describe = entity_result.get("describe")

        # Is object in current project?
        if project != entity_result["describe"]["project"]:
            err_exit(
                'Unable to resolve "{}" to a data object or folder name in {}. Please make sure the object is in the selected project.'.format(
                    args.path, project
                )
            )

        # Is object a cohort or a dataset?
        EXPECTED_TYPES = ["Dataset", "CohortBrowser"]
        _record_types = entity_result["describe"]["types"]
        if entity_result["describe"]["class"] != "record" or all(
            x not in _record_types for x in EXPECTED_TYPES
        ):
            err_exit(
                "{} Invalid path. The path must point to a record type of cohort or dataset and not a {} object.".format(
                    entity_result["id"], _record_types
                )
            )

        # Cohort/Dataset handling
        record = DXRecord(entity_describe["id"])
        dataset, cohort_info = Dataset.resolve_cohort_to_dataset(record)

        if cohort_info:
            if args.assay_name or args.list_assays:
                err_exit(
                    'Currently "--assay-name" and "--list-assays" may not be used with a CohortBrowser record (Cohort Object) as input. To select a specific assay or to list assays, please use a Dataset object as input.'
                )
            if float(cohort_info["details"]["version"]) < 3.0:
                err_exit(
                    "{}: Version of the cohort is too old. Version must be at least 3.0.".format(
                        cohort_info["id"]
                    )
                )
            
            BASE_SQL = cohort_info.get("details").get("baseSql")
            COHORT_FILTERS = cohort_info.get("details").get("filters")
            IS_COHORT = True
        else:
            BASE_SQL = None
            COHORT_FILTERS = None
            IS_COHORT = False
            if float(dataset.version) < 3.0:
                err_exit(
                    "{}: Version of the dataset is too old. Version must be at least 3.0.".format(
                        dataset.id
                    )
                )

    if args.list_assays:
        print(*dataset.assay_names_list("molecular_expression"), sep="\n")
        sys.exit(0)

    # Check whether assay_name is valid
    # If no assay_name is provided, the first molecular_expression assay in the dataset must be selected
    if args.assay_name:
        if not dataset.is_assay_name_valid(args.assay_name, "molecular_expression"):
            err_exit(
                "Assay {} does not exist in {}, or the assay name provided cannot be recognized as a molecular expression assay. For valid assays accepted by the function, `extract_assay expression`, please use the --list-assays flag.".format(
                    args.assay_name, dataset.id
                )
            )
    elif not args.assay_name:
        if dataset.assays_info_dict.get("molecular_expression") is None or len(dataset.assays_info_dict.get("molecular_expression")) == 0:
            err_exit("No molecular expression assays found in the dataset")

    # Load args.filter_json or args.filter_json_file into a dict
    if sys.version_info.major == 2:
        # In Python 2 JSONDecodeError does not exist
        json.JSONDecodeError = ValueError

    if args.filter_json:
        try:
            user_filters_json = json.loads(args.filter_json)
        except json.JSONDecodeError as e:
            json_reading_error = (
                "JSON provided for --retrieve-expression is malformatted."
                + "\n"
                + str(e)
            )
            err_exit(json_reading_error)
        except Exception as e:
            err_exit(str(e))

    elif args.filter_json_file:
        try:
            with open(args.filter_json_file) as f:
                user_filters_json = json.load(f)
        except json.JSONDecodeError as e:
            json_reading_error = (
                "JSON provided for --retrieve-expression is malformatted."
                + "\n"
                + str(e)
            )
            err_exit(json_reading_error)
        except OSError as e:
            if "No such file or directory" in str(e):
                err_exit(
                    "JSON file {} provided to --retrieve-expression does not exist".format(
                        e.filename
                    )
                )
            else:
                err_exit(str(e))
        except Exception as e:
            err_exit(str(e))

    if user_filters_json == {}:
        err_exit(
            "No filter JSON is passed with --retrieve-expression or input JSON for --retrieve-expression does not contain valid filter information."
        )

    if args.expression_matrix:
        if "expression" in user_filters_json:
            err_exit(
                'Expression filters are not compatible with --expression-matrix argument. Please remove "expression" from filters JSON.'
            )

        if args.additional_fields:
            err_exit(
                "--additional-fields cannot be used with --expression-matrix argument."
            )

    # Replace 'str' with 'unicode' when checking types in Python 2
    if sys.version_info.major == 2:
        EXTRACT_ASSAY_EXPRESSION_JSON_SCHEMA["location"]["items"]["properties"][
            "chromosome"
        ]["type"] = unicode
        EXTRACT_ASSAY_EXPRESSION_JSON_SCHEMA["location"]["items"]["properties"][
            "starting_position"
        ]["type"] = unicode
        EXTRACT_ASSAY_EXPRESSION_JSON_SCHEMA["location"]["items"]["properties"][
            "ending_position"
        ]["type"] = unicode

    # Validate filters JSON provided by the user according to a predefined schema
    input_json_validator = JSONValidator(
        schema=EXTRACT_ASSAY_EXPRESSION_JSON_SCHEMA, error_handler=err_exit
    )
    input_json_validator.validate(input_json=user_filters_json)

    if "location" in user_filters_json:
        if args.sql:
            EXTRACT_ASSAY_EXPRESSION_FILTERING_CONDITIONS["filtering_conditions"][
                "location"
            ]["max_item_limit"] = None

        else:
            # Genomic range adding together across multiple contigs should be smaller than 250 Mbps
            input_json_validator.are_list_items_within_range(
                input_json=user_filters_json,
                key="location",
                start_subkey="starting_position",
                end_subkey="ending_position",
                window_width=250000000,
                check_each_separately=False,
            )

    input_json_parser = JSONFiltersValidator(
        input_json=user_filters_json,
        schema=EXTRACT_ASSAY_EXPRESSION_FILTERING_CONDITIONS,
        error_handler=err_exit,
    )
    vizserver_raw_filters = input_json_parser.parse()

    _db_columns_list = EXTRACT_ASSAY_EXPRESSION_FILTERING_CONDITIONS[
        "output_fields_mapping"
    ].get("default")

    if args.additional_fields:
        # All three of the following should work:
        # --additional_fields field1 field2
        # --additional_fields field1,field2
        # --additional_fields field1, field2 (note the space char)
        # In the first case, the arg will look like ["field1", "field2"]
        # In the second case: ["field1,field2"]
        # In the third case: ["field1,", "field2"]
        additional_fields = []
        for item in args.additional_fields:
            field = [x.strip() for x in item.split(",") if x.strip()]
            additional_fields.extend(field)

        all_additional_cols = EXTRACT_ASSAY_EXPRESSION_FILTERING_CONDITIONS[
            "output_fields_mapping"
        ].get("additional")
        incorrect_cols = set(additional_fields) - set(
            {k for d in all_additional_cols for k in d.keys()}
        )
        if len(incorrect_cols) != 0:
            err_exit(
                "One or more of the supplied fields using --additional-fields are invalid. Please run --additional-fields-help for a list of valid fields"
            )
        user_additional_cols = [
            i for i in all_additional_cols if set(i.keys()) & set(additional_fields)
        ]
        _db_columns_list.extend(user_additional_cols)

    viz = VizPayloadBuilder(
        project_context=project,
        output_fields_mapping=_db_columns_list,
        filters={"filters": COHORT_FILTERS} if IS_COHORT else None,
        order_by=EXTRACT_ASSAY_EXPRESSION_FILTERING_CONDITIONS["order_by"],
        limit=None,
        base_sql=BASE_SQL,
        is_cohort=IS_COHORT,
        error_handler=err_exit,
    )

    DATASET_DESCRIPTOR = dataset.descriptor_file_dict
    ASSAY_NAME = (
        args.assay_name if args.assay_name else dataset.assay_names_list("molecular_expression")[0]
    )

    if args.assay_name:
        # Assumption: assay names are unique in a dataset descriptor
        # i.e. there are never two assays of the same type with the same name in the same dataset
        for molecular_assay in dataset.assays_info_dict["molecular_expression"]:
            if molecular_assay["name"] == args.assay_name:
                ASSAY_ID = molecular_assay["uuid"]
                break
    else:
        ASSAY_ID = dataset.assays_info_dict["molecular_expression"][0]["uuid"]

    viz.assemble_assay_raw_filters(
        assay_name=ASSAY_NAME, assay_id=ASSAY_ID, filters=vizserver_raw_filters
    )
    vizserver_payload = viz.build()

    # Get the record ID and vizserver URL from the Dataset object
    record_id = dataset.detail_describe["id"]
    url = dataset.vizserver_url

    # Create VizClient object and get data from vizserver using generated payload
    client = VizClient(url, project, err_exit)
    if args.sql:
        vizserver_response = client.get_raw_sql(vizserver_payload, record_id)
    else:
        vizserver_response = client.get_data(vizserver_payload, record_id)

    colnames = None

    # Output is on the "sql" key rather than the "results" key when sql is requested
    output_data = (
        vizserver_response["sql"] if args.sql else vizserver_response["results"]
    )

    # Output data (from vizserver_response["results"]) will be an empty list if no data is returned for the given filters
    if args.expression_matrix and output_data:
        transformed_response, colnames = transform_to_expression_matrix(
            vizserver_response["results"]
        )
        output_data = transformed_response

    if not output_data:
        # write_expression_output expects a list of dicts
        output_data = [{}]

    write_expression_output(
        args.output,
        args.delim,
        args.sql,
        output_data,
        save_uncommon_delim_to_txt=True,
        output_file_name=dataset.detail_describe["name"],
        colnames=colnames,
    )



#### CREATE COHORT ####
def resolve_validate_dx_path(path):
    """
    Resolves dx path into project, folder and name. Fails if non existing folder is provided.
    """
    project, folder, name = resolve_path(path)
    err_msg, folder_exists = None, None
    if folder != "/":
        folder_name = os.path.basename(folder)
        folder_path = os.path.dirname(folder)
        try:
            folder_exists = check_folder_exists(project, folder_path, folder_name)
        except ResolutionError as e:
            if "folder could not be found" in str(e):
                folder_exists = False
            else:
                raise e
        if not folder_exists:
            err_msg = "The folder: {} could not be found in the project: {}".format(
                folder, project
            )
    
    return project, folder, name, err_msg

class VizserverError(Exception):
    pass

def validate_cohort_ids(descriptor, project, resp, ids):
    # Usually the name of the table
    entity_name = descriptor.model["global_primary_key"]["entity"]
    # The name of the column or field in the table
    field_name = descriptor.model["global_primary_key"]["field"] 

    # Get data type of global primay key field
    gpk_type = descriptor.model["entities"][entity_name]["fields"][field_name]["mapping"]["column_sql_type"]
    # Prepare a payload to find entries matching the input ids in the dataset
    if gpk_type in ["integer", "bigint"]:
        if USING_PYTHON2:
            lambda_for_list_conv = lambda a, b: a+[long(b)]
        else:
            lambda_for_list_conv = lambda a, b: a+[int(b)]
    elif gpk_type in ["float", "double"]:
        lambda_for_list_conv = lambda a, b: a+[float(b)]
    elif gpk_type in ["string"]:
        lambda_for_list_conv = lambda a, b: a+[str(b)]
    else:
        err_msg = "Invalid input record. Cohort ID field in the input dataset or cohortbrowser record is of type, {type}. Support is currently only available for Cohort ID fields having one of the following types; string, integer and float".format(type = gpk_type)
        raise ValueError(err_msg) 
    id_list = reduce(lambda_for_list_conv, ids, [])

    entity_field_name = "{}${}".format(entity_name, field_name)
    fields_list = [{field_name: entity_field_name}]

    # Note that pheno filters do not need name or id fields
    payload = {
        "project_context": project,
        "fields": fields_list,
        "filters":{
            "pheno_filters": {
                "filters": {
                    entity_field_name: [
                        {"condition": "in", "values": id_list}
                    ]
                }
            }
        }
    }

    # Use the dxpy raw_api_function to send a POST request to the server with our payload
    try:
        resp_raw = raw_api_call(resp, payload)
    except Exception as exc:
        raise VizserverError(
            "Exception caught while validating cohort ids.  Bad response from Vizserver."
            "Original Error message:\n{}"
        ).format(str(exc))
    # Order of samples doesn't matter so using set here
    discovered_ids = set()
    # Parse the results objects for the cohort ids
    for result in resp_raw["results"]:
        discovered_ids.add(result[field_name])

    # Compare the discovered cohort ids to the user-provided cohort ids
    if discovered_ids != set(id_list):
        # Find which given samples are not present in the dataset
        missing_ids = set(id_list).difference(discovered_ids)
        err_msg = "The following supplied IDs do not match IDs in the main entity of dataset, {dataset_name}: {ids}".format(dataset_name = resp["dataset"], ids = missing_ids)
        raise ValueError(err_msg)
    
    return id_list, lambda_for_list_conv

        
def has_access_level(project, access_level):
    """
    Validates that issuing user has required access level.
    Args:
        project: str: tasked project_id
        access_level: str: minimum requested level
    Retuns: boolean
    """
    level_rank = ["VIEW", "UPLOAD", "CONTRIBUTE", "ADMINISTER"]
    access_level_idx = level_rank.index(access_level)
    try:
        project_describe = describe(project)
    except PermissionDenied:
        return False
    if level_rank.index(project_describe["level"]) < access_level_idx:
        return False
    return True


def validate_project_access(project, access_level="UPLOAD"):
    """
    Validates that project has requested access.
    Args:
        project: str: tasked project_id
        access_level: str: minimum requested level Default at least UPLOAD
    Returns:
        Error message

    """
    if not has_access_level(project, access_level):
        return "At least {} permission is required to create a record in a project".format(
            access_level
        )


def create_cohort(args):
    """
    Create a cohort from dataset/cohort and specified list of samples.
    """
    #### Validation ####
    # Validate and resolve 'PATH' input
    
    # default path values
    path_project = dxpy.WORKSPACE_ID
    path_folder = dxpy.config.get('DX_CLI_WD', '/')
    path_name = None
    if args.PATH:
        path_project, path_folder, path_name, err_msg = resolve_validate_dx_path(
            args.PATH
        )
        if err_msg:
            err_exit(err_msg)
        err_msg = validate_project_access(path_project)
        if err_msg:
            err_exit(err_msg)
    # validate and resolve 'from' input
    FROM = args.__dict__.get("from")
    from_project, entity_result, resp, dataset_project = resolve_validate_record_path(FROM)

    #### Reading input sample ids from file or command line ####
    samples=[]
    # from file
    if args.cohort_ids_file:
        with open(args.cohort_ids_file, "r") as infile:
            for line in infile:
                samples.append(line.strip("\n"))
    # from string
    if args.cohort_ids:
        samples = [id.strip() for id in args.cohort_ids.split(",")]
    
    #### Validate the input cohort IDs ####
    # Get the table/entity and field/column of the dataset from the descriptor
    rec_descriptor = DXDataset(resp["dataset"], project=dataset_project).get_descriptor()

    try:
        list_of_ids, lambda_for_list_conv = validate_cohort_ids(rec_descriptor, dataset_project, resp, samples)
    except ValueError as err:
        err_exit(str(err), expected_exceptions=(ValueError,))
    except VizserverError as err:
        err_exit(str(err), expected_exceptions=(VizserverError,))
    except Exception as err:
        err_exit(str(err))
    # Input cohort IDs have been succesfully validated    

    # converting list of IDs to list of string IDs

    base_sql = resp.get("baseSql", resp.get("base_sql"))
    try:
        raw_cohort_query_payload = cohort_filter_payload(
            list_of_ids,
            rec_descriptor.model["global_primary_key"]["entity"],
            rec_descriptor.model["global_primary_key"]["field"],
            resp.get("filters", {}),
            from_project,
            lambda_for_list_conv,
            base_sql,
        )
    except Exception as e:
        err_exit("{}: {}".format(entity_result["id"], e))
    sql = raw_cohort_query_api_call(resp, raw_cohort_query_payload)
    cohort_payload = cohort_final_payload(
        path_name,
        path_folder,
        path_project,
        resp["databases"],
        resp["dataset"],
        resp["schema"],
        raw_cohort_query_payload["filters"],
        sql,
        base_sql,
        resp.get("combined"),
    )

    dx_record = dxpy.bindings.dxrecord.new_dxrecord(**cohort_payload)
    # print record details to stdout
    if args.brief:
        print(dx_record.get_id())
    else:
        try:
            print_desc(dx_record.describe(incl_properties=True, incl_details=True), args.verbose)
        except Exception as e:
            err_exit(str(e))


class DXDataset(DXRecord):
    """
    A class to handle record objects of type Dataset.
    Inherits from DXRecord, but automatically populates default fields, details and properties.

    Attributes:
        All the same as DXRecord
        name - from record details
        description - from record details
        schema - from record details
        version - from record details
        descriptor - DXDatasetDescriptor object
    Functions
        get_descriptor - calls DXDatasetDescriptor(descriptor_dxfile) if descriptor is None
        get_dictionary - calls descriptor.get_dictionary

    """

    _record_type = "Dataset"

    def __init__(self, dxid=None, project=None):
        super(DXDataset, self).__init__(dxid, project)
        self.describe(default_fields=True, fields={"properties", "details"})
        assert self._record_type in self.types
        assert "descriptor" in self.details
        if is_dxlink(self.details["descriptor"]):
            self.descriptor_dxfile = DXFile(self.details["descriptor"], mode="rb", project=project)
        else:
            err_exit("%s : Invalid cohort or dataset" % self.details["descriptor"])
        self.descriptor = None
        self.name = self.details.get("name")
        self.description = self.details.get("description")
        self.schema = self.details.get("schema")
        self.version = self.details.get("version")

    def get_descriptor(self):
        if self.descriptor is None:
            self.descriptor = DXDatasetDescriptor(
                self.descriptor_dxfile, schema=self.schema
            )
        return self.descriptor

    def get_dictionary(self):
        if self.descriptor is None:
            self.get_descriptor()
        return self.descriptor.get_dictionary()


class DXDatasetDescriptor:
    """
    A class to represent a parsed descriptor of a Dataset record object.

    Attributes
        Representation of JSON object stored in descriptor file
    Functions
        get_dictionary - calls DXDatasetDictionary(descriptor)

    """

    def __init__(self, dxfile, **kwargs):
        python3_5_x = sys.version_info.major == 3 and sys.version_info.minor == 5

        with as_handle(dxfile, is_gzip=True, **kwargs) as f:
            if python3_5_x:
                jsonstr = f.read()
                if type(jsonstr) != str:
                    jsonstr = jsonstr.decode("utf-8")

                obj = json.loads(jsonstr, object_pairs_hook=collections.OrderedDict)
            else:
                obj = json.load(f, object_pairs_hook=collections.OrderedDict)

        for key in obj:
            setattr(self, key, obj[key])
        self.schema = kwargs.get("schema")

    def get_dictionary(self):
        return DXDatasetDictionary(self)


class DXDatasetDictionary:
    """
    A class to represent data, coding and entity dictionaries based on the descriptor.
    All 3 dictionaries will have the same internal representation as dictionaries of string to pandas dataframe.
    Attributes
        data - dictionary of entity name to pandas dataframe representing entity with fields, relationships, etc.
        entity - dictionary of entity name to pandas dataframe representing entity title, etc.
        coding - dictionary of coding name to pandas dataframe representing codes, their hierarchy (if applicable) and their meanings
    """

    def __init__(self, descriptor):
        self.data_dictionary = self.load_data_dictionary(descriptor)
        self.coding_dictionary = self.load_coding_dictionary(descriptor)
        self.entity_dictionary = self.load_entity_dictionary(descriptor)

    def load_data_dictionary(self, descriptor):
        """
        Processes data dictionary from descriptor
        """
        eblocks = collections.OrderedDict()
        join_path_to_entity_field = collections.OrderedDict()
        for entity_name in descriptor.model["entities"]:
            eblocks[entity_name] = self.create_entity_dframe(
                descriptor.model["entities"][entity_name],
                is_primary_entity=(
                    entity_name == descriptor.model["global_primary_key"]["entity"]
                ),
                global_primary_key=(descriptor.model["global_primary_key"]),
            )

            join_path_to_entity_field.update(
                self.get_join_path_to_entity_field_map(
                    descriptor.model["entities"][entity_name]
                )
            )

        edges = []
        for ji in descriptor.join_info:
            skip_edge = False

            for path in [ji["joins"][0]["to"], ji["joins"][0]["from"]]:
                if path not in join_path_to_entity_field:
                    skip_edge = True
                    break

            if not skip_edge:
                edges.append(self.create_edge(ji, join_path_to_entity_field))

        for edge in edges:
            source_eblock = eblocks.get(edge["source_entity"])
            if not source_eblock.empty:
                eb_row_idx = source_eblock["name"] == edge["source_field"]
                if eb_row_idx.sum() != 1:
                    raise ValueError("Invalid edge: " + str(edge))

                ref = source_eblock["referenced_entity_field"].values
                rel = source_eblock["relationship"].values
                ref[eb_row_idx] = "{}:{}".format(
                    edge["destination_entity"], edge["destination_field"]
                )
                rel[eb_row_idx] = edge["relationship"]

                source_eblock = source_eblock.assign(
                    relationship=rel, referenced_entity_field=ref
                )

        return eblocks

    def create_entity_dframe(self, entity, is_primary_entity, global_primary_key):
        """
        Returns DataDictionary pandas DataFrame for an entity.
        """
        required_columns = ["entity", "name", "type", "primary_key_type"]

        extra_cols = [
            "coding_name",
            "concept",
            "description",
            "folder_path",
            "is_multi_select",
            "is_sparse_coding",
            "linkout",
            "longitudinal_axis_type",
            "referenced_entity_field",
            "relationship",
            "title",
            "units",
        ]
        dataset_datatype_dict = {
            "integer": "integer",
            "double": "float",
            "date": "date",
            "datetime": "datetime",
            "string": "string",
        }
        dcols = {col: [] for col in required_columns + extra_cols}
        dcols["entity"] = [entity["name"]] * len(entity["fields"])
        dcols["referenced_entity_field"] = [""] * len(entity["fields"])
        dcols["relationship"] = [""] * len(entity["fields"])

        for field in entity["fields"]:
            # Field-level parameters
            field_dict = entity["fields"][field]
            dcols["name"].append(field_dict["name"])
            dcols["type"].append(dataset_datatype_dict[field_dict["type"]])
            dcols["primary_key_type"].append(
                ("global" if is_primary_entity else "local")
                if (
                    entity["primary_key"]
                    and field_dict["name"] == entity["primary_key"]
                )
                else ""
            )
            # Optional cols to be filled in with blanks regardless
            dcols["coding_name"].append(
                field_dict["coding_name"] if field_dict["coding_name"] else ""
            )
            dcols["concept"].append(field_dict["concept"])
            dcols["description"].append(field_dict["description"])
            dcols["folder_path"].append(
                " > ".join(field_dict["folder_path"])
                if ("folder_path" in field_dict.keys() and field_dict["folder_path"])
                else ""
            )
            dcols["is_multi_select"].append(
                "yes" if field_dict["is_multi_select"] else ""
            )
            dcols["is_sparse_coding"].append(
                "yes" if field_dict["is_sparse_coding"] else ""
            )
            dcols["linkout"].append(field_dict["linkout"])
            dcols["longitudinal_axis_type"].append(
                field_dict["longitudinal_axis_type"]
                if field_dict["longitudinal_axis_type"]
                else ""
            )
            dcols["title"].append(field_dict["title"])
            dcols["units"].append(field_dict["units"])

        try:
            dframe = pd.DataFrame(dcols)
        except ValueError as exc:
            raise exc

        return dframe

    def get_join_path_to_entity_field_map(self, entity):
        """
        Returns map with "database$table$column", "unique_database$table$column",
        as keys and values are (entity, field)
        """
        join_path_to_entity_field = collections.OrderedDict()
        for field in entity["fields"]:
            field_value = entity["fields"][field]["mapping"]
            db_tb_col_path = "{}${}${}".format(
                field_value["database_name"],
                field_value["table"],
                field_value["column"],
            )
            join_path_to_entity_field[db_tb_col_path] = (entity["name"], field)

            if field_value["database_unique_name"] and database_unique_name_regex.match(
                field_value["database_unique_name"]
            ):
                unique_db_tb_col_path = "{}${}${}".format(
                    field_value["database_unique_name"],
                    field_value["table"],
                    field_value["column"],
                )
                join_path_to_entity_field[unique_db_tb_col_path] = (
                    entity["name"],
                    field,
                )
            elif (
                field_value["database_name"]
                and field_value["database_id"]
                and database_id_regex.match(field_value["database_name"])
            ):
                unique_db_name = "{}__{}".format(
                    field_value["database_id"].replace("-", "_").lower(),
                    field_value["database_name"],
                )
                join_path_to_entity_field[unique_db_name] = (entity["name"], field)
        return join_path_to_entity_field

    def create_edge(self, join_info_joins, join_path_to_entity_field):
        """
        Convert an item join_info to an edge. Returns ordereddict.
        """
        edge = collections.OrderedDict()
        column_to = join_info_joins["joins"][0]["to"]
        column_from = join_info_joins["joins"][0]["from"]
        edge["source_entity"], edge["source_field"] = join_path_to_entity_field[
            column_to
        ]
        (
            edge["destination_entity"],
            edge["destination_field"],
        ) = join_path_to_entity_field[column_from]
        edge["relationship"] = join_info_joins["relationship"]
        return edge

    def load_coding_dictionary(self, descriptor):
        """
        Processes coding dictionary from descriptor
        """
        cblocks = collections.OrderedDict()
        for entity in descriptor.model["entities"]:
            for field in descriptor.model["entities"][entity]["fields"]:
                coding_name_value = descriptor.model["entities"][entity]["fields"][
                    field
                ]["coding_name"]
                if coding_name_value and coding_name_value not in cblocks:
                    cblocks[coding_name_value] = self.create_coding_name_dframe(
                        descriptor.model, entity, field, coding_name_value
                    )
        return cblocks

    def create_coding_name_dframe(self, model, entity, field, coding_name_value):
        """
        Returns CodingDictionary pandas DataFrame for a coding_name.
        """
        dcols = {}
        if model["entities"][entity]["fields"][field]["is_hierarchical"]:
            displ_ord = 0

            def unpack_hierarchy(nodes, parent_code, displ_ord):
                """Serialize the node hierarchy by depth-first traversal.

                Yields: tuples of (code, parent_code)
                """
                for node in nodes:
                    if isinstance(node, dict):
                        next_parent_code, child_nodes = next(iter(node.items()))
                        # internal: unpack recursively
                        displ_ord += 1
                        yield next_parent_code, parent_code, displ_ord
                        for deep_node, deep_parent, displ_ord in unpack_hierarchy(
                            child_nodes, next_parent_code, displ_ord
                        ):
                            yield (deep_node, deep_parent, displ_ord)
                    else:
                        # terminal: serialize
                        displ_ord += 1
                        yield (node, parent_code, displ_ord)

            all_codes, parents, displ_ord = zip(
                *unpack_hierarchy(
                    model["codings"][coding_name_value]["display"], "", displ_ord
                )
            )
            dcols.update(
                {
                    "code": all_codes,
                    "parent_code": parents,
                    "meaning": [
                        model["codings"][coding_name_value]["codes_to_meanings"][c]
                        for c in all_codes
                    ],
                    "concept": [
                        model["codings"][coding_name_value]["codes_to_concepts"][c]
                        if (
                            model["codings"][coding_name_value]["codes_to_concepts"]
                            and c
                            in model["codings"][coding_name_value][
                                "codes_to_concepts"
                            ].keys()
                        )
                        else None
                        for c in all_codes
                    ],
                    "display_order": displ_ord,
                }
            )

        else:
            # No hierarchy; just unpack the codes dictionary
            codes, meanings = zip(
                *model["codings"][coding_name_value]["codes_to_meanings"].items()
            )
            if model["codings"][coding_name_value]["codes_to_concepts"]:
                codes, concepts = zip(
                    *model["codings"][coding_name_value]["codes_to_concepts"].items()
                )
            else:
                concepts = [None] * len(codes)
            display_order = [
                int(model["codings"][coding_name_value]["display"].index(c) + 1)
                for c in codes
            ]
            dcols.update(
                {
                    "code": codes,
                    "meaning": meanings,
                    "concept": concepts,
                    "display_order": display_order,
                }
            )

        dcols["coding_name"] = [coding_name_value] * len(dcols["code"])

        try:
            dframe = pd.DataFrame(dcols)
        except ValueError as exc:
            raise exc

        return dframe

    def load_entity_dictionary(self, descriptor):
        """
        Processes entity dictionary from descriptor
        """
        entity_dictionary = collections.OrderedDict()
        for entity_name in descriptor.model["entities"]:
            entity = descriptor.model["entities"][entity_name]
            entity_dictionary[entity_name] = pd.DataFrame.from_dict(
                [
                    {
                        "entity": entity_name,
                        "entity_title": entity.get("entity_title"),
                        "entity_label_singular": entity.get("entity_label_singular"),
                        "entity_label_plural": entity.get("entity_label_plural"),
                        "entity_description": entity.get("entity_description"),
                    }
                ]
            )
        return entity_dictionary

    def write(
        self, output_file_data="", output_file_entity="", output_file_coding="", sep=","
    ):
        """
        Create CSV files with the contents of the dictionaries.
        """
        csv_opts = dict(
            sep=sep,
            header=True,
            index=False,
            na_rep="",
        )

        def sort_dataframe_columns(dframe, required_columns):
            """Sort dataframe columns alphabetically but with `required_columns` first."""
            extra_cols = dframe.columns.difference(required_columns)
            sorted_cols = list(required_columns) + extra_cols.sort_values().tolist()
            return dframe.loc[:, sorted_cols]

        def as_dataframe(ord_dict_of_df, required_columns):
            """Join all blocks into a pandas DataFrame."""
            df = pd.concat([b for b in ord_dict_of_df.values()], sort=False)
            return sort_dataframe_columns(df, required_columns)

        if self.data_dictionary:
            data_dframe = as_dataframe(
                self.data_dictionary,
                required_columns=["entity", "name", "type", "primary_key_type"],
            )
            data_dframe.to_csv(output_file_data, **csv_opts)

        if self.coding_dictionary:
            coding_dframe = as_dataframe(
                self.coding_dictionary,
                required_columns=["coding_name", "code", "meaning"],
            )
            coding_dframe.to_csv(output_file_coding, **csv_opts)

        if self.entity_dictionary:
            entity_dframe = as_dataframe(
                self.entity_dictionary, required_columns=["entity", "entity_title"]
            )
            entity_dframe.to_csv(output_file_entity, **csv_opts)
