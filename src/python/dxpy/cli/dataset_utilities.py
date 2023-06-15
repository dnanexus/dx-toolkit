#!/usr/bin/env python
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

from __future__ import print_function, unicode_literals, division, absolute_import

import sys
import collections
import json
import os
import re
import csv
import dxpy
import codecs
import subprocess
from ..utils.printing import fill
from ..bindings import DXRecord
from ..bindings.dxdataobject_functions import is_dxlink
from ..bindings.dxfile import DXFile
from ..utils.resolver import resolve_existing_path, is_hashid, ResolutionError
from ..utils.file_handle import as_handle
from ..exceptions import (
    err_exit,
    PermissionDenied,
    InvalidInput,
    InvalidState,
    ResourceNotFound,
    default_expected_exceptions,
)

from ..dx_extract_utils.filter_to_payload import validate_JSON, final_payload

database_unique_name_regex = re.compile("^database_\w{24}__\w+$")
database_id_regex = re.compile("^database-\\w{24}$")


def resolve_validate_path(path):
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
            "%s : Invalid path. The path must point to a record type of cohort or dataset"
            % entity_result["describe"]["class"]
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


def raw_query_api_call(resp, payload):
    resource_val = resp["url"] + "/viz-query/3.0/" + resp["dataset"] + "/raw-query"
    try:
        resp_raw_query = dxpy.DXHTTPRequest(
            resource=resource_val, data=payload, prepend_srv=False
        )

    except Exception as details:
        err_exit(str(details))
    sql_results = resp_raw_query["sql"] + ";"
    return sql_results


def raw_api_call(resp, payload):
    resource_val = resp["url"] + "/data/3.0/" + resp["dataset"] + "/raw"
    try:
        resp_raw = dxpy.DXHTTPRequest(
            resource=resource_val, data=payload, prepend_srv=False
        )
        if "error" in resp_raw.keys():
            if resp_raw["error"]["type"] == "InvalidInput":
                print("Insufficient permissions due to the project policy.")
                print(resp_raw["error"]["message"])
            elif resp_raw["error"]["type"] == "QueryTimeOut":
                print(resp_raw["error"]["message"])
                print(
                    "Please consider using `--sql` option to generate the SQL query and query via a private compute cluster."
                )
            else:
                print(resp_raw["error"])
            sys.exit(1)
    except Exception as details:
        err_exit(str(details))
    return resp_raw


def extract_dataset(args):
    """
    Retrieves the data or generates SQL to retrieve the data from a dataset or cohort for a set of entity.fields. Additionally, the dataset’s dictionary can be extracted independently or in conjunction with data.
    """
    if (
        not args.dump_dataset_dictionary
        and not args.list_fields
        and not args.list_entities
        and args.fields is None
    ):
        err_exit(
            "Must provide at least one of the following options: --fields, --dump-dataset-dictionary, --list-fields, --list-entities"
        )

    listing_restricted = {
        "dump_dataset_dictionary": False,
        "sql": False,
        "fields": None,
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

    project, entity_result, resp, dataset_project = resolve_validate_path(args.path)

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

    if args.fields:
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
    if args.fields is not None:
        fields_list = "".join(args.fields).split(",")
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
        err_exit("`--sql` passed without `--fields`")

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

    #### Check if the retrieve options are passed correctly, print help if needed ####
    if args.retrieve_allele:
        if args.json_help:
            print(
                '# Filters and respective definitions\n#\n#  rsid: rsID associated with an allele or set of alleles. If multiple values are provided, the conditional search will be, "OR." For example, ["rs1111", "rs2222"], will search for alleles which match either "rs1111" or "rs2222". String match is case sensitive.\n#  type: Type of allele. Accepted values are "SNP", "Ins", "Del", "Mixed". If multiple values are provided, the conditional search will be, "OR." For example, ["SNP", "Ins"], will search for variants which match either "SNP" or "Ins". String match is case sensitive.\n#  dataset_alt_af: Dataset alternate allele frequency, a json object with empty content or two sets of key/value pair: {min: 0.1, max:0.5}. Accepted numeric value for each key is between and including 0 and 1.  If a user does not want to apply this filter but still wants this information in the output, an empty json object should be provided.\n#  gnomad_alt_af: gnomAD alternate allele frequency. a json object with empty content or two sets of key/value pair: {min: 0.1, max:0.5}. Accepted value for each key is between 0 and 1. If a user does not want to apply this filter but still wants this information in the output, an empty json object should be provided.\n#  location: Genomic range in the reference genome where the starting position of alleles fall into. If multiple values are provided in the list, the conditional search will be, "OR." String match is case sensitive.\n# JSON filter template for --retrieve-allele\n{\n  "rsid": ["rs11111", "rs22222"],\n  "type": ["SNP", "Del", "Ins"],\n  "dataset_alt_af": {"min": 0.001, "max": 0.05},\n  "gnomad_alt_af": {"min": 0.001, "max": 0.05},\n  "location": [\n    {\n      "chromosome": "1",\n      "starting_position": "10000",\n      "ending_position": "20000"\n    },\n    {\n      "chromosome": "X",\n      "starting_position": "500",\n      "ending_position": "1700"\n    }\n  ]\n}'
            )
            sys.exit(0)
    elif args.retrieve_annotation:
        if args.json_help:
            print(
                '# Filters and respective definitions\n#\n#  allele_id: ID of an allele for which annotations should be returned. If multiple values are provided, annotations for any alleles that match one of the values specified will be listed. For example, ["1_1000_A_T", "1_1010_C_T"], will search for annotations of alleles which match either "1_1000_A_T" or ""1_1010_C_T". String match is case insensitive.\n#  gene_name: Gene name of the annotation. A list of gene names whose annotations will be returned. If multiple values are provided, the conditional search will be, "OR." For example, ["BRCA2", "ASPM"], will search for annotations which match either "BRCA2" or "ASPM". String match is case insensitive.\n#  gene_id: Ensembl gene ID (ENSG) of the annotation. If multiple values are provided, the conditional search will be, "OR." For example, ["ENSG00000302118", "ENSG00004000504"], will search for annotations which match either "ENSG00000302118" or "ENSG00004000504". String match is case insensitive.\n#  feature_id: Ensembl feature id (ENST) where the range overlaps with the variant. Currently, only  coding transcript IDs are searched. If multiple values are provided, the conditional search will be, "OR." For example, ["ENST00000302118.5", "ENST00004000504.1"], will search for annotations which match either "ENST00000302118.5" or "ENST00004000504.1". String match is case insensitive.\n#  consequences: Consequence as recorded in the annotation. If multiple values are provided, the conditional search will be, "OR." For example, ["5_prime_UTR_variant", "3_prime_UTR_variant"], will search for annotations which match either "5 prime UTR variant" or "3 prime UTR variant". String match is case sensitive. For all supported consequences terms, please refer to snpeff: http://pcingola.github.io/SnpEff/se_inputoutput/#effect-prediction-details (Effect Seq. Ontology column). This filter cannot be specified by itself, and must be included with at least one of the following filters: "gene_id", "gene_name",or "feature_id".\n#  putative_impact: Putative impact as recorded in the annotation. Possible values are [ "HIGH", "MODERATE", "LOW", "MODIFIER"]. If multiple values are provided, the conditional search will be, "OR." For example, ["MODIFIER", "HIGH"], will search for annotations which match either "MODIFIER" or "HIGH". String match is case insensitive. For all supported terms, please refer to snpeff: http://pcingola.github.io/SnpEff/se_inputoutput/#impact-prediction. This filter cannot be specified by itself, and must be included with at least one of the following filters: "gene_id", "gene_name", or "transcript_id".\n#  hgvs_c: HGVS (DNA) code of the annotation. If multiple values are provided, the conditional search will be, "OR." For example, ["c.-49A>G", "c.-20T>G"], will search for annotations which match either "c.-49A>G" or "c.-20T>G". String match is case sensitive.\n#  hgvs_p: HGVS (Protein) code of the annotation. If multiple values are provided, the conditional search will be, "OR." For example, ["p.Gly2Asp", "p.Aps2Gly"], will search for annotations which match either "p.Gly2Asp" or "p.Aps2Gly". String match is case sensitive.\n# JSON filter template for --retrieve-annotation\n{\n  "allele_id":["1_1000_A_T","2_1000_G_C"],\n  "gene_name": ["BRCA2"],\n  "gene_id": ["ENST00000302118"],\n  "feature_id": ["ENST00000302118.5"],\n  "consequences": ["5 prime UTR variant"],\n  "putative_impact": ["MODIFIER"],\n  "hgvs_c": ["c.-49A>G"],\n  "hgvs_p": ["p.Gly2Asp"]\n}'
            )
            sys.exit(0)
    elif args.retrieve_genotype:
        if args.json_help:
            print(
                '# Filters and respective definitions\n#  allele_id: ID(s) of one or more alleles for which sample genotypes will be returned. If multiple values are provided, any samples having at least one allele that match any of the values specified will be listed. For example, ["1_1000_A_T", "1_1010_C_T"], will search for samples with at least one allele matching either "1_1000_A_T" or "1_1010_C_T". String match is case insensitive.\n#  sample_id: Optional, one or more sample IDs for which sample genotypes will be returned. If the provided object is a cohort, this further intersects the sample ids. If a user has a list of samples more than 1,000, it is recommended to use a cohort id containing all the samples.\n#  genotype_type: Optional, one or more genotype types for which sample genotype types will be returned. One of: hom-alt (homozygous for the non-ref allele), het-ref (heterozygous with a ref allele and alt allele), het-alt (heterozygous with two distinct alt alleles), half (only one alt allele is known, second allele is unknown).\n# JSON filter template for --retrieve-genotype\n{\n  "sample_id": ["s1", "s2"],\n  "allele_id": ["1_1000_A_T","2_1000_G_C"],\n  "genotype_type": ["het-ref", "hom-alt"]\n}'
            )
            sys.exit(0)

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

        validate_JSON(filter, filter_type)

        return filter

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
    project, entity_result, resp, dataset_project = resolve_validate_path(args.path)
    if "CohortBrowser" in resp["recordTypes"] and any([args.list_assays,args.assay_name]):
            err_exit(
                "Currently --assay-name and --list-assays may not be used with a CohortBrowser record (Cohort Object) as input. To select a specific assay or to list assays, please use a Dataset Object as input."
            )
    dataset_id = resp["dataset"]
    rec_descriptor = DXDataset(dataset_id, project=dataset_project).get_descriptor()

    #### Get names of genetic assays ####
    if args.list_assays:
        (geno_assays, other_assays) = get_assay_info(
            rec_descriptor, assay_type="genetic_variant"
        )
        if not geno_assays:
            err_exit("There's no genetic assay in the dataset provided.")
        else:
            for a in geno_assays:
                print(a["name"])
            sys.exit(0)

    #### Decide which assay is to be queried and which ref genome is to be used ####
    (geno_assays, other_assays) = get_assay_info(
        rec_descriptor, assay_type="genetic_variant"
    )
    geno_assay_names = [ga["name"] for ga in geno_assays]
    geno_assay_ids = [ga["uuid"] for ga in geno_assays]
    other_assay_names = [oa["name"] for oa in other_assays]
    other_assay_ids = [oa["uuid"] for oa in other_assays]
    selected_assay_name = geno_assay_names[0]
    selected_assay_id = geno_assay_ids[0]
    if args.assay_name:
        if args.assay_name not in list(geno_assay_names):
            if args.assay_name in list(other_assay_names):
                err_exit(
                    "This is not a valid assay. For valid assays accepted by the function, `extract_assay germline`, please use the --list-assays flag."
                )
            else:
                err_exit(
                    "Assay {assay_name} does not exist in the {path}.".format(
                        assay_name=args.assay_name, path=args.path
                    )
                )
        else:
            selected_assay_name = args.assay_name
            for ga in geno_assays:
                if ga["name"] == args.assay_name:
                    selected_assay_id = ga["uuid"]

    selected_ref_genome = "GRCh38.92"
    for a in geno_assays:
        if a["name"] == selected_assay_name and a["reference_genome"]:
            selected_ref_genome = a["reference_genome"]["name"]

    #### Decide output method based on --output and --sql ####
    if args.sql:
        file_name_suffix = ".data.sql"
    else:
        file_name_suffix = ".tsv"
    file_already_exist = []
    files_to_check = []
    out_file = ""

    print_to_stdout = False
    if args.output is None:
        out_directory = os.getcwd()
        out_file = os.path.join(out_directory, resp["recordName"] + file_name_suffix)
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

    payload = {}
    if args.retrieve_allele:
        payload, fields_list = final_payload(
            full_input_dict=filter_dict,
            name=selected_assay_name,
            id=selected_assay_id,
            project_context=project,
            genome_reference=selected_ref_genome,
            filter_type="allele",
        )
    elif args.retrieve_annotation:
        payload, fields_list = final_payload(
            full_input_dict=filter_dict,
            name=selected_assay_name,
            id=selected_assay_id,
            project_context=project,
            genome_reference=selected_ref_genome,
            filter_type="annotation",
        )
    elif args.retrieve_genotype:
        payload, fields_list = final_payload(
            full_input_dict=filter_dict,
            name=selected_assay_name,
            id=selected_assay_id,
            project_context=project,
            genome_reference=selected_ref_genome,
            filter_type="genotype",
        )

    if "CohortBrowser" in resp["recordTypes"]:
        if resp.get("baseSql"):
            payload["base_sql"] = resp.get("baseSql")
        payload["filters"] = resp["filters"]

    #### Run api call to get sql or extract data ####
    if filter_given:
        if args.sql:
            sql_results = raw_query_api_call(resp, payload)
            if args.retrieve_genotype:
                geno_table = re.search(
                    r"\bgenotype_alt_read_optimized\w+", sql_results
                ).group()
                substr = "`" + geno_table + "`.`type`"
                sql_results = sql_results.replace(
                    substr, "REPLACE(`" + geno_table + "`.`type`, 'hom', 'hom-alt')", 1
                )

            if print_to_stdout:
                print(sql_results)
            else:
                with open(out_file, "w") as sql_file:
                    print(sql_results, file=sql_file)
        else:
            resp_raw = raw_api_call(resp, payload)
            if args.retrieve_genotype:
                for r in resp_raw["results"]:
                    if r["genotype_type"] == "hom":
                        r["genotype_type"] = "hom-alt"

            csv_from_json(
                out_file_name=out_file,
                print_to_stdout=print_to_stdout,
                sep="\t",
                raw_results=resp_raw["results"],
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
        quoting=csv.QUOTE_MINIMAL,
        skipinitialspace=False,
        strict=False,
        fieldnames=column_names,
    )
    csv_writer.writeheader()
    for entry in raw_results:
        csv_writer.writerow(entry)

    if not print_to_stdout:
        fields_output.close()


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
            self.descriptor_dxfile = DXFile(self.details["descriptor"], mode="rb")
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
