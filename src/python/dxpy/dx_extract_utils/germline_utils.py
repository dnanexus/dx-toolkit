from __future__ import annotations
import json
import os
import re

from .filter_to_payload import extract_utils_basepath
from .input_validation import GENOTYPE_TYPES


GENOTYPE_ONLY_TYPES = (
    "ref",
    "no-call",
)

SELECT_LIST_REGEX = r"SELECT\s+(DISTINCT\s+)?(.+?)\s+FROM"
NAMED_EXPRESSION_REGEX = r"(`?(\w+)?`?\.)?`?(\w+)`?( AS `?(\w+)`?)?"


def get_genotype_types(filter_dict):
    """
    Returns a list of genotype types for the filter that are use in genotype and allele table queries.
    """

    genotype_types = filter_dict.get("genotype_type") or GENOTYPE_TYPES
    return [genotype_type for genotype_type in genotype_types if genotype_type not in GENOTYPE_ONLY_TYPES]


def get_genotype_only_types(filter_dict, exclude_refdata, exclude_halfref, exclude_nocall):
    """
    Returns a list of genotype types for the filter that are use in genotype table only queries.
    """

    genotype_type_exclude_flag_map = {
        "ref": exclude_refdata,
        "half": exclude_halfref,
        "no-call": exclude_nocall,
    }
    genotype_types = filter_dict.get("genotype_type") or list(genotype_type_exclude_flag_map)
    genotype_only_types = []
    for genotype_type, excluded in genotype_type_exclude_flag_map.items():
        if genotype_type in genotype_types and excluded is not None and not excluded:
            genotype_only_types.append(genotype_type)

    return genotype_only_types


def get_types_to_filter_out_when_infering(
    requested_types: list[str],
) -> list:
    """
    Infer option require all genotypes types to be queried.
    If users wishes to obtain only certain types of genotypes,
    reminder of the types should be filtered out post querying
    """
    return [
        type
        for type in ["ref", "het-ref", "hom", "het-alt", "half", "no-call"]
        if type not in requested_types
    ] if requested_types else []


def add_germline_base_sql(resp, payload):
    if "CohortBrowser" in resp["recordTypes"]:
        if resp.get("baseSql"):
            payload["base_sql"] = resp.get("baseSql")
        payload["filters"] = resp["filters"]


def sort_germline_variant(d):
    if "allele_id" in d and d["allele_id"]:
        chrom, pos, _, alt = d["allele_id"].split("_")
    elif "locus_id" in d and d["locus_id"]:
        chrom, pos = d["locus_id"].split("_")[:2]
        alt = ""
    sample_id = d.get("sample_id", "")
    if chrom.isdigit():
        return int(chrom), "", int(pos), alt, sample_id
    return float("inf"), chrom, int(pos), alt, sample_id


def _parse_sql_select_named_expression(sql):
    """
    Parses the SELECT list of a SQL statement and returns a generator of named expressions table, column, and alias.
    """
    select_list_match = re.match(SELECT_LIST_REGEX, sql).group(2)
    for named_expression in [x.strip() for x in select_list_match.split(",")]:
        named_expression_match = re.match(NAMED_EXPRESSION_REGEX, named_expression).groups()
        yield named_expression_match[1], named_expression_match[2], named_expression_match[4]


def _harmonize_sql_select_named_expression(sql, return_columns, **kwargs):
    """
    Harmonizes the SELECT list of a SQL statement to include all columns in return_columns. NULL values are used for
    columns not in the SELECT list.
    """
    select_info = {alias: (table, column) for table, column, alias in _parse_sql_select_named_expression(sql)}

    select_lists = []
    for return_column in return_columns:
        return_column = tuple(return_column)[0]
        if return_column in select_info:
            table, column = select_info[return_column]
            select_lists.append("`{table}`.`{column}` AS `{return_column}`".format(table=table,
                                                                                   column=column,
                                                                                   return_column=return_column))
        elif return_column in kwargs:
            select_lists.append("`{table}`.`{column}` AS `ref`".format(table=kwargs[return_column][0],
                                                                       column=kwargs[return_column][1]))
        else:
            select_lists.append("NULL AS `{return_column}`".format(return_column=return_column))
    select_list = ", ".join(select_lists)

    distinct = re.match(SELECT_LIST_REGEX, sql).group(1)
    return re.sub(
        SELECT_LIST_REGEX,
        "SELECT {distinct}{select_list} FROM".format(distinct=distinct, select_list=select_list),
        sql,
    )


def harmonize_germline_sql(sql):
    """
    Harmonize the SQL statement for genotype table only queries to include columns to UNION with genotype and allele
    table queries. JOIN genotype table to allele table on locus_id to include ref columns values.
    """
    with open(os.path.join(extract_utils_basepath, "return_columns_genotype.json")) as infile:
        genotype_return_columns = json.load(infile)

    # join genotype table to allele table on locus_id
    join_condition_regex = r"ON\s+`(\w+)`\.`(a_id)`\s+=\s+`(\w+)`\.`(a_id)`\s+WHERE"
    allele_table = re.search(join_condition_regex, sql).group(2)
    sql = re.sub(join_condition_regex, "ON `\\1`.`locus_id` = `\\3`.`locus_id` WHERE", sql)

    # Add return columns missing in SQL statement to the SELECT list as NULL values
    sql = _harmonize_sql_select_named_expression(sql, genotype_return_columns, ref=(allele_table, "a_id"))

    return sql


def harmonize_germline_results(results, fields_list):
    """
    Harmonizes raw query results to include all columns in fields_list. Columns not in fields_list have value None.
    """
    harmonized_results = []
    for result in results:
        harmonized_result = {}
        for field in fields_list:
            if field in result:
                harmonized_result[field] = result[field]
            else:
                harmonized_result[field] = None
        harmonized_results.append(harmonized_result)
    return harmonized_results


def get_germline_ref_payload(results, genotype_payload):
    """
    Create a payload to query locus_id/ref pairs from the allele table for genotypes missing ref column values.
    """
    locus_ids = set((r["locus_id"], r["chromosome"], r["starting_position"]) for r in results if r["ref"] is None)
    allele_filters = []
    if not locus_ids:
        return None
    for locus_id, chr, pos in locus_ids:
        allele_filters.append({
            "condition": "in",
            "values": [locus_id],
            "geno_bins": [{"chr": chr, "start": pos, "end": pos}],
        })
    return {
        "project_context": genotype_payload["project_context"],
        "adjust_geno_bins": False,
        "distinct": True,
        "logic": "and",
        "fields": [
            {"locus_id": "allele$locus_id"},
            {"ref": "allele$ref"},
        ],
        "is_cohort": False,
        "raw_filters": {
            "assay_filters": {
                "id": genotype_payload["raw_filters"]["assay_filters"]["id"],
                "name": genotype_payload["raw_filters"]["assay_filters"]["name"],
                "filters": {
                    "allele$a_id": allele_filters,
                },
            },
        },
    }


def update_genotype_only_ref(results, locus_id_refs):
    """
    Update genotype results with ref column values from locus_id/ref query result
    """
    locus_id_ref_map = {result["locus_id"]: result["ref"] for result in locus_id_refs["results"]}
    for result in results:
        if result["ref"] is not None:
            continue
        result["ref"] = locus_id_ref_map[result["locus_id"]]


def get_germline_loci_payload(locations, genotype_payload):
    """
    Create a payload to query locus ids from the allele table with a location filter
    """
    return {
        "project_context": genotype_payload["project_context"],
        "adjust_geno_bins": False,
        "distinct": True,
        "logic": "and",
        "fields": [
            {"locus_id": "allele$locus_id"},
            {"chromosome": "allele$chr"},
            {"starting_position": "allele$pos"},
            {"ref": "allele$ref"},
        ],
        "is_cohort": False,
        "raw_filters": {
            "assay_filters": {
                "id": genotype_payload["raw_filters"]["assay_filters"]["id"],
                "name": genotype_payload["raw_filters"]["assay_filters"]["name"],
                "filters": {
                    "allele$a_id": [{
                        "condition": "in",
                        "values": [],
                        "geno_bins": [
                            {
                                "chr": location["chromosome"],
                                "start": location["starting_position"],
                                "end": location["starting_position"]
                            } for location in locations
                        ],
                    }],
                },
            },
        },
    }


def _produce_loci_dict(loci: list[dict], results_entries: list[dict]) -> dict:
    """
    Produces a dictionary with locus_id as key and a set of samples and entry as value.
    """
    loci_dict = {}
    for locus in loci:
        loci_dict[locus["locus_id"]] = {
            "samples": set(),
            "entry": {
                "allele_id": None,
                "locus_id": locus["locus_id"],
                "chromosome": locus["chromosome"],
                "starting_position": locus["starting_position"],
                "ref": locus["ref"],
                "alt": None,
            },
        }

    for entry in results_entries:
        loci_dict[entry["locus_id"]]["samples"].add(entry["sample_id"])

    return loci_dict


def infer_genotype_type(
    samples: list, loci: list[dict], result_entries: list[dict], type_to_infer: str
) -> list[dict]:
    """
    If the result_entries does not contain entry with sample_id of specifific starting_position the the genotype type is either no-call or ref.
    Args:
        samples: list of all samples
        loci: list of information on each loci within the filter  e.g.
            {
            "locus_id": "1_1076145_A_T",
            "chromosome": "1",
            "starting_position": 1076145,
            "ref": "A",
            }
        result_entries: list of results from extract_assay query. e.g.
            {
            "sample_id": "SAMPLE_2",
            "allele_id": "1_1076145_A_AT",
            "locus_id": "1_1076145_A_T",
            "chromosome": "1",
            "starting_position": 1076145,
            "ref": "A",
            "alt": "AT",
            "genotype_type": "het-alt",
            }
        type_to_infer: type to infer either  "ref" or "no-call"
    Returns: list of infered entries with added inferred genotype type and other entries retrieved from result for loci of interest.
    """
    loci_dict = _produce_loci_dict(loci, result_entries)
    inferred_entries = []
    for locus in loci_dict:
        for sample in samples:
            if sample not in loci_dict[locus]["samples"]:
                inferred_entries.append(
                    {
                        "sample_id": sample,
                        **loci_dict[locus]["entry"],
                        "genotype_type": type_to_infer,
                    }
                )
    return result_entries + inferred_entries


def filter_results(
    results: list[dict], key: str, restricted_values: list
) -> list[dict]:
    """
    Filters results by key and restricted_values.
    Args:
        results: list of results from extract_assay query. e.g.
            {
            "sample_id": "SAMPLE_2",
            "allele_id": "1_1076145_A_AT",
            "locus_id": "1_1076145_A_T",
            "chromosome": "1",
            "starting_position": 1076145,
            "ref": "A",
            "alt": "AT",
            "genotype_type": "het-alt",
            }
        key: key to filter by
        restricted_values: list of values to filter by
    Returns: list of filtered entries
    """
    return [entry for entry in results if entry[key] not in restricted_values]

