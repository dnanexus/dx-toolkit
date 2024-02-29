import json
import os
import re

from .filter_to_payload import extract_utils_basepath


def get_genotype_only_types(filter_dict, exclude_refdata, exclude_halfref, exclude_nocall):
    if "alleld_id" in filter_dict:
        return []

    genotype_type_exclude_flag_map = {
        "ref": exclude_refdata,
        "half": exclude_halfref,
        "no-call": exclude_nocall,
    }
    genotype_types = filter_dict.get("genotype_type") or list(genotype_type_exclude_flag_map)
    genotype_only_types = []
    for genotype_type, excluded in genotype_type_exclude_flag_map.items():
        if genotype_type in genotype_types and not excluded:
            genotype_only_types.append(genotype_type)

    return genotype_only_types


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


def massage_germline_sql(sql):
    with open(os.path.join(extract_utils_basepath, "return_columns_genotype.json")) as infile:
        genotype_return_columns = json.load(infile)

    select_list_regex = r"SELECT\s+(DISTINCT\s+)?(.+?)\s+FROM"
    select_list_match = re.match(select_list_regex, sql).group(2)
    named_expressions = [x.strip() for x in select_list_match.split(",")]
    named_expression_regex = r"(`?(\w+)?`?\.)?`?(\w+)`?( AS `?(\w+)`?)?"
    select_info = {}
    for named_expression in named_expressions:
        named_expression_match = re.match(named_expression_regex, named_expression).groups()
        table, column, alias = (
            named_expression_match[1], named_expression_match[2], named_expression_match[4])
        select_info[alias] = (table, column)

    join_condition_re = r"ON\s+`(\w+)`\.`(a_id)`\s+=\s+`(\w+)`\.`(a_id)`\s+WHERE"
    sql = re.sub(join_condition_re, "ON `\\1`.`locus_id` = `\\3`.`locus_id` WHERE", sql)

    select_lists = []
    for genotype_return_column in genotype_return_columns:
        genotype_return_column = tuple(genotype_return_column)[0]
        if genotype_return_column in select_info:
            table, column = select_info[genotype_return_column]
            select_lists.append("`{table}`.`{column}` AS `{genotype_return_column}`".format(
                table=table, column=column, genotype_return_column=genotype_return_column))
        elif genotype_return_column == "ref":
            locus_id = "`{}`.`{}`".format(*select_info["locus_id"])
            # FIXME: this is normREF, not REF
            select_lists.append("SPLIT({locus_id}, \"_\")[2] AS `ref`".format(locus_id=locus_id))
        else:
            select_lists.append("NULL AS `{genotype_return_column}`".format(
                genotype_return_column=genotype_return_column))
    select_list = ", ".join(select_lists)

    return re.sub(select_list_regex, "SELECT {select_list} FROM".format(select_list=select_list), sql)


def massage_germline_results(results, fields_list):
    massaged_results = []
    for result in results:
        massaged_result = {}
        for field in fields_list:
            if field in result:
                massaged_result[field] = result[field]
            else:
                massaged_result[field] = None
        massaged_results.append(massaged_result)
    return massaged_results


def get_germline_ref_payload(results, genotype_payload):
    locus_ids = set((r["locus_id"], r["chromosome"], r["starting_position"]) for r in results if r["ref"] is None)
    allele_filters = []
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
    locus_id_ref_map = {result["locus_id"]: result["ref"] for result in locus_id_refs["results"]}
    for result in results:
        if result["ref"] is not None:
            continue
        result["ref"] = locus_id_ref_map[result["locus_id"]]
