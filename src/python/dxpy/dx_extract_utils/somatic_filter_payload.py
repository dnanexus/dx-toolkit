import json
import pprint
import os

from ..exceptions import err_exit, ResourceNotFound
import dxpy

# A dictionary relating the user-facing names of columns to their actual column

column_conversion = {
    "allele_id": "variant_read_optimized$allele_id",
    "variant_type": "variant_read_optimized$variant_type",
    "symbol": "variant_read_optimized$SYMBOL",
    "gene": "variant_read_optimized$Gene",
    "feature": "variant_read_optimized$Feature",
    "hgvsc": "variant_read_optimized$HGVSc",
    "hgvsp": "variant_read_optimized$HGVSp",
    "assay_sample_id": "variant_read_optimized$assay_sample_id",
    "sample_id": "variant_read_optimized$sample_id",
    "tumor_normal": "variant_read_optimized$tumor_normal",
}

column_conditions = {
    "allele_id": "in",
    "variant_type": "in",
    "symbol": "any",
    "gene": "any",
    "feature": "any",
    "hgvsc": "any",
    "hgvsp": "any",
    "assay_sample_id": "in",
    "sample_id": "in",
    "tumor_normal": "is",
}


def basic_filter(
    table, friendly_name, values=[], project_context=None, genome_reference=None
):
    """
    A low-level filter consisting of a dictionary with one key defining the table and column
    and whose value is dictionary defining the user-provided value to be compared to, and the logical operator
    used to do the comparison
    ex.
    {"allele$allele_type": [
        {
            "condition": "in",
            "values": [
                "SNP"
            ]
        }
    ]}
    """
    table = "variant_read_optimized"
    # Get the name of this field in the variant table
    # If the column isn't in the regular fields list, use the friendly name itself as the column name
    # This could be the case when "--additional-fields" flag is used
    filter_key = column_conversion.get(
        friendly_name, "variant_read_optimized${}".format(friendly_name)
    )
    # Handle special cases where values need to be capitalized
    if friendly_name == "variant_type" or friendly_name == "gene" or friendly_name == "feature":
        values = [str(x).upper() for x in values]
    # Get the condition ofr this field
    condition = column_conditions.get(friendly_name, "in")

    listed_filter = {filter_key: [{"condition": condition, "values": values}]}
    return listed_filter


def location_filter(raw_location_list):
    """
    The somatic assay model does not currently support geno bins
    Locations are implemented as filters on chromosome and starting position
    Returns structure of the form:
    {
    "logic":"or",
    "compound":{
        "logic":"and"
        "filters":{
            "variant_read_optimized$CHROM":[
                    {
                    "condition":"is",
                    "values":"12"
                    }
                ]
            },
            "variant_read_optimized$POS":[
                {
                "condition":"greater-than",
                "values":"1000"
                },
                {
                "condition":"less-than",
                "values":"5000"
                }
            ]
        }
    }
    """

    # Location filters are related to each other by "or"
    location_compound = {"compound": [], "logic": "or"}
    for location in raw_location_list:
        # atomic filters within an individual location filters are related by "and"
        indiv_loc_filter = {"filters": {}, "logic": "and"}
        start = int(location["starting_position"])
        end = int(location["ending_position"])
        if end - start > 250000000:
            err_exit(
                "Error in location {}\nLocation filters may not specify regions larger than 250 megabases".format(
                    location
                )
            )
        # First make the chr filter
        indiv_loc_filter["filters"]["variant_read_optimized$CHROM"] = [
            {"condition": "is", "values": location["chromosome"]}
        ]
        # Then the positional filters
        indiv_loc_filter["filters"]["variant_read_optimized$POS"] = [
            {"condition": "greater-than", "values": start},
            {"condition": "less-than", "values": end},
        ]
        location_compound["compound"].append(indiv_loc_filter)

    return location_compound


def generate_assay_filter(
    full_input_dict,
    name,
    id,
    project_context,
    genome_reference=None,
    include_normal=False,
):
    """
    Generate asasy filter consisting of a compound that links the Location filters if present
    to the regular filters
    {
        "assay_filters": {
            "id": "<id>",
            "name":"<name>",
            "compound":[
                {
                    <contents of location compound>
                }
            ]
        }
    }
    """
    assay_filter = {
        "assay_filters": {"name": name, "id": id, "logic": "and", "compound": []}
    }
    basic_filters = {"filters": {}, "logic": "and"}

    for filter_group in full_input_dict.keys():
        if filter_group == "location":
            location_compound = location_filter(full_input_dict["location"])
            assay_filter["assay_filters"]["compound"].append(location_compound)
        else:
            for individual_filter_name in full_input_dict[filter_group].keys():
                indiv_basic_filter = basic_filter(
                    "variant_read_optimized",
                    individual_filter_name,
                    full_input_dict[filter_group][individual_filter_name],
                    project_context,
                    genome_reference,
                )
                basic_filters["filters"].update(indiv_basic_filter)
    # If include_normal is False, then add a filter to select data where tumor_normal = tumor
    if not include_normal:
        tumor_normal_filter = basic_filter(
            "variant_read_optimized",
            "tumor_normal",
            "tumor",
            project_context,
            genome_reference,
        )
        basic_filters["filters"].update(tumor_normal_filter)

    if len(basic_filters["filters"]) > 0:
        assay_filter["assay_filters"]["compound"].append(basic_filters)

    return assay_filter


def somatic_final_payload(
    full_input_dict,
    name,
    id,
    project_context,
    genome_reference=None,
    additional_fields=None,
    include_normal=False,
):
    """
    Assemble the top level payload.  Top level dict contains the project context, fields (return columns),
    and raw filters objects.  This payload is sent in its entirety to the vizserver via an
    HTTPS POST request
    """
    # Generate the assay filter component of the payload
    assay_filter = generate_assay_filter(
        full_input_dict, name, id, project_context, genome_reference, include_normal
    )

    final_payload = {}
    # Set the project context
    final_payload["project_context"] = project_context

    fields = [
        {"assay_sample_id": "variant_read_optimized$assay_sample_id"},
        {"allele_id": "variant_read_optimized$allele_id"},
        {"CHROM": "variant_read_optimized$CHROM"},
        {"POS": "variant_read_optimized$POS"},
        {"REF": "variant_read_optimized$REF"},
        {"allele": "variant_read_optimized$allele"},
    ]

    order_by = [
        {"CHROM":"asc"},
        {"POS":"asc"},
        {"allele_id":"asc"},
        {"assay_sample_id":"asc"}
    ]

    # If the user has specified additional return columns, add them to the payload here
    if additional_fields:
        for add_field in additional_fields:
            fields.append(
                {"{}".format(add_field): "variant_read_optimized${}".format(add_field)}
            )

    final_payload["fields"] = fields
    final_payload["order_by"] = order_by
    final_payload["raw_filters"] = assay_filter
    final_payload["distinct"] = True

    field_names = []
    for f in fields:
        field_names.append(list(f.keys())[0])

    return final_payload, field_names
