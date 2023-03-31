import json

from jsonschema import validate
from ..exceptions import err_exit, ResourceNotFound
import argparse
import os
import dxpy
import subprocess

extract_utils_basepath = os.path.join(
    os.path.dirname(dxpy.__file__), "dx_extract_utils"
)

# A dictionary relating the user-facing names of columns to their actual column
# names in the CLIGAM tables
with open(
    os.path.join(extract_utils_basepath, "column_conversion.json"), "r"
) as infile:
    column_conversion = json.load(infile)

# A dictionary relating the user-facing names of columns to the condition that needs
# to be applied in the basic filter for the column
with open(
    os.path.join(extract_utils_basepath, "column_conditions.json"), "r"
) as infile:
    column_conditions = json.load(infile)


def retrieve_geno_bins(list_of_genes, project, genome_reference):
    project_desc = dxpy.describe(project)
    geno_positions = []

    try:
        with open(
            os.path.join(extract_utils_basepath, "Homo_sapiens_genes_manifest.json"),
            "r",
        ) as geno_bin_manifest:
            r = json.load(geno_bin_manifest)
        dxpy.describe(r[genome_reference][project_desc["region"]])
    except ResourceNotFound:
        with open(
            os.path.join(
                extract_utils_basepath, "Homo_sapiens_genes_manifest_staging.json"
            ),
            "r",
        ) as geno_bin_manifest:
            r = json.load(geno_bin_manifest)

    geno_bins = subprocess.check_output(
        ["dx", "cat", r[genome_reference][project_desc["region"]]]
    )
    geno_bins_json = json.loads(geno_bins)
    invalid_genes = []

    for gene in list_of_genes:
        bin = geno_bins_json.get(gene)
        if bin is None:
            invalid_genes.append(gene)
        else:
            bin.pop("strand")
            geno_positions.append(bin)

    if invalid_genes:
        err_exit("Following gene names or IDs are invalid: %r" % invalid_genes)

    return geno_positions


def BasicFilter(
    table, friendly_name, values=[], project_context=None, genome_reference=None
):
    # A low-level filter consisting of a dictionary with one key defining the table and column
    # and values defining the user-provided value to be compared to, and the logical operator
    # used to do the comparison

    filter_key = column_conversion[table][friendly_name]
    condition = column_conditions[table][friendly_name]

    # Input validation.  Check that the user hasn't provided an invalid min/max in any fields
    if condition == "between":
        min_val = float(values["min"])
        max_val = float(values["max"])
        if min_val > max_val:
            err_exit(
                "min value greater than max value for filter {}".format(friendly_name)
            )

        values = [min_val, max_val]
    if condition == "less-than" or condition == "greater-than":
        values = int(values)

    # Check for special cases where the user-input values need to be changed before creating payload
    # Case 1: genotype filter, genotype_type field, hom changes to hom-alt
    if table == "genotype" and friendly_name == "genotype_type":
        values = [x if x != "hom-alt" else "hom" for x in values]
    # Case 2: Some fields need to be changed to upper case
    if friendly_name in [
        "allele_id",
        "gene_name",
        "gene_id",
        "feature_id",
        "putative_impact",
    ]:
        values = [x.upper() for x in values]

    # Check if we need to add geno bins as well
    # This is only necessary for gene_id and a_id.  For rsid the vizserver calculates it itself
    if friendly_name == "gene_id" or friendly_name == "gene_name":
        genome_reference = "GRCh38.92"
        listed_filter = {
            filter_key: [
                {
                    "condition": condition,
                    "values": values,
                    "geno_bins": retrieve_geno_bins(
                        values, project_context, genome_reference
                    ),
                }
            ]
        }
    else:
        listed_filter = {filter_key: [{"condition": condition, "values": values}]}
    return listed_filter


def LocationFilter(location_list):
    # A location filter is actually an allele$a_id filter with no filter values
    # The geno_bins perform the actual location filtering
    # On the raw_filters route, the items within the geno_bins list are related by "or"

    location_aid_filter = {
        "allele$a_id": [
            {
                "condition": "in",
                "values": [],
                "geno_bins": [],
            }
        ]
    }

    for location in location_list:
        # First, ensure that the geno bins width isn't greater than 250 megabases
        start = int(location["starting_position"])
        end = int(location["ending_position"])
        if end - start > 250000000:
            err_exit(
                "Error in location {}\nLocation filters may not specify regions larger than 250 megabases".format(
                    location
                )
            )

        location_aid_filter["allele$a_id"][0]["geno_bins"].append(
            {
                "chr": location["chromosome"],
                "start": start,
                "end": end,
            }
        )
    return location_aid_filter


def GenerateAssayFilter(
    full_input_dict,
    name,
    id,
    project_context,
    genome_reference,
    filter_type,
):
    # Generate the entire assay filters object by reading the filter JSON, making the relevant
    # Basic and Location filters, and creating the structure that relates them logically

    # There are three possible types of input JSON: a genotype filter, an allele filter,
    # and an annotation filter
    filters_dict = {}
    table = filter_type

    location_aid_filter = None
    for key in full_input_dict.keys():
        # Location needs to be handled slightly differently
        if key == "location":
            location_list = full_input_dict["location"]
            location_aid_filter = LocationFilter(location_list)

        else:
            if not (full_input_dict[key] == "*" or full_input_dict[key] == None):
                filters_dict.update(
                    BasicFilter(
                        table,
                        key,
                        full_input_dict[key],
                        project_context,
                        genome_reference,
                    )
                )
    final_filter_dict = {"assay_filters": {"name": name, "id": id, "compound": []}}

    # Additional structure of the payload
    final_filter_dict["assay_filters"]["compound"].append({"filters": filters_dict})
    # The general filters are related by "and"
    final_filter_dict["assay_filters"]["compound"][0]["logic"] = "and"
    # Add the location filter as a second part of the compound if it exists
    if location_aid_filter:
        final_filter_dict["assay_filters"]["compound"].append(
            {"filters": location_aid_filter}
        )
        # The location filter is related to the general filters by "and"
        final_filter_dict["assay_filters"]["logic"] = "and"

    return final_filter_dict


def FinalPayload(
    full_input_dict, name, id, project_context, genome_reference, filter_type
):

    # First, ensure that the JSON is valid
    # ValidateJSON(full_input_dict, filter_type, sql_flag)
    # Second, generate the assay filter component of the payload
    assay_filter = GenerateAssayFilter(
        full_input_dict,
        name,
        id,
        project_context,
        genome_reference,
        filter_type,
    )

    final_payload = {}
    final_payload["project_context"] = project_context
    # This might be set automatically depending on whether the raw or raw-query
    # API is selected, if not we need to set it here

    # Section for defining returned columns for each of the three filter types

    if filter_type == "allele":
        order_by = [{"allele_id": "asc"}]
        with open(
            os.path.join(extract_utils_basepath, "return_columns_allele.json"), "r"
        ) as infile:
            fields = json.load(infile)
    elif filter_type == "annotation":
        order_by = [{"allele_id": "asc"}]
        with open(
            os.path.join(extract_utils_basepath, "return_columns_annotation.json"), "r"
        ) as infile:
            fields = json.load(infile)
    elif filter_type == "genotype":
        order_by = [{"sample_id": "asc"}]
        with open(
            os.path.join(extract_utils_basepath, "return_columns_genotype.json"), "r"
        ) as infile:
            fields = json.load(infile)

    final_payload["fields"] = fields
    final_payload["order_by"] = order_by
    final_payload["adjust_geno_bins"] = False
    final_payload["raw_filters"] = assay_filter
    field_names = []
    for f in fields:
        field_names.append(list(f.keys())[0])

    # TODO remove this
    print(final_payload)
    return final_payload, field_names


def ValidateJSON(filter, type):
    # Check JSON against schema
    # Errors out if JSON is invalid, continues otherwise

    schema_file = "retrieve_{}_schema.json".format(type)

    # Open the schema asset
    with open(os.path.join(extract_utils_basepath, schema_file), "r") as infile:
        json_schema = json.load(infile)

    # The jsonschema validation function will error out if the schema is invalid.  The error message will contain
    # an explanation of which part of the schema failed
    try:
        # pass
        validate(filter, json_schema)
    except Exception as inst:
        err_exit(inst.message)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--filter", help="path to filter JSON file", required=True)
    parser.add_argument("--output", help="path to output file", required=True)
    parser.add_argument(
        "--type",
        help="type of filter being applied",
        choices=["allele", "annotation", "genotype"],
        required=True,
    )
    parser.add_argument(
        "--project-context",
        help="project ID of parent project of record",
        required=True,
    )
    parser.add_argument("--name", help="name of assay", required=True)
    parser.add_argument("--id", help="ID of assay", required=True)
    parser.add_argument("--reference", help="genome reference", default="GRCh38.92")
    parser.add_argument(
        "--sql_flag",
        help="set to true if intention is to use raw-query which only returns sql",
        default=False,
    )

    args = parser.parse_args()

    with open(args.filter, "r") as infile:
        full_input_dict = json.load(infile)

    final_payload = FinalPayload(
        full_input_dict,
        args.name,
        args.id,
        args.project_context,
        args.reference,
        args.type,
        args.sql_flag,
    )

    with open(args.output, "w") as outfile:
        json.dump(final_payload, outfile)
