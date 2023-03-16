import json

from jsonschema import validate
from ..exceptions import err_exit, ResourceNotFound
import argparse
import os
import dxpy
import subprocess

asset_basepath = os.path.join(os.path.dirname(dxpy.__file__), "dx_extract_utils")

# A dictionary relating the user-facing names of columns to their actual column
# names in the CLIGAM tables
with open(os.path.join(asset_basepath, "column_conversion.json"), "r") as infile:
    column_conversion = json.load(infile)

# A dictionary relating the user-facing names of columns to the condition that needs
# to be applied in the basic filter for the column
with open(os.path.join(asset_basepath, "column_conditions.json"), "r") as infile:
    column_conditions = json.load(infile)


def retrieve_geno_bins(list_of_genes, project, genome_reference):
    project_desc = dxpy.describe(project)
    geno_positions = []

    try:
        with open(
            os.path.join(asset_basepath, "Homo_sapiens_genes_manifest.json"),
            "r",
        ) as geno_bin_manifest:
            r = json.load(geno_bin_manifest)
        dxpy.describe(r[genome_reference][project_desc["region"]])
    except ResourceNotFound:
        with open(
            os.path.join(asset_basepath, "Homo_sapiens_genes_manifest_staging.json"),
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
    table, friendly_name, values, project_context=None, genome_reference=None
):
    # A low-level filter consisting of a dictionary with one key defining the table and column
    # and values defining the user-provided value to be compared to, and the logical operator
    # used to do the comparison

    # Allele and annotation filters only reference the allele and annotation tables respectively
    # but the genotype filter references the genotype table and the allele table
    # If the table is genotype, check if we need to replace it with the allele table
    if table == "genotype" and friendly_name == "allele_id":
        table = "allele"

    column_name = column_conversion[table][friendly_name]
    condition = column_conditions[table][friendly_name]
    filter_key = "{}${}".format(table, column_name)
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

    # Check if we need to add geno bins as well
    # This is only necessary for gene_id and a_id.  For rsid the vizserver calculates it itself
    if column_name == "gene_id" or column_name == "gene_name":
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


def LocationFilter(table, location):
    # A location filter has two atomic filters for pos with the exact same key.  Thus, the two filters
    # need to be in the list under the table$column key.  There is a third filter defining the chromosome
    # The three basic filters in a location filter are related to each other by "and"
    filter_group = BasicFilter(
        table,
        "chromosome",
        location["chromosome"],
    )
    pos_key = "{}$pos".format(table)
    # Note that the starting and ending filter objects are both values within the same allele$pos key
    starting_filter = {
        "condition": column_conditions[table]["starting_position"],
        "values": int(location["starting_position"]),
    }
    ending_filter = {
        "condition": column_conditions[table]["ending_position"],
        "values": int(location["ending_position"]),
    }
    filter_group[pos_key] = [starting_filter, ending_filter]
    location_filter = {"filters": filter_group}
    location_filter["logic"] = "and"
    return location_filter


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

    # There are three possible types of input JSON: a sample filter, an allele filter,
    # and an annotation filter
    filters_dict = {}
    table = filter_type
    location_compound = None

    for key in full_input_dict.keys():
        # Location needs to be handled slightly differently
        if key == "location":
            location_compound = {"compound": []}
            location_list = full_input_dict["location"]
            grouped_location_filter = []
            for location in location_list:
                # The grouped filters object consisting of up to three atomic filters
                location_filter = LocationFilter(table, location)
                grouped_location_filter.append(location_filter)

            location_compound["compound"] = grouped_location_filter
            location_compound["logic"] = "or"
            location_compound["name"] = "location"

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
    final_filter_dict["assay_filters"]["compound"][0]["logic"] = "and"
    if location_compound:
        final_filter_dict["assay_filters"]["compound"].append(location_compound)
    final_filter_dict["assay_filters"]["logic"] = "and"

    return final_filter_dict


def FinalPayload(
    full_input_dict, name, id, project_context, genome_reference, filter_type, sql_flag
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
            os.path.join(asset_basepath, "return_columns_allele.json"), "r"
        ) as infile:
            fields = json.load(infile)
    elif filter_type == "annotation":
        order_by = [{"allele_id": "asc"}]
        with open(
            os.path.join(asset_basepath, "return_columns_annotation.json"), "r"
        ) as infile:
            fields = json.load(infile)
    elif filter_type == "genotype":
        order_by = [{"sample_id": "asc"}]
        with open(
            os.path.join(asset_basepath, "return_columns_genotype.json"), "r"
        ) as infile:
            fields = json.load(infile)

    final_payload["fields"] = fields
    final_payload["order_by"] = order_by
    final_payload["raw_filters"] = assay_filter
    return final_payload


def ValidateJSON(filter, type, sql_flag=False):
    # Check JSON against schema
    # Errors out if JSON is invalid, continues otherwise

    # If the sql flag is given, versions of the allele and annotation schema that do not have required fields
    # must be used
    if sql_flag and (filter == "allele" or filter == "annotation"):
        schema_file = "retrieve_{}_schema_sql.json".format(type)
    else:
        schema_file = "retrieve_{}_schema.json".format(type)

    # Open the schema asset
    with open(schema_file, "r") as infile:
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
