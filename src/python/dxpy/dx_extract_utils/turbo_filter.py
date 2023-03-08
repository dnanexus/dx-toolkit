import json
from jsonschema import validate
from ..exceptions import err_exit
from ..cli.dataset_utilities import retrieve_geno_bins
import argparse

# A dictionary relating the fields in each input file to the table that they
# need to filter data in
# As of now, the allele and annotation files get all their data from the allele and
# annotation tables respectively, only the sample file references data in multiple tables

# TODO remove this for now and place somewhere else
with open("file_to_table.json", "r") as infile:
    file_to_table = json.load(infile)

# A dictionary relating the user-facing names of columns to their actual column
# names in the CLIGAM tables
with open("column_conversion.json", "r") as infile:
    column_conversion = json.load(infile)

# A dictionary relating the user-facing names of columns to the condition that needs
# to be applied in the basic filter for the column
with open("column_conditions.json", "r") as infile:
    column_conditions = json.load(infile)


def BasicFilter(table, friendly_name, values):
    # A low-level filter consisting of a dictionary with one key defining the table and column
    # and values defining the user-provided value to be compared to, and the logical operator
    # used to do the comparison

    column_name = column_conversion[table][friendly_name]
    condition = column_conditions[table][friendly_name]
    filter_key = "{}${}".format(table, column_name)
    if condition == "between":
        min_val = float(values["min"])
        max_val = float(values["max"])
        if min_val > max_val:
            print(
                "min value greater than max value for filter {}".format(friendly_name)
            )
            raise err_exit
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
                        values, args.project_context, "GRCh38.92"
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


def GenerateAssayFilter(full_input_dict, name, id, filter_type):
    # Generate the entire assay filters object by reading the filter JSON, making the relevant
    # Basic and Location filters, and creating the structure that relates them logically

    # There are three possible types of input JSON: a sample filter, an allele filter,
    # and an annotation filter
    filters_dict = {}
    table = filter_type
    location_compound = None

    for key in full_input_dict.keys():
        # Override the table name if we are working with a sample filter, as this filter
        # hits multiple table
        if filter_type == "sample":
            table = file_to_table[filter_type][key]

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
                    )
                )
    final_filter_dict = {"assay_filters": {"name": name, "id": id, "compound": []}}

    final_filter_dict["assay_filters"]["compound"].append({"filters": filters_dict})
    final_filter_dict["assay_filters"]["compound"][0]["logic"] = "and"
    if location_compound:
        final_filter_dict["assay_filters"]["compound"].append(location_compound)
    final_filter_dict["assay_filters"]["logic"] = "and"

    return final_filter_dict


def FinalPayload(full_input_dict, name, id, project_context, filter_type):

    # First, ensure that the JSON is valid
    ValidateJSON(full_input_dict, filter_type)
    # Second, generate the assay filter component of the payload
    assay_filter = GenerateAssayFilter(full_input_dict, name, id, filter_type)

    final_payload = {}
    final_payload["project_context"] = project_context
    # This might be set automatically depending on whether the raw or raw-query
    # API is selected, if not we need to set it here
    # final_payload["stat"] = "raw"

    # Section for defining returned columns for each of the three filter types

    if filter_type == "allele":
        order_by = [{"allele_id": "asc"}]
        with open("return_columns_allele.json", "r") as infile:
            fields = json.load(infile)
    elif filter_type == "annotation":
        order_by = [{"allele_id": "asc"}]
        with open("return_columns_annotation.json", "r") as infile:
            fields = json.load(infile)
    elif filter_type == "sample":
        order_by = [{"sample_id": "asc"}]
        with open("return_columns_sample.json", "r") as infile:
            fields = json.load(infile)

    final_payload["fields"] = fields
    final_payload["order_by"] = order_by
    final_payload["raw_filters"] = assay_filter
    final_payload["validate_geno_bins"] = True
    return final_payload


def ValidateJSON(filter, type):
    # Check JSON against schema
    # Errors out if JSON is invalid, continues otherwise
    schema_file = "retrieve_{}_schema.json".format(type)
    with open(schema_file, "r") as infile:
        json_schema = json.load(infile)

    try:
        validate(full_input_dict, json_schema)
        print("JSON file {} is valid".format(filter))
    except Exception as inst:
        print(inst)
        raise err_exit


if __name__ == "__main__":
    # Temporarily hardcode some variables that we will eventually get from the command line
    # args.filter = "/Users/jmulka@dnanexus.com/Development/dx-toolkit/src/python/dxpy/genomic_assay_model/test_input/unit_tests/allele_01.json"
    # output_file = "test_output/allele_01_output.json"
    # filter_type = "allele"
    # project_context = "project-FkyXg38071F1vGy2GyXyYYQB"
    # Name and ID, and context will come from the descriptor, which is processed upstream of this script
    # name = "veo_demo_dataset_assay"
    # id = "da6a4ffc-7571-4b2f-853d-445460a18396"

    parser = argparse.ArgumentParser()
    parser.add_argument("--filter", help="path to filter JSON file", required=True)
    parser.add_argument("--output", help="path to output file", required=True)
    parser.add_argument(
        "--type",
        help="type of filter being applied",
        choices=["allele", "annotation", "sample"],
        required=True,
    )
    parser.add_argument(
        "--project-context",
        help="project ID of parent project of record",
        required=True,
    )
    parser.add_argument("--name", help="name of assay", required=True)
    parser.add_argument("--id", help="ID of assay", required=True)

    args = parser.parse_args()

    with open(args.filter, "r") as infile:
        full_input_dict = json.load(infile)

    final_payload = FinalPayload(
        full_input_dict, args.name, args.id, args.project_context, args.type
    )

    with open(args.output, "w") as outfile:
        json.dump(final_payload, outfile)
