import json
import argparse
import dxpy

# from ..utils.resolver import resolve_existing_path
# from ..cli.dataset_utilities import DXDataset


# A dictionary relating the fields in each input file to the table that they
# need to filter data in
# As of now, the allele and annotation files get all their data from the allele and
# annotation tables respectively, only the sample file references data in multiple tables

with open("assets/file_to_table.json", "r") as infile:
    file_to_table = json.load(infile)

# A dictionary relating the user-facing names of columns to their actual column
# names in the CLIGAM tables
with open("assets/column_conversion.json", "r") as infile:
    column_conversion = json.load(infile)

# A dictionary relating the user-facing names of columns to the condition that needs
# to be applied in the basic filter for the column
with open("assets/column_conditions.json", "r") as infile:
    column_conditions = json.load(infile)


def BasicFilter(table, friendly_name, values):
    column_name = column_conversion[table][friendly_name]
    condition = column_conditions[table][friendly_name]
    filter_key = "{}${}".format(table, column_name)
    if condition == "between":
        values = [float(values["min"]), float(values["max"])]
    if condition == "less-than" or condition == "greater-than":
        values = int(values)

    # Check if we need to add geno bins as well
    # This is only necessary for gene_id and a_id.  For rsid the vizserver calculates it itself
    if column_name == "gene_id" or column_name == "gene_name":
        listed_filter = {
            filter_key: [
                {
                    "condition": condition,
                    "values": values,
                    "geno_bins": [{"chr": "3", "start": 1000, "end": 2000}],
                }
            ]
        }
    else:
        listed_filter = {filter_key: [{"condition": condition, "values": values}]}
    return listed_filter


def LocationFilter(table, location):
    # A location filter has two atomic filters with the exact same key.  Thus, the two filters
    # need to be in the list under the table$column key
    filter_group = BasicFilter(
        table,
        "chromosome",
        location["chromosome"],
    )
    pos_key = "{}$pos".format(table)
    # first handle starting_position
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
    # filter_type = allele,annotation, sample

    # There are three possible types of input JSON: a sample filter, an allele filter,
    # and an annotation filter
    filters_dict = {}
    table = filter_type

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
    final_filter_dict["assay_filters"]["compound"].append(location_compound)
    final_filter_dict["assay_filters"]["logic"] = "and"

    return final_filter_dict


def FinalPayload(assay_filter, project_context, filter_type):
    final_payload = {}
    final_payload["project_context"] = project_context
    # This might be set automatically depending on whether the raw or raw-query
    # API is selected, if not we need to set it here
    # final_payload["stat"] = "raw"

    # Section for defining returned columns for each of the three filter types

    if filter_type == "allele":
        order_by = [{"allele_id": "asc"}]
        with open("assets/allele_return_columns.json", "r") as infile:
            fields = json.load(infile)
    elif filter_type == "annotation":
        order_by = [{"allele_id": "asc"}]
        with open("assets/annotation_return_columns.json", "r") as infile:
            fields = json.load(infile)
    elif filter_type == "sample":
        order_by = [{"sample_id": "asc"}]
        with open("assets/sample_return_columns.json", "r") as infile:
            fields = json.load(infile)

    final_payload["fields"] = fields
    final_payload["order_by"] = order_by
    final_payload["filters"] = assay_filter
    final_payload["validate_geno_bins"] = True
    return final_payload


if __name__ == "__main__":
    # Temporarily hardcode some variables that we will eventually get from the command line
    json_path = "/Users/jmulka@dnanexus.com/Development/dx-toolkit/src/python/dxpy/genomic_assay_model/test_input/allele_filter.json"
    output_file = "test_output/final_payload.json"

    filter_type = "allele"
    record_id = "record-FyFPyz0071F54Zjb32vG82Gj"
    project_context = "project-FkyXg38071F1vGy2GyXyYYQB"

    # project_context, path, entity_result = resolve_existing_path(
    #    "{}:{}".format(project_id, record_id)
    # )
    # rec_descriptor = DXDataset(record_id, project=project_id).get_descriptor()
    # rec_dict = rec_descriptor.get_dictionary()

    # Name and ID, and context will come from the descriptor, which is processed upstream of this script
    name = "veo_demo_dataset_assay"
    id = "da6a4ffc-7571-4b2f-853d-445460a18396"

    with open(json_path, "r") as infile:
        full_input_dict = json.load(infile)

    assay_filter = GenerateAssayFilter(full_input_dict, name, id, filter_type)

    final_payload = FinalPayload(assay_filter, project_context, filter_type)

    with open(output_file, "w") as outfile:
        json.dump(final_payload, outfile)
