import json
import argparse
import dxpy

# from ..utils.resolver import resolve_existing_path
# from ..cli.dataset_utilities import DXDataset


# Create a dictionary relating the fields in each input file to the table that they
# need to filter data in
# As of now, the allele and annotation files get all their data from the allele and
# annotation tables respectively, only the sample file references data in multiple tables
file_to_table = {}
file_to_table["sample"] = {}
file_to_table["sample"]["allele_id"] = "allele"
file_to_table["sample"]["sample_id"] = "sample"
file_to_table["sample"]["genotype"] = "genotype"


# Create a dictionary for converting the friendly names used by the user to the
# columns as they appear in the GAM tables
column_conversion = {
    "sample": {},
    "allele": {},
    "genotype": {},
    "annotation": {},
}

column_conversion["sample"] = {"sample_id": "sample_id"}

column_conversion["allele"] = {
    "allele_id": "a_id",
    "id": "a_id",
    "rsid": "dbsnp151_rsid",
    "type": "allele_type",
    "ref": "ref",
    "dataset_alt_af": "alt_freq",
    "gnomad_alt_af": "Gnomad201_alt_freq",
    "chromosome": "chr",
    "starting_position": "pos",
    "ending_position": "pos",
}
column_conversion["genotype"] = {"genotype": "type"}

column_conversion["annotation"] = {
    "allele_id": "allele",
    "gene_name": "gene_name",
    "gene_id": "gene_id",
    "transcript_id": "feature_id",
    "feature_id": "feature_id",
    "consequences": "effect",
    "putative_impact": "putative_impact",
    "hgvs_c": "hgcs_c",
    "hgvs_p": "hgvs_p",
}


# Create a dictionary for determining the correct condition to use with each field
column_conditions = {
    "sample": {},
    "allele": {},
    "genotype": {},
    "location": {},
    "annotation": {},
}

column_conditions["sample"]["sample_id"] = "in"

column_conditions["allele"] = {}
column_conditions["allele"]["allele_id"] = "in"
column_conditions["allele"]["id"] = "in"
column_conditions["allele"]["rsid"] = "in"
column_conditions["allele"]["type"] = "in"
column_conditions["allele"]["ref"] = "in"
column_conditions["allele"]["dataset_alt_af"] = "between"
column_conditions["allele"]["gnomad_alt_af"] = "between"
# For use by location blocks
column_conditions["allele"]["chromosome"] = "is"
column_conditions["allele"]["starting_position"] = "greater-than"
column_conditions["allele"]["ending_position"] = "less-than"

column_conditions["genotype"] = {}
column_conditions["genotype"]["genotype"] = "in"


column_conditions["annotation"] = {}
column_conditions["annotation"]["allele_id"] = "in"
column_conditions["annotation"]["gene_name"] = "in"
column_conditions["annotation"]["gene_id"] = "in"
column_conditions["annotation"]["transcript_id"] = "in"
column_conditions["annotation"]["feature_id"] = "in"
column_conditions["annotation"]["consequences"] = "in"
column_conditions["annotation"]["putative_impact"] = "in"
column_conditions["annotation"]["hgvs_c"] = "in"
column_conditions["annotation"]["hgvs_p"] = "in"


def BasicFilter(table, friendly_name, values):
    column_name = column_conversion[table][friendly_name]
    condition = column_conditions[table][friendly_name]
    filter_key = "{}${}".format(table, column_name)
    if condition == "between":
        values = [float(values["min"]), float(values["max"])]
    if condition == "less-than" or condition == "greater-than":
        values = int(values)
    listed_filter = {filter_key: [{"condition": condition, "values": values}]}
    return listed_filter


def LocationFilter(table, location):
    # A location filter has two atomic filters with the exact same key.  Thus, the two filters
    # need to be in the list under the table$column key
    chr_filter = BasicFilter(
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
    position_filter = {pos_key: [starting_filter, ending_filter]}
    location_filter = {"filters": position_filter}
    location_filter["filters"]["logic"] = "and"
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
            location_compound["compound"][0]["logic"] = "or"
            location_compound["compound"][0]["name"] = "allele location"

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
    final_filter_dict["assay_filters"]["compound"][0]["filters"]["logic"] = "and"
    final_filter_dict["assay_filters"]["compound"].append(location_compound)
    final_filter_dict["assay_filters"]["compound"].append({"logic": "and"})

    return final_filter_dict


def FinalPayload(assay_filter, project_context, filter_type):
    final_payload = {}
    final_payload["project_context"] = project_context
    final_payload["stat"] = "raw"

    # Section for defining returned columns for each of the three filter types

    if filter_type == "allele":
        order_by = [{"allele$allele_id": "asc"}]
        with open("assets/allele_return_columns.json", "r") as infile:
            fields = json.load(infile)
    elif filter_type == "annotation":
        order_by = [{"annotation$a_id": "asc"}]
        with open("assets/annotation_return_columns.json", "r") as infile:
            fields = json.load(infile)
    elif filter_type == "sample":
        order_by = [{"sample$sample_id": "asc"}]
        with open("assets/sample_return_columns.json", "r") as infile:
            fields = json.load(infile)

    final_payload["fields"] = fields
    final_payload["order_by"] = order_by

    final_payload["filters"] = assay_filter
    return final_payload


if __name__ == "__main__":
    # Temporarily hardcode some variables that we will eventually get from the command line
    json_path = "/Users/jmulka@dnanexus.com/Development/dx-toolkit/src/python/dxpy/genomic_assay_model/test_input/allele_filter.json"
    output_file = "test_output/final_payload.json"

    filter_type = "allele"
    record_id = "record-FyFPyz0071F54Zjb32vG82Gj"
    project_id = "project-FkyXg38071F1vGy2GyXyYYQB"

    # project_context, path, entity_result = resolve_existing_path(
    #    "{}:{}".format(project_id, record_id)
    # )
    # rec_descriptor = DXDataset(record_id, project=project_id).get_descriptor()
    # rec_dict = rec_descriptor.get_dictionary()

    # Name and ID, and context will come from the descriptor, which is processed upstream of this script
    name = "testname"
    id = "testid"
    project_context = "context"

    with open(json_path, "r") as infile:
        full_input_dict = json.load(infile)

    assay_filter = GenerateAssayFilter(full_input_dict, name, id, filter_type)

    final_payload = FinalPayload(assay_filter, project_context, filter_type)

    with open(output_file, "w") as outfile:
        json.dump(final_payload, outfile)
