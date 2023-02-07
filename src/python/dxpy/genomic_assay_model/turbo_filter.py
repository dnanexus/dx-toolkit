import json
import argparse

# Temporarily hardcode some variables that we will eventually get from the command line
json_path = "/Users/jmulka@dnanexus.com/Development/dx-toolkit/src/python/dxpy/genomic_assay_model/test_input/allele_filter.json"
output_file = "test_output/sample_output.json"
name = "testname"
id = "testid"

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


def AtomicFilter(table, friendly_name, condition, values):
    column_name = column_conversion[table][friendly_name]
    filter_key = "{}${}".format(table, column_name)
    if condition == "between":
        values = [float(values["min"]), float(values["max"])]
    if condition == "less-than" or condition == "greater-than":
        values = int(values)
    listed_filter = [{filter_key: {"condition": condition, "values": values}}]
    return listed_filter


if __name__ == "__main__":

    with open(json_path, "r") as infile:
        full_input_dict = json.load(infile)

    # There are three possible types of input JSON: a sample filter, an allele filter,
    # and an annotation filter

    filter_file = "allele"
    filters_dict = {}
    table = filter_file

    for key in full_input_dict.keys():
        # Override the table name if we are working with a sample filter, as this filter
        # hits multiple table
        if filter_file == "sample":
            table = file_to_table[filter_file][key]

        # Location needs to be handled slightly differently
        if key == "location":
            location_list = full_input_dict["location"]
            for location in location_list:
                for location_element in location.keys():
                    condition = column_conditions["allele"][location_element]
                    print(
                        AtomicFilter(
                            "allele",
                            location_element,
                            condition,
                            location[location_element],
                        )
                    )

        else:
            condition = column_conditions[table][key]

            if not (full_input_dict[key] == "*" or full_input_dict[key] == None):
                print(
                    AtomicFilter(
                        table,
                        key,
                        condition,
                        full_input_dict[key],
                    )
                )
