import json
import pprint
import os

# from ...exceptions import err_exit, ResourceNotFound
import dxpy

if False:
    extract_utils_basepath = os.path.join(
        os.path.dirname(dxpy.__file__), "dx_extract_utils/somatic"
    )
else:
    extract_utils_basepath = "/Users/jmulka@dnanexus.com/Development/dx-toolkit/src/python/dxpy/dx_extract_utils/somatic"

# A dictionary relating the user-facing names of columns to their actual column
# names in the tables
with open(
    os.path.join(extract_utils_basepath, "somatic_column_conversion.json"), "r"
) as infile:
    column_conversion = json.load(infile)


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
    # The table is always "variant_read_optimized" in somatic assays
    table = "variant_read_optimized"
    filter_key = column_conversion[table][friendly_name]
    # All current filterable fields use the "in" condition
    condition = "in"
    listed_filter = {filter_key: [{"condition": condition, "values": values}]}
    return listed_filter


def location_filter(raw_location_list):
    """
    The somatic assay model does not currently support geno bins
    Locations are implemented as filters on chromosome and starting position
    """

    # Location filters are related to each other by "or"
    location_compound = {"compound": [], "logic": "or"}
    for location in raw_location_list:
        # atomic filters within an individual location filters are related by "and"
        indiv_loc_filter = {"filters": {}, "logic": "and"}
        start = int(location["starting_position"])
        end = int(location["ending_position"])
        if end - start > 250000000:
            exit(1)
            # err_exit(
            #    "Error in location {}\nLocation filters may not specify regions larger than 250 megabases".format(
            #        location
            #    )
            # )
        # First make the chr filter
        indiv_loc_filter["filters"]["variant_read_optimized$CHROM"] = [
            {"condition": "is", "values": location["chromosome"]}
        ]
        # Then the positional filters
        indiv_loc_filter["filters"]["variant_read_optimized$POS"] = [
            {"condition": "less-than", "values": end},
            {"condition": "greater-than", "values": start},
        ]
        location_compound["compound"].append(indiv_loc_filter)

    return location_compound


def generate_assay_filter(
    full_input_dict, name, id, project_context, genome_reference, filter_type
):
    """
    Generate asasy filter consisting of a compound that links the Location filters if present
    to the regular filters
    """


if __name__ == "__main__":
    # Test path section
    # TODO remove later
    location_json_path = (
        "/Users/jmulka@dnanexus.com/Development/dx-toolkit/clisam_location_filter.json"
    )

    with open(location_json_path, "r") as infile:
        location_dict = json.load(infile)

    location_list = location_dict["location"]
    pprint.pprint(location_filter(location_list))
