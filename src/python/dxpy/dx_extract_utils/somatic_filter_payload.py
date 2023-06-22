import json
import pprint
import os

# from ...exceptions import err_exit, ResourceNotFound
import dxpy


# extract_utils_basepath = "/Users/jmulka@dnanexus.com/Development/dx-toolkit/src/python/dxpy/dx_extract_utils/somatic"

if False:
    # path to resources
    extract_utils_basepath = os.path.join(
        os.path.dirname(dxpy.__file__), "dx_extract_utils"
    )

    # A dictionary relating the user-facing names of columns to their actual column
    # names in the tables
    # with open(
    #    os.path.join(extract_utils_basepath, "somatic_column_conversion.json"), "r"
    # ) as infile:
    #    column_conversion = json.load(infile)
column_conversion = {
    "allele_id": "variant_read_optimized$allele_id",
    "variant_type": "variant_read_optimized$variant_type",
    "symbol": "variant_read_optimized$SYMBOL",
    "gene": "variant_read_optimized$Gene",
    "feature": "variant_read_optimized$Feature",
    "hgvs-c": "variant_read_optimized$HGVSc",
    "hgvs-p": "variant_read_optimized$HGVSp",
    "assay_sample_id": "variant_read_optimized$assay_sample_id",
    "sample_id": "variant_read_optimized$sample_id",
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
    # The table is always "variant_read_optimized" in somatic assays
    table = "variant_read_optimized"
    filter_key = column_conversion[friendly_name]
    # All current filterable fields use the "in" condition
    condition = "in"
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
            {"condition": "greater-than", "values": start},
            {"condition": "less-than", "values": end},
        ]
        location_compound["compound"].append(indiv_loc_filter)

    return location_compound


def generate_pheno_filter(
    full_input_dict,
    name,
    id,
    project_context,
    genome_reference,
):
    """
    Generate asasy filter consisting of a compound that links the Location filters if present
    to the regular filters
    {
        "pheno_filters": {
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
    pheno_filter = {
        "pheno_filters": {"name": name, "id": id, "logic": "and", "compound": []}
    }
    basic_filters = {"filters": {}, "logic": "and"}

    for filter_group in full_input_dict.keys():
        if filter_group == "location":
            location_compound = location_filter(full_input_dict["location"])
            pheno_filter["pheno_filters"]["compound"].append(location_compound)
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
    if len(basic_filters["filters"]) > 0:
        pheno_filter["pheno_filters"]["compound"].append(basic_filters)
    # else:
    # In the case where only a location filter is given, we need to remove the outer compound
    #    pass
    return pheno_filter


def somatic_final_payload(
    full_input_dict, name, id, project_context, genome_reference, additional_fields=None
):
    """
    Assemble the top level payload.  Top level dict contains the project context, fields (return columns),
    and raw filters objects.  This payload is sent in its entirety to the vizserver via an
    HTTPS POST request
    """
    # Generate the assay filter component of the payload
    pheno_filter = generate_pheno_filter(
        full_input_dict,
        name,
        id,
        project_context,
        genome_reference,
    )

    final_payload = {}
    # Set the project context
    final_payload["project_context"] = project_context
    if False:
        pass
        # with open(
        #    os.path.join(extract_utils_basepath, "return_columns_somatic.json")
        # ) as infile:
        #    fields = json.load(infile)
    fields = [
        {"assay_sample_id": "variant_read_optimized$assay_sample_id"},
        {"allele_id": "variant_read_optimized$allele_id"},
        {"CHROM": "variant_read_optimized$CHROM"},
        {"POS": "variant_read_optimized$POS"},
        {"REF": "variant_read_optimized$REF"},
        {"allele": "variant_read_optimized$allele"},
    ]

    if additional_fields:
        for add_field in additional_fields.split(","):
            fields.append(
                {"{}".format(add_field): "variant_read_optimized${}".format(add_field)}
            )

    final_payload["fields"] = fields

    final_payload["raw_filters"] = pheno_filter
    final_payload["is_cohort"] = True
    final_payload["distinct"] = True

    field_names = []
    for f in fields:
        field_names.append(list(f.keys())[0])

    return final_payload, field_names


if __name__ == "__main__":
    # Test path section
    # TODO remove later

    name = "assay_title_annot_complete"
    id = "f6a09c05-a1ea-4eb8-a8c1-6663992007a6"
    genome_reference = "Homo_sapiens.GRCh38.92"
    proj_id = "project-GP7B0X80VBvx6pGKJ3fq1Q7G"

    test_dir = "/Users/jmulka@dnanexus.com/Development/dx-toolkit/src/python/test/clisam_test_filters"
    input_dir = os.path.join(test_dir, "input")
    output_dir = os.path.join(test_dir, "output")
    test_filter = "single_location.json"

    test_json_path = os.path.join(input_dir, test_filter)

    with open(test_json_path, "r") as infile:
        test_dict = json.load(infile)

    payload, field_names = somatic_final_payload(
        test_dict, name, id, proj_id, genome_reference
    )

    with open(
        os.path.join(output_dir, test_filter[:-5] + "_payload.json"), "w"
    ) as outfile:
        json.dump(payload, outfile)
