import json

# Generic error messages
malformed_filter = "found following invalid filters: {}"


def isListOfStrings(object):
    if not isinstance(object, list):
        return False
    for item in object:
        if not isinstance(item, str):
            return False
    return True


def validateFilter(filter, filter_type, sql=False):
    keys = filter.keys()
    if filter_type == "allele":
        # Check for required fields
        if not sql:
            if ("location" in keys) and ("rsid" in keys):
                print(
                    "location and rsid fields cannot both be specified in the same filter"
                )
                exit(1)
            if not (("location" in keys) or ("rsid" in keys)):
                print("Either location or rsid must be specified in an allele filter")
                exit(1)
        # Check rsid field
        if "rsid" in keys:
            if not isListOfStrings(filter["rsid"]):
                print(malformed_filter.format("rsid"))
                exit(1)
        # Check type field
        if "type" in keys:
            if not isListOfStrings(filter["type"]):
                exit(1)
            # Check against allowed values
            for item in filter["type"]:
                if item not in ["SNP", "Ins", "Del", "Mixed"]:
                    exit(1)
        # Check dataset_alt_af
        if "dataset_alt_af" in keys:
            min_val = filter["dataset_alt_af"]["min"]
            max_val = filter["dataset_alt_af"]["max"]
            if min_val < 0:
                exit(1)
            if max_val > 1:
                exit(1)
            if min_val > max_val:
                exit(1)
        # Check gnomad_alt_af
        if "gnomad_alt_af" in keys:
            min_val = filter["gnomad_alt_af"]["min"]
            max_val = filter["gnomad_alt_af"]["max"]
            if min_val < 0:
                exit(1)
            if max_val > 1:
                exit(1)
            if min_val > max_val:
                exit(1)
        # Check location field
        if "location" in keys:
            for indiv_location in filter["location"]:
                indiv_loc_keys = indiv_location.keys()
                # Ensure all keys are there
                if not (
                    ("chromosome" in indiv_loc_keys)
                    and ("starting_position" in indiv_loc_keys)
                    and ("ending_position" in indiv_loc_keys)
                ):
                    exit(1)
                # Check that each key is a string
                for val in indiv_location.values():
                    if not (isinstance(val, str)):
                        exit(1)
    if filter_type == "annotation":
        keys = filter.keys()
        # Check for required fields if sql flag is not given
        if not sql:
            if not (
                ("allele_id" in filter.keys())
                or ("gene_name" in filter.keys())
                or ("gene_id" in filter.keys())
            ):
                exit(1)
        # All annotation fields are lists of strings
        for key in keys:
            if not isListOfStrings(filter[key]):
                exit(1)
    if filter_type == "genotype":
        keys = filter.keys()
        # Check for required fields if sql flag is not given
        if not sql:
            if not "allele_id" in keys:
                exit(1)
        # Check allele_id field
        if "allele_id" in keys:
            if not isListOfStrings(filter["allele_id"]):
                exit(1)
        # Check sample_id field
        if "sample_id" in keys:
            if not isListOfStrings(filter["sample_id"]):
                exit(1)
        # Check genotype field
        if "genotype" in keys:
            if not isListOfStrings(filter["genotype"]):
                exit(1)
            # Check against allowed values
            if filter["genotype"] not in ["hom", "het-ref", "het-alt", "alt"]:
                exit(1)


if __name__ == "__main__":
    filter_path = "/Users/jmulka@dnanexus.com/Development/dx-toolkit/src/python/test/CLIGAM_tests/test_input/unit_tests/allele_01.json"

    with open(filter_path, "r") as infile:
        filter = json.load(infile)
