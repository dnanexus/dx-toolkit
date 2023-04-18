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


def validateFilter(filter, filter_type):
    keys = filter.keys()
    if filter_type == "allele":
        # Check for required fields
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
                print(malformed_filter.format("type"))
                exit(1)
            # Check against allowed values
            for item in filter["type"]:
                if item not in ["SNP", "Ins", "Del", "Mixed"]:
                    print(malformed_filter.format("type"))
                    exit(1)
        # Check dataset_alt_af
        if "dataset_alt_af" in keys:
            min_val = filter["dataset_alt_af"]["min"]
            max_val = filter["dataset_alt_af"]["max"]
            if min_val < 0:
                print(malformed_filter.format("dataset_alt_af"))
                exit(1)
            if max_val > 1:
                print(malformed_filter.format("dataset_alt_af"))
                exit(1)
            if min_val > max_val:
                print(malformed_filter.format("dataset_alt_af"))
                exit(1)
        # Check gnomad_alt_af
        if "gnomad_alt_af" in keys:
            min_val = filter["gnomad_alt_af"]["min"]
            max_val = filter["gnomad_alt_af"]["max"]
            if min_val < 0:
                print(malformed_filter.format("gnomad_alt_af"))
                exit(1)
            if max_val > 1:
                print(malformed_filter.format("gnomad_alt_af"))
                exit(1)
            if min_val > max_val:
                print(malformed_filter.format("gnomad_alt_af"))
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
                    print(malformed_filter.format("location"))
                    exit(1)
                # Check that each key is a string
                for val in indiv_location.values():
                    if not (isinstance(val, str)):
                        print(malformed_filter.format("location"))
                        exit(1)
    if filter_type == "annotation":
        keys = filter.keys()
        if not (
            ("allele_id" in filter.keys())
            or ("gene_name" in filter.keys())
            or ("gene_id" in filter.keys())
        ):
            print("allele_id, gene_name, or gene_id is required in annotation_filters")
            exit(1)
        # Ensure only one of the required fields is given
        if "allele_id" in filter.keys():
            if ("gene_name" in filter.keys()) or ("gene_id" in filter.keys()):
                print(
                    "Only one of allele_id, gene_name, and gene_id can be provided in an annotation filter"
                )
                exit(1)
        elif "gene_id" in filter.keys():
            if ("gene_name" in filter.keys()) or ("allele_id" in filter.keys()):
                print(
                    "Only one of allele_id, gene_name, and gene_id can be provided in an annotation filter"
                )
                exit(1)
        elif "gene_name" in filter.keys():
            if ("gene_id" in filter.keys()) or ("allele_id" in filter.keys()):
                print(
                    "Only one of allele_id, gene_name, and gene_id can be provided in an annotation filter"
                )
                exit(1)
        # All annotation fields are lists of strings
        for key in keys:
            if not isListOfStrings(filter[key]):
                print(malformed_filter.format(key))
                exit(1)
    if filter_type == "genotype":
        keys = filter.keys()
        if not "allele_id" in keys:
            print("allele_id is required in genotype filters")
            exit(1)
        # Check allele_id field
        if "allele_id" in keys:
            if not isListOfStrings(filter["allele_id"]):
                print(malformed_filter.format("allele_id"))
                exit(1)
        # Check sample_id field
        if "sample_id" in keys:
            if not isListOfStrings(filter["sample_id"]):
                print(malformed_filter.format("sample_id"))
                exit(1)
        # Check genotype field
        if "genotype_type" in keys:
            if not isListOfStrings(filter["genotype_type"]):
                print("genotype type is not a list of strings")
                print(malformed_filter.format("genotype_type"))
                exit(1)
            # Check against allowed values
            for item in filter["genotype_type"]:
                if item not in ["hom", "het-ref", "het-alt", "alt"]:
                    print("value {} is not a valid genotype_type".format(item))
                    print(malformed_filter.format("genotype_type"))
                    exit(1)


if __name__ == "__main__":
    filter_path = "/Users/jmulka@dnanexus.com/Development/dx-toolkit/src/python/test/CLIGAM_tests/test_input/unit_tests/allele_01.json"

    with open(filter_path, "r") as infile:
        filter = json.load(infile)
