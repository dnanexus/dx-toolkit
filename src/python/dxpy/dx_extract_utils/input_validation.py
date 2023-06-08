import json
import sys
from ..exceptions import err_exit

# Generic error messages
malformed_filter = "Found following invalid filters: {}"
maxitem_message = "Too many items given in field {}, maximum is {}"
# An integer equel to 2 if script is run with python2, and 3 if run with python3
python_version = sys.version_info.major


def is_list_of_strings(object):
    if not isinstance(object, list):
        return False
    for item in object:
        # Note that in python 2.7 these strings are read in as unicode
        if python_version == 2:
            if not (isinstance(item, str) or isinstance(item, unicode)):
                return False
        else:
            if not isinstance(item, str):
                return False
    return True


def validate_filter(filter, filter_type):
    keys = filter.keys()
    if filter_type == "allele":
        # Check for required fields
        if ("location" in keys) and ("rsid" in keys):
            print(
                "location and rsid fields cannot both be specified in the same filter"
            )
            err_exit()
        if not (("location" in keys) or ("rsid" in keys)):
            print("Either location or rsid must be specified in an allele filter")
            err_exit()
        # Check rsid field
        if "rsid" in keys:
            if not is_list_of_strings(filter["rsid"]):
                print(malformed_filter.format("rsid"))
                err_exit()
            # Check for max item number
            if len(filter["rsid"]) > 100:
                print(maxitem_message.format("rsid",100))
        # Check type field
        if "type" in keys:
            if not is_list_of_strings(filter["type"]):
                print(malformed_filter.format("type"))
                err_exit()
            # Check against allowed values
            for item in filter["type"]:
                if item not in ["SNP", "Ins", "Del", "Mixed"]:
                    print(malformed_filter.format("type"))
                    err_exit()
            # Check for max item number
            if len(filter["type"]) > 100:
                print(maxitem_message.format("type",100))

        # Check dataset_alt_af
        if "dataset_alt_af" in keys:
            min_val = filter["dataset_alt_af"]["min"]
            max_val = filter["dataset_alt_af"]["max"]
            if min_val < 0:
                print(malformed_filter.format("dataset_alt_af"))
                err_exit()
            if max_val > 1:
                print(malformed_filter.format("dataset_alt_af"))
                err_exit()
            if min_val > max_val:
                print(malformed_filter.format("dataset_alt_af"))
                err_exit()
        # Check gnomad_alt_af
        if "gnomad_alt_af" in keys:
            min_val = filter["gnomad_alt_af"]["min"]
            max_val = filter["gnomad_alt_af"]["max"]
            if min_val < 0:
                print(malformed_filter.format("gnomad_alt_af"))
                err_exit()
            if max_val > 1:
                print(malformed_filter.format("gnomad_alt_af"))
                err_exit()
            if min_val > max_val:
                print(malformed_filter.format("gnomad_alt_af"))
                err_exit()
        # Check location field
        if "location" in keys:
            # Ensure there are not more than 100 locations
            if len(filter["location"]) > 100:
                print(maxitem_message.format("location",100))
            for indiv_location in filter["location"]:
                indiv_loc_keys = indiv_location.keys()
                # Ensure all keys are there
                if not (
                    ("chromosome" in indiv_loc_keys)
                    and ("starting_position" in indiv_loc_keys)
                    and ("ending_position" in indiv_loc_keys)
                ):
                    print(malformed_filter.format("location"))
                    err_exit()
                # Check that each key is a string
                if not is_list_of_strings(list(indiv_location.values())):
                    print(malformed_filter.format("location"))
                    err_exit()

    if filter_type == "annotation":
        keys = filter.keys()
        if not (
            ("allele_id" in filter.keys())
            or ("gene_name" in filter.keys())
            or ("gene_id" in filter.keys())
        ):
            print("allele_id, gene_name, or gene_id is required in annotation_filters")
            err_exit()
        # Ensure only one of the required fields is given
        if "allele_id" in keys:
            if ("gene_name" in keys) or ("gene_id" in keys):
                print(
                    "Only one of allele_id, gene_name, and gene_id can be provided in an annotation filter"
                )
                err_exit()
        elif "gene_id" in keys:
            if ("gene_name" in keys) or ("allele_id" in keys):
                print(
                    "Only one of allele_id, gene_name, and gene_id can be provided in an annotation filter"
                )
                err_exit()
        elif "gene_name" in keys:
            if ("gene_id" in keys) or ("allele_id" in keys):
                print(
                    "Only one of allele_id, gene_name, and gene_id can be provided in an annotation filter"
                )
                err_exit()
        # Consequences and putative impact cannot be provided without at least one of gene_id, gene_name, feature_id
        if ("consequences" in keys) or ("putative_impact" in keys):
            if (
                ("gene_id" not in keys)
                and ("gene_name" not in keys)
                and ("feature_id" not in keys)
            ):
                print(
                    "consequences and putative impact fields may not be specified without "
                    + "at least one of gene_id, gene_name, or feature_id"
                )
                err_exit()

        # All annotation fields are lists of strings
        for key in keys:
            if not is_list_of_strings(filter[key]):
                print(malformed_filter.format(key))
                err_exit()
        
        # Now ensure no fields have more than the maximum allowable number of items
        if "allele_id" in keys:
            if len(filter["allele_id"]) > 100:
                print(maxitem_message.format("allele_id",100))
                err_exit()
        if "gene_name" in keys:
            if len(filter["gene_name"]) > 100:
                print(maxitem_message.format("gene_name",100))
                err_exit()
        if "gene_id" in keys:
            if len(filter["gene_id"]) > 100:
                print(maxitem_message.format("gene_id",100))
                err_exit()
        if "hgvs_c" in keys:
            if len(filter["hgvs_c"]) > 100:
                print(maxitem_message.format("hgvs_c",100))
                err_exit()
        if "hgvs_p" in keys:
            if len(filter["hgvs_p"]) > 100:
                print(maxitem_message.format("hgvs_p",100))
                err_exit()

    if filter_type == "genotype":
        keys = filter.keys()
        if not "allele_id" in keys:
            print("allele_id is required in genotype filters")
            err_exit()
        # Check allele_id field
        if "allele_id" in keys:
            if not is_list_of_strings(filter["allele_id"]):
                print(malformed_filter.format("allele_id"))
                err_exit()
            # Check for too many values given
            if len(filter["allele_id"]) > 100:
                print(maxitem_message.format("allele_id",100))
                err_exit()
        # Check sample_id field
        if "sample_id" in keys:
            if not is_list_of_strings(filter["sample_id"]):
                print(malformed_filter.format("sample_id"))
                err_exit()
            # Check for too many values given
            if len(filter["sample_id"]) > 1000:
                print(maxitem_message.format("sample_id",1000))
                err_exit()
        # Check genotype field
        if "genotype_type" in keys:
            if not is_list_of_strings(filter["genotype_type"]):
                print("genotype type is not a list of strings")
                print(malformed_filter.format("genotype_type"))
                err_exit()
            # Check against allowed values
            for item in filter["genotype_type"]:
                if item not in ["hom-alt", "het-ref", "het-alt", "half"]:
                    print("value {} is not a valid genotype_type".format(item))
                    print(malformed_filter.format("genotype_type"))
                    err_exit()
            # Check for too many values given
            if len(filter["genotype_type"]) > 4:
                print(maxitem_message.format("genotype_type",4))
                err_exit()
