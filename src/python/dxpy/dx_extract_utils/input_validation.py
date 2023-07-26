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
            err_exit(
                "location and rsid fields cannot both be specified in the same filter"
            )
        if not (("location" in keys) or ("rsid" in keys)):
            err_exit("Either location or rsid must be specified in an allele filter")
        # Check rsid field
        if "rsid" in keys:
            if not is_list_of_strings(filter["rsid"]):
                err_exit(malformed_filter.format("rsid"))
            # Check for max item number
            if len(filter["rsid"]) > 100:
                err_exit(maxitem_message.format("rsid", 100))
        # Check type field
        if "type" in keys:
            if not is_list_of_strings(filter["type"]):
                err_exit(malformed_filter.format("type"))
            # Check against allowed values
            for item in filter["type"]:
                if item not in ["SNP", "Ins", "Del", "Mixed"]:
                    err_exit(malformed_filter.format("type"))
            # Check for max item number
            if len(filter["type"]) > 100:
                err_exit(maxitem_message.format("type", 100))

        # Check dataset_alt_af
        if "dataset_alt_af" in keys:
            min_val = filter["dataset_alt_af"]["min"]
            max_val = filter["dataset_alt_af"]["max"]
            if min_val < 0:
                err_exit(malformed_filter.format("dataset_alt_af"))
            if max_val > 1:
                err_exit(malformed_filter.format("dataset_alt_af"))
            if min_val > max_val:
                err_exit(malformed_filter.format("dataset_alt_af"))
        # Check gnomad_alt_af
        if "gnomad_alt_af" in keys:
            min_val = filter["gnomad_alt_af"]["min"]
            max_val = filter["gnomad_alt_af"]["max"]
            if min_val < 0:
                err_exit(malformed_filter.format("gnomad_alt_af"))
            if max_val > 1:
                err_exit(malformed_filter.format("gnomad_alt_af"))
            if min_val > max_val:
                err_exit(malformed_filter.format("gnomad_alt_af"))
        # Check location field
        if "location" in keys:
            # Ensure there are not more than 100 locations
            if len(filter["location"]) > 100:
                err_exit(maxitem_message.format("location", 100))
            for indiv_location in filter["location"]:
                indiv_loc_keys = indiv_location.keys()
                # Ensure all keys are there
                if not (
                    ("chromosome" in indiv_loc_keys)
                    and ("starting_position" in indiv_loc_keys)
                    and ("ending_position" in indiv_loc_keys)
                ):
                    err_exit(malformed_filter.format("location"))
                # Check that each key is a string
                if not is_list_of_strings(list(indiv_location.values())):
                    err_exit(malformed_filter.format("location"))

    if filter_type == "annotation":
        keys = filter.keys()
        if not (
            ("allele_id" in filter.keys())
            or ("gene_name" in filter.keys())
            or ("gene_id" in filter.keys())
        ):
            err_exit(
                "allele_id, gene_name, or gene_id is required in annotation_filters"
            )

        # Ensure only one of the required fields is given
        if "allele_id" in keys:
            if ("gene_name" in keys) or ("gene_id" in keys):
                err_exit(
                    "Only one of allele_id, gene_name, and gene_id can be provided in an annotation filter"
                )

        elif "gene_id" in keys:
            if ("gene_name" in keys) or ("allele_id" in keys):
                err_exit(
                    "Only one of allele_id, gene_name, and gene_id can be provided in an annotation filter"
                )

        elif "gene_name" in keys:
            if ("gene_id" in keys) or ("allele_id" in keys):
                err_exit(
                    "Only one of allele_id, gene_name, and gene_id can be provided in an annotation filter"
                )

        # Consequences and putative impact cannot be provided without at least one of gene_id, gene_name, feature_id
        if ("consequences" in keys) or ("putative_impact" in keys):
            if (
                ("gene_id" not in keys)
                and ("gene_name" not in keys)
                and ("feature_id" not in keys)
            ):
                err_exit(
                    "consequences and putative impact fields may not be specified without "
                    + "at least one of gene_id, gene_name, or feature_id"
                )

        # All annotation fields are lists of strings
        for key in keys:
            if not is_list_of_strings(filter[key]):
                err_exit(malformed_filter.format(key))

        # Now ensure no fields have more than the maximum allowable number of items
        if "allele_id" in keys:
            if len(filter["allele_id"]) > 100:
                err_exit(maxitem_message.format("allele_id", 100))

        if "gene_name" in keys:
            if len(filter["gene_name"]) > 100:
                err_exit(maxitem_message.format("gene_name", 100))

        if "gene_id" in keys:
            if len(filter["gene_id"]) > 100:
                err_exit(maxitem_message.format("gene_id", 100))

        if "hgvs_c" in keys:
            if len(filter["hgvs_c"]) > 100:
                err_exit(maxitem_message.format("hgvs_c", 100))

        if "hgvs_p" in keys:
            if len(filter["hgvs_p"]) > 100:
                err_exit(maxitem_message.format("hgvs_p", 100))

    if filter_type == "genotype":
        keys = filter.keys()
        if not "allele_id" in keys:
            err_exit("allele_id is required in genotype filters")

        # Check allele_id field
        if "allele_id" in keys:
            if not is_list_of_strings(filter["allele_id"]):
                err_exit(malformed_filter.format("allele_id"))

            # Check for too many values given
            if len(filter["allele_id"]) > 100:
                err_exit(maxitem_message.format("allele_id", 100))

        # Check sample_id field
        if "sample_id" in keys:
            if not is_list_of_strings(filter["sample_id"]):
                err_exit(malformed_filter.format("sample_id"))

            # Check for too many values given
            if len(filter["sample_id"]) > 1000:
                err_exit(maxitem_message.format("sample_id", 1000))

        # Check genotype field
        if "genotype_type" in keys:
            if not is_list_of_strings(filter["genotype_type"]):
                err_exit(malformed_filter.format("genotype_type") + "\ngenotype type is not a list of strings")

            # Check against allowed values
            for item in filter["genotype_type"]:
                if item not in ["hom-alt", "het-ref", "het-alt", "half"]:
                    err_exit(malformed_filter.format("genotype_type") +"\nvalue {} is not a valid genotype_type".format(item))

            # Check for too many values given
            if len(filter["genotype_type"]) > 4:
                err_exit(maxitem_message.format("genotype_type", 4))
