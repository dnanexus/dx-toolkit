from __future__ import annotations
import sys
from ..exceptions import err_exit

# Generic error messages
malformed_filter = "Found following invalid filters: {}"
maxitem_message = "Too many items given in field {}, maximum is {}"
# An integer equel to 2 if script is run with python2, and 3 if run with python3
python_version = sys.version_info.major


GENOTYPE_TYPES = (
    "ref",
    "het-ref",
    "hom",
    "het-alt",
    "half",
    "no-call",
)


def warn(msg: str):
    print(f"WARNING: {msg}", file=sys.stderr)


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
        if not ("allele_id" in keys or "location" in keys):
            err_exit("allele_id or location is required in genotype filters")

        if "allele_id" in keys and "location" in keys:
            err_exit("allele_id and location fields cannot both be specified in the same genotype filter")

        # Check allele_id field
        if "allele_id" in keys:
            if not is_list_of_strings(filter["allele_id"]):
                err_exit(malformed_filter.format("allele_id"))

            # Check for too many values given
            if len(filter["allele_id"]) > 100:
                err_exit(maxitem_message.format("allele_id", 100))

        # Check location field
        if "location" in keys:
            # Ensure there are not more than 100 locations
            if len(filter["location"]) > 100:
                err_exit(maxitem_message.format("location", 100))
            for indiv_location in filter["location"]:
                indiv_loc_keys = indiv_location.keys()
                # Ensure all keys are there
                if not ("chromosome" in indiv_loc_keys and "starting_position" in indiv_loc_keys):
                    err_exit(malformed_filter.format("location"))
                if "ending_position" in indiv_loc_keys:
                    err_exit(malformed_filter.format("location"))
                # Check that each key is a string
                if not is_list_of_strings(list(indiv_location.values())):
                    err_exit(malformed_filter.format("location"))

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
                if item not in GENOTYPE_TYPES:
                    err_exit(malformed_filter.format("genotype_type") +"\nvalue {} is not a valid genotype_type".format(item))

            # Check for too many values given
            if len(filter["genotype_type"]) > 6:
                err_exit(maxitem_message.format("genotype_type", 4))

def validate_infer_flags(
    infer_nocall: bool,
    infer_ref: bool,
    exclude_nocall: bool,
    exclude_refdata: bool,
    exclude_halfref: bool,
):
    # Validate that the genomic_variant assay ingestion exclusion marks and the infer flags are used properly
    if (infer_ref or infer_nocall) and exclude_nocall is None:
        err_exit(
            "The --infer-ref or --infer-nocall flags can only be used when the undelying assay is of version generalized_assay_model_version 1.0.1/1.1.1 or higher."
        )
    ingestion_parameters_str = f"Exclusion parameters set at the ingestion: exclude_nocall={str(exclude_nocall).lower()}, exclude_halfref={str(exclude_halfref).lower()}, exclude_refdata={str(exclude_refdata).lower()}"
    if infer_ref:
        if not (
            exclude_nocall is False
            and exclude_halfref is False
            and exclude_refdata
        ):
            err_exit(
                f"The --infer-ref flag can only be used when exclusion parameters at ingestion were set to 'exclude_nocall=false', 'exclude_halfref=false', and 'exclude_refdata=true'.\n{ingestion_parameters_str}"
            )
    if infer_nocall:
        if not (
            exclude_nocall
            and exclude_halfref is False
            and exclude_refdata is False
        ):
            err_exit(
                f"The --infer-nocall flag can only be used when exclusion parameters at ingestion were set to 'exclude_nocall=true', 'exclude_halfref=false', and 'exclude_refdata=false'.\n{ingestion_parameters_str}"
            )


def validate_filter_applicable_genotype_types(
    infer_nocall: bool,
    infer_ref: bool,
    filter_dict: dict,
    exclude_nocall: bool,
    exclude_refdata: bool,
    exclude_halfref: bool,
):
    # Check filter provided genotype_types against exclusion options at ingestion.
    # e.g. no-call is not applicable when exclude_genotype set and infer-nocall false

    if "genotype_type" in filter_dict:
        if exclude_nocall and not infer_nocall:
            if "no-call" in filter_dict["genotype_type"]:
                warn(
                    "Filter requested genotype type 'no-call', genotype entries of this type were not ingested in the provided dataset and the --infer-nocall flag is not set!"
                )
            if filter_dict["genotype_type"] == []:
                warn(
                    "No genotype type requested in the filter. All genotype types will be returned. Genotype entries of type 'no-call' were not ingested in the provided dataset and the --infer-nocall flag is not set!"
                )
        if exclude_refdata and not infer_ref:
            if "ref" in filter_dict["genotype_type"]:
                warn(
                    "Filter requested genotype type 'ref', genotype entries of this type were not ingested in the provided dataset and the --infer-ref flag is not set!"
                )
            if filter_dict["genotype_type"] == []:
                warn(
                    "No genotype type requested in the filter. All genotype types will be returned. Genotype entries of type 'ref' were not ingested in the provided dataset and the --infer-ref flag is not set!"
                )
        if exclude_halfref:
            if "half" in filter_dict["genotype_type"]:
                warn(
                    "Filter requested genotype type 'half', 'half-ref genotype' entries (0/.) were not ingested in the provided dataset!"
                )
            if filter_dict["genotype_type"] == []:
                warn(
                    "No genotype type requested in the filter. All genotype types will be returned.  'half-ref' genotype entries (0/.) were not ingested in the provided dataset!"
                )
        if (
            exclude_refdata is None
            and "ref" in filter_dict["genotype_type"]
            or exclude_nocall is None
            and "no-call" in filter_dict["genotype_type"]
        ):
            err_exit(
                '"ref" and "no-call" genotype types can only be filtered when the undelying assay is of version generalized_assay_model_version 1.0.1/1.1.1 or higher.'
            )


def inference_validation(
    infer_nocall: bool,
    infer_ref: bool,
    filter_dict: dict,
    exclude_nocall: bool,
    exclude_refdata: bool,
    exclude_halfref: bool,
):
    validate_infer_flags(
        infer_nocall, infer_ref, exclude_nocall, exclude_refdata, exclude_halfref
    )
    validate_filter_applicable_genotype_types(
        infer_nocall,
        infer_ref,
        filter_dict,
        exclude_nocall,
        exclude_refdata,
        exclude_halfref,
    )


