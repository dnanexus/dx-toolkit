import json

from ..exceptions import err_exit
from .input_validation import validate_filter
import os
import dxpy
from .retrieve_bins import retrieve_bins

extract_utils_basepath = os.path.join(
    os.path.dirname(dxpy.__file__), "dx_extract_utils"
)

# A dictionary relating the user-facing names of columns to their actual column
# names in the tables
with open(
    os.path.join(extract_utils_basepath, "column_conversion.json"), "r"
) as infile:
    column_conversion = json.load(infile)

# A dictionary relating the user-facing names of columns to the condition that needs
# to be applied in the basic filter for the column
with open(
    os.path.join(extract_utils_basepath, "column_conditions.json"), "r"
) as infile:
    column_conditions = json.load(infile)


def retrieve_geno_bins(list_of_genes, project, genome_reference):
    """
    A function for determining appropriate geno bins to attach to a given annotation$gene_name
    or annotation$gene_id
    """
    stage_file = "Homo_sapiens_genes_manifest_staging.json"
    platform_file = "Homo_sapiens_genes_manifest.json"
    error_message = "Following gene names or IDs are invalid"
    geno_bins = retrieve_bins(list_of_genes, project, genome_reference, extract_utils_basepath,
                  stage_file,platform_file,error_message)
    return geno_bins


def basic_filter(
    filter_type, friendly_name, values=[], project_context=None, genome_reference=None
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
    filter_key = column_conversion[filter_type][friendly_name]
    condition = column_conditions[filter_type][friendly_name]

    # Input validation.  Check that the user hasn't provided an invalid min/max in any fields
    if condition == "between":
        min_val = float(values["min"])
        max_val = float(values["max"])
        if min_val > max_val:
            err_exit(
                "min value greater than max value for filter {}".format(friendly_name)
            )

        values = [min_val, max_val]
    if condition == "less-than" or condition == "greater-than":
        values = int(values)

    # Check for special cases where the user-input values need to be changed before creating payload
    # Case 1: Some fields need to be changed to upper case
    if friendly_name in ["gene_id", "feature_id", "putative_impact"]:
        values = [x.upper() for x in values]
    # Case 2: remove duplicate rsid values
    if filter_type == "allele" and friendly_name == "rsid":
        values = list(set(values))

    # Check if we need to add geno bins as well
    if friendly_name == "gene_id" or friendly_name == "gene_name":
        listed_filter = {
            filter_key: [
                {
                    "condition": condition,
                    "values": values,
                    "geno_bins": retrieve_geno_bins(
                        values, project_context, genome_reference
                    ),
                }
            ]
        }
    else:
        listed_filter = {filter_key: [{"condition": condition, "values": values}]}

    return listed_filter


def location_filter(location_list, table):
    """
    A location filter is actually an table$a_id filter with no filter values
    The geno_bins perform the actual location filtering.  Related to other geno_bins filters by "or"
    """

    location_aid_filter = {
        "{}$a_id".format(table): [
            {
                "condition": "in",
                "values": [],
                "geno_bins": [],
            }
        ]
    }

    for location in location_list:
        start = int(location["starting_position"])
        if "ending_position" in location:
            end = int(location["ending_position"])
            # Ensure that the geno bins width isn't greater than 5 megabases
            if end - start > 5000000:
                err_exit('\n'.join([
                    "Error in location {}".format(location),
                    "Location filters may not specify regions larger than 5 megabases",
                ]))
        else:
            end = start

        # Fill out the contents of an object in the geno_bins array
        location_aid_filter["{}$a_id".format(table)][0]["geno_bins"].append(
            {
                "chr": location["chromosome"],
                "start": start,
                "end": end,
            }
        )

    return location_aid_filter


def generate_assay_filter(
    full_input_dict,
    name,
    id,
    project_context,
    genome_reference,
    filter_type,
    exclude_nocall=None,
    exclude_refdata=None,
    exclude_halfref=None
):
    """
    Generate the entire assay filters object by reading the filter JSON, making the relevant
    Basic and Location filters, and creating the structure that relates them logically

    There are three possible types of input JSON: a genotype filter, an allele filter,
    and an annotation filter
    """

    filters_dict = {}
    table = filter_type if filter_type != "genotype_only" else "genotype"

    for key in full_input_dict.keys():
        if key == "location":
            location_list = full_input_dict["location"]
            location_aid_filter = location_filter(location_list, table)
            filters_dict.update(location_aid_filter)

        else:
            if not (full_input_dict[key] == "*" or full_input_dict[key] == None):
                filters_dict.update(
                    basic_filter(
                        filter_type,
                        key,
                        full_input_dict[key],
                        project_context,
                        genome_reference,
                    )
                )
    final_filter_dict = {"assay_filters": {"name": name, "id": id}}

    # Additional structure of the payload
    final_filter_dict["assay_filters"].update({"filters": filters_dict})
    # The general filters are related by "and"
    final_filter_dict["assay_filters"]["logic"] = "and"

    if exclude_nocall is not None:
        # no-call genotypes
        final_filter_dict["assay_filters"]["nocall_yn"] = not exclude_nocall
    if exclude_refdata is not None:
        # reference genotypes
        final_filter_dict["assay_filters"]["ref_yn"] = not exclude_refdata
    if exclude_halfref is not None:
        # half-reference genotypes
        final_filter_dict["assay_filters"]["halfref_yn"] = not exclude_halfref

    return final_filter_dict


def final_payload(
    full_input_dict,
    name,
    id,
    project_context,
    genome_reference,
    filter_type,
    order=True,
    exclude_nocall=None,
    exclude_refdata=None,
    exclude_halfref=None
):
    """
    Assemble the top level payload.  Top level dict contains the project context, fields (return columns),
    and raw filters objects.  This payload is sent in its entirety to the vizserver via an
    HTTPS POST request
    """
    # Generate the assay filter component of the payload
    assay_filter = generate_assay_filter(
        full_input_dict,
        name,
        id,
        project_context,
        genome_reference,
        filter_type,
        exclude_nocall,
        exclude_refdata,
        exclude_halfref
    )

    final_payload = {}
    # Set the project context
    final_payload["project_context"] = project_context

    # Section for defining returned columns for each of the three filter types
    if filter_type == "allele":
        with open(
            os.path.join(extract_utils_basepath, "return_columns_allele.json"), "r"
        ) as infile:
            fields = json.load(infile)
    elif filter_type == "annotation":
        with open(
            os.path.join(extract_utils_basepath, "return_columns_annotation.json"), "r"
        ) as infile:
            fields = json.load(infile)
    elif filter_type == "genotype":
        with open(
            os.path.join(extract_utils_basepath, "return_columns_genotype.json"), "r"
        ) as infile:
            fields = json.load(infile)
    elif filter_type == "genotype_only":
        with open(
            os.path.join(extract_utils_basepath, "return_columns_genotype_only.json"), "r"
        ) as infile:
            fields = json.load(infile)

    if order:
        order_by = [{"allele_id":"asc"}]

        if any("locus_id" in field for field in fields):
            order_by.insert(0, {"locus_id":"asc"})

        # In order for output to be deterministic, we need to do a secondary sort by sample_id
        # if it is present in the fields
        sample_id_present = False
        for field in fields:
            if "sample_id" in field:
                sample_id_present = True
        if sample_id_present:
            order_by.append({"sample_id":"asc"})

        final_payload["order_by"] = order_by

    final_payload["fields"] = fields
    final_payload["adjust_geno_bins"] = False
    final_payload["raw_filters"] = assay_filter
    final_payload["is_cohort"] = True
    final_payload["distinct"] = True

    field_names = []
    for f in fields:
        field_names.append(list(f.keys())[0])

    return final_payload, field_names


def validate_JSON(filter, type):
    """
    Check user-provdied JSON filter for validity
    Errors out if JSON is invalid, continues otherwise
    """

    schema_file = "retrieve_{}_schema.json".format(type)

    # Open the schema asset.
    with open(os.path.join(extract_utils_basepath, schema_file), "r") as infile:
        json_schema = json.load(infile)

    # Note: jsonschema disabled in this release
    # The jsonschema validation function will error out if the schema is invalid.  The error message will contain
    # an explanation of which part of the schema failed
    try:
        # A function for doing basic input validation that does not rely on jsonschema
        validate_filter(filter, type)
        # validate(filter, json_schema)
    except Exception as inst:
        err_exit(inst)
