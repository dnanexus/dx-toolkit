import os

from ..exceptions import err_exit
from .retrieve_bins import retrieve_bins
import dxpy

# A dictionary relating the user-facing names of columns to their actual column

column_conversion = {
    "allele_id": "variant_read_optimized$allele_id",
    "variant_type": "variant_read_optimized$variant_type",
    "symbol": "variant_read_optimized$SYMBOL",
    "gene": "variant_read_optimized$Gene",
    "feature": "variant_read_optimized$Feature",
    "hgvsc": "variant_read_optimized$HGVSc",
    "hgvsp": "variant_read_optimized$HGVSp",
    "assay_sample_id": "variant_read_optimized$assay_sample_id",
    "sample_id": "variant_read_optimized$sample_id",
    "tumor_normal": "variant_read_optimized$tumor_normal",
}

column_conditions = {
    "allele_id": "in",
    "variant_type": "in",
    "symbol": "any",
    "gene": "any",
    "feature": "any",
    "hgvsc": "any",
    "hgvsp": "any",
    "assay_sample_id": "in",
    "sample_id": "in",
    "tumor_normal": "is",
}

extract_utils_basepath = os.path.join(
    os.path.dirname(dxpy.__file__), "dx_extract_utils"
)

chrs = []
for i in range(1, 23):
    chrs.append(str(i))
chrs.extend(["Y", "X"])

def retrieve_geno_bins(list_of_genes, project, genome_reference, friendly_name):
    """
    A function for determining appropriate geno bins to attach to a given SYMBOL, Gene or Feature
    """
    stage_file = "Homo_sapiens_genes_manifest_staging_vep.json"
    platform_file = "Homo_sapiens_genes_manifest_vep.json"
    error_message = "Following symbols, genes or features are invalid"
    genome_reference = genome_reference + "_" + friendly_name
    geno_bins = retrieve_bins(list_of_genes, project, genome_reference, extract_utils_basepath,
                  stage_file,platform_file,error_message)
    updated_bins = []
    # If a gene, symbol or feature has non standard contig,
    # The correct bin is 'Other'
    for bin in geno_bins:
        if bin['chr'].strip("chr").strip("Chr") not in chrs:
            bin['chr'] = 'Other'
        updated_bins.append(bin)
    return updated_bins

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
    table = "variant_read_optimized"
    # Get the name of this field in the variant table
    # If the column isn't in the regular fields list, use the friendly name itself as the column name
    # This could be the case when "--additional-fields" flag is used
    filter_key = column_conversion.get(
        friendly_name, "variant_read_optimized${}".format(friendly_name)
    )
    # Handle special cases where values need to be capitalized
    if friendly_name == "variant_type" or friendly_name == "gene" or friendly_name == "feature":
        values = [str(x).upper() for x in values]
    # Get the condition ofr this field
    condition = column_conditions.get(friendly_name, "in")

    # Check if we need to add geno bins as well
    if friendly_name in ["symbol", "gene", "feature"]:
        listed_filter = {
            filter_key: [
                {
                    "condition": condition,
                    "values": values,
                    "geno_bins": retrieve_geno_bins(
                        values, project_context, genome_reference, friendly_name
                    ),
                }
            ]
        }
    else:
        listed_filter = {filter_key: [{"condition": condition, "values": values}]}
    
    return listed_filter

def location_filter(location_list):
    """
    A location filter is actually an variant_read_optimized$allele_id filter with no filter values
    The geno_bins perform the actual location filtering. Related to other geno_bins filters by "or"
    """

    location_aid_filter = {
        "variant_read_optimized$allele_id": [
            {
                "condition": "in",
                "values": [],
                "geno_bins": [],
            }
        ]
    }

    chrom =[]

    for location in location_list:
        # First, ensure that the geno bins width isn't greater than 250 megabases
        start = int(location["starting_position"])
        end = int(location["ending_position"])
        if end - start > 250000000:
            err_exit(
                "Error in location {}\nLocation filters may not specify regions larger than 250 megabases".format(
                    location
                )
            )

        # Fill out the contents of an object in the geno_bins array
        chrom.append(location["chromosome"])
        chr = location["chromosome"].strip("chr").strip("Chr")
        
        # If a non standard contig ID is passed in location filter's chr,
        # Add the filter as a CHROM filter and pass 'Other' in geno_bins
        # Standard contigs are also passed to CHROM filter
        if chr not in chrs:
            chr = "Other"
            
        location_aid_filter["variant_read_optimized$allele_id"][0]["geno_bins"].append(
            {
                "chr": chr,
                "start": start,
                "end": end,
            }
        )
    
    return location_aid_filter, list(set(chrom))


def generate_assay_filter(
    full_input_dict,
    name,
    id,
    project_context,
    genome_reference=None,
    include_normal=False,
):
    """
    Generate asasy filter consisting of a compound that links the Location filters if present
    to the regular filters
    {
        "assay_filters": {
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

    filters_dict = {}

    for filter_group in full_input_dict.keys():
        if filter_group == "location":
            location_list = full_input_dict["location"]
            if location_list:
                location_aid_filter, chrom = location_filter(location_list)
                filters_dict.update(location_aid_filter)
                # Add a CHROM filter to handle non standard contigs
                filters_dict.update(basic_filter(
                    "variant_read_optimized",
                    "CHROM",
                    chrom,
                    project_context,
                    genome_reference,
                ))
            
        else:
            for individual_filter_name in full_input_dict[filter_group].keys():
                individual_filter = full_input_dict[filter_group][individual_filter_name]
                if individual_filter:
                    filters_dict.update(basic_filter(
                        "variant_read_optimized",
                        individual_filter_name,
                        individual_filter,
                        project_context,
                        genome_reference,
                    ))

    # If include_normal is False, then add a filter to select data where tumor_normal = tumor
    if not include_normal:
        filters_dict.update(basic_filter(
            "variant_read_optimized",
            "tumor_normal",
            "tumor",
            project_context,
            genome_reference,
        ))

    final_filter_dict = {"assay_filters": {"name": name, "id": id}}

    # Additional structure of the payload
    final_filter_dict["assay_filters"].update({"filters": filters_dict})
    # The general filters are related by "and"
    final_filter_dict["assay_filters"]["logic"] = "and"

    return final_filter_dict


def somatic_final_payload(
    full_input_dict,
    name,
    id,
    project_context,
    genome_reference=None,
    additional_fields=None,
    include_normal=False,
):
    """
    Assemble the top level payload.  Top level dict contains the project context, fields (return columns),
    and raw filters objects.  This payload is sent in its entirety to the vizserver via an
    HTTPS POST request
    """
    # Generate the assay filter component of the payload
    assay_filter = generate_assay_filter(
        full_input_dict, name, id, project_context, genome_reference, include_normal
    )

    final_payload = {}
    # Set the project context
    final_payload["project_context"] = project_context

    fields = [
        {"assay_sample_id": "variant_read_optimized$assay_sample_id"},
        {"allele_id": "variant_read_optimized$allele_id"},
        {"CHROM": "variant_read_optimized$CHROM"},
        {"POS": "variant_read_optimized$POS"},
        {"REF": "variant_read_optimized$REF"},
        {"allele": "variant_read_optimized$allele"},
    ]

    order_by = [
        {"CHROM":"asc"},
        {"POS":"asc"},
        {"allele_id":"asc"},
        {"assay_sample_id":"asc"}
    ]

    # If the user has specified additional return columns, add them to the payload here
    if additional_fields:
        for add_field in additional_fields:
            fields.append(
                {"{}".format(add_field): "variant_read_optimized${}".format(add_field)}
            )

    final_payload["fields"] = fields
    final_payload["order_by"] = order_by
    final_payload["raw_filters"] = assay_filter
    final_payload["distinct"] = True
    final_payload["adjust_geno_bins"] = False

    field_names = []
    for f in fields:
        field_names.append(list(f.keys())[0])

    return final_payload, field_names
