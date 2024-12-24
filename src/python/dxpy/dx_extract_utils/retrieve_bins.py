import json
from ..exceptions import err_exit, ResourceNotFound
import os
import dxpy
import subprocess

def retrieve_bins(list_of_genes, project, genome_reference, extract_utils_basepath,
                  stage_file,platform_file,error_message):
    """
    A function for determining appropriate geno bins to attach to a given filter
    """
    project_desc = dxpy.describe(project)
    geno_positions = []

    try:
        with open(
            os.path.join(
                extract_utils_basepath, platform_file
            ),
            "r",
        ) as geno_bin_manifest:
            r = json.load(geno_bin_manifest)
        dxpy.describe(r[genome_reference][project_desc["region"]])
    except ResourceNotFound:
        with open(
            os.path.join(
                extract_utils_basepath, stage_file
            ),
            "r",
        ) as geno_bin_manifest:
            r = json.load(geno_bin_manifest)

    geno_bins = subprocess.check_output(
        ["dx", "cat", r[genome_reference][project_desc["region"]]]
    )
    geno_bins_json = json.loads(geno_bins)
    invalid_genes = []

    for gene in list_of_genes:
        bin = geno_bins_json.get(gene)
        if bin is None:
            invalid_genes.append(gene)
        else:
            bin.pop("strand")
            geno_positions.append(bin)

    if invalid_genes:
        error_message = error_message + ": " + str(invalid_genes)
        err_exit(error_message)

    return geno_positions