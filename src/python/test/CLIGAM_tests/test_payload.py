# To be run within a docker container that the dxpy package has been build locally in
# exucute with: python /dx-toolkit/src/python/test/CLIGAM_tests/test_payload.py
import subprocess

filter = "/dx-toolkit/src/python/test/CLIGAM_tests/test_input/unit_tests/single_filters/allele_rsid.json"
output = "allele_rsid_output.json"
dataset = "project-FkyXg38071F1vGy2GyXyYYQB:record-FyFPyz0071F54Zjb32vG82Gj"

command = "dx extract_assay germline {} --retrieve-allele {} --output {}".format(
    dataset, filter, output
)

process = subprocess.run(command, shell=True)
