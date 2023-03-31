# To be run within a docker container that the dxpy package has been build locally in
# exucute with: python /dx-toolkit/src/python/test/CLIGAM_tests/test_payload.py
import subprocess
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--sql", action="store_true")
args = parser.parse_args()

filter = "/dx-toolkit/src/python/test/extract_assay_germline/test_input/single_filters/allele/allele_location.json"
# output = "allele_rsid_output.json"
dataset = "project-FkyXg38071F1vGy2GyXyYYQB:record-FyFPyz0071F54Zjb32vG82Gj"

command = "dx extract_assay germline {} --retrieve-allele {} ".format(dataset, filter)

if args.sql:
    command += " --sql"

process = subprocess.run(command, shell=True)
