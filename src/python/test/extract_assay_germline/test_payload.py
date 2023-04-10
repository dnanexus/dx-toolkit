# To be run within a docker container that the dxpy package has been build locally in
# exucute with: python /dx-toolkit/src/python/test/extract_assay_germline/test_payload.py
import subprocess
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--sql", action="store_true")
args = parser.parse_args()

filter = "/dx-toolkit/src/python/test/extract_assay_germline/test_input/single_filters/allele/allele_rsid.json"
output = "allele_rsid_output.tsv"
dataset = "project-FkyXg38071F1vGy2GyXyYYQB:record-FyFPyz0071F54Zjb32vG82Gj"
test02_dataset = "project-G9j1pX00vGPzF2XQ7843k2Jq:record-GQQKBJ80yP3gBXqXpkY4z4ZK"
filter_type = "allele"

command = "dx extract_assay germline {} --retrieve-{} {} -o {}".format(
    dataset, filter_type, filter, output
)
# print(command)

if args.sql:
    command += " --sql"

process = subprocess.run(command, shell=True)
