# To be run within a docker container that the dxpy package has been build locally in
# exucute with: python /dx-toolkit/src/python/test/CLIGAM_tests/test_payload.py
import subprocess
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--sql", action="store_true")
args = parser.parse_args()

filter = "/dx-toolkit/src/python/test/CLIGAM_tests/test_input/unit_tests/annotation_full.json"
output = "annotation_full.json"
dataset = "project-FkyXg38071F1vGy2GyXyYYQB:record-FyFPyz0071F54Zjb32vG82Gj"
filter_type = "annotation"

command = "dx extract_assay germline {} --retrieve-{} {} -o {}".format(
    dataset, filter_type, filter, output
)

if args.sql:
    command += " --sql"


process = subprocess.run(command, shell=True)
