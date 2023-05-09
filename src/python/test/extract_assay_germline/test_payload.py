# To be run within a docker container that the dxpy package has been build locally in
# exucute with: python /dx-toolkit/src/python/test/extract_assay_germline/test_payload.py
import subprocess
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--sql", action="store_true")
parser.add_argument("filter",type=str,help="path to filter JSON")
parser.add_argument("output",type=str,help="path to output tsv file")
parser.add_argument("dataset",type=str,help="dataset represented as <project-id>:<record-id>")
parser.add_argument("filter_type",type=str,choices=["allele","annotation","genotype"],help="type of filter being applied")
                    
args = parser.parse_args()

command = "dx extract_assay germline {} --retrieve-{} {} -o {}".format(
    args.dataset, args.filter_type, args.filter, args.output
)
# print(command)

if args.sql:
    command += " --sql"

process = subprocess.run(command, shell=True)
