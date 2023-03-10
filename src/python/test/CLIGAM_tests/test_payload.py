# To be run within a docker container that the dxpy package has been build locally in
import json
from dxpy.dx_extract_utils.turbo_filter import FinalPayload

filter = "/dx-toolkit/src/python/test/CLIGAM_tests/test_input/unit_tests/allele_malformatted.json"
output = "~/allele_malformatted_output.json"
type = "allele"
project_context = "project-FkyXg38071F1vGy2GyXyYYQB"
name = "veo_demo_dataset_assay"
id = "da6a4ffc-7571-4b2f-853d-445460a18396"
ref_genome = "GRCh38.92"
sql_flag = False


with open(filter, "r") as infile:
    filter_dict = json.load(infile)
full_payload = FinalPayload(
    filter_dict, name, id, project_context, ref_genome, type, sql_flag
)

print(full_payload)

# Send payload to vizserver
# response = requests.post("/viz-data/3.0/record-xxxx/raw", json=json.dumps(full_payload))

# print("Status Code: {}".format(response.status_code))
# print("Response JSON: {}".format(response.json()))
