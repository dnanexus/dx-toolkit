from variant_filter import VariantFilter
from turbo_filter import FinalPayload, GenerateAssayFilter
import requests
import json
import dxpy


name = "testname"
id = "testid"
json_path = "/Users/jmulka@dnanexus.com/Development/dx-toolkit/src/python/dxpy/genomic_assay_model/test_input/allele_filter.json"
record_id = "record-FyFPyz0071F54Zjb32vG82Gj"
project_id = "project-FkyXg38071F1vGy2GyXyYYQB"
with open(json_path, "r") as infile:
    full_input_json = json.load(infile)
project_context, path, entity_result = resolve_existing_path(
    "{}:{}".format(project_id, record_id)
)
assay_filter = GenerateAssayFilter(full_input_json, name, id, "allele")
full_payload = FinalPayload(assay_filter, project_context, "allele")

response = requests.post("/viz-data/3.0/record-xxxx/raw", json=json.dumps(full_payload))

print("Status Code: {}".format(response.status_code))
print("Response JSON: {}".format(response.json()))
