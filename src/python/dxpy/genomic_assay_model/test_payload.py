from variant_filter import VariantFilter
import requests
import json
import dxpy


name = "testname"
id = "testid"
json_path = "/Users/jmulka@dnanexus.com/Development/dx-toolkit/src/python/dxpy/genomic_assay_model/test_input/example_input.json"
with open(json_path, "r") as infile:
    full_input_json = json.load(infile)

filter_object = VariantFilter(full_input_json, name, id)
filter_dict = filter_object.compile_filters()

response = requests.post("/viz-data/3.0/record-xxxx/raw", json=filter_dict)

print("Status Code: {}".format(response.status_code))
print("Response JSON: {}".format(response.json()))
