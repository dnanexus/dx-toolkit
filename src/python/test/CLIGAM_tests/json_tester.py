import json
from jsonschema import validate

filter_type = "annotation"
schema = "schemas/retrieve_{}_schema.json".format(filter_type)
input_data_name = "test_input/unit_tests/annotation_01.json"

with open(schema, "r") as infile:
    json_schema = json.load(infile)

with open(input_data_name, "r") as infile:
    json_data = json.load(infile)

try:
    validate(json_data, json_schema)
    print("JSON file {} is valid".format(input_data_name))
except Exception as inst:
    print(inst)
