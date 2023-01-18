import json
from jsonschema import RefResolver, validate, validators

input_schema_name = "input_schema_draft.json"
input_data_name = "example_input.json"
schema_dir = "/Users/jmulka@dnanexus.com/Development/pseudopod/json_schema/cligam/variant_filter_json"

with open(input_schema_name, "r") as infile:
    json_schema = json.load(infile)

with open(input_data_name, "r") as infile:
    json_data = json.load(infile)

base_uri = "file://{}/{}".format(schema_dir, input_schema_name)
resolver = RefResolver(referrer=json_schema, base_uri=base_uri)
validator = validators.Draft202012Validator(json_schema, resolver)


try:
    validator.validate(json_data)
    print("JSON file {} is valid :)".format(input_data_name))
except Exception as inst:
    print(inst)
