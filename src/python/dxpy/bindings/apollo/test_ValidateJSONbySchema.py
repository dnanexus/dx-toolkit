from ValidateJSONbySchema import JSONValidator

# Test schema
schema = {
    "key1": {
        "type": dict,
        "properties": {"prop": {"type": str, "required": True}, "test": {"type": str}},
    },
    "key2": {"type": dict, "properties": {"xyz": {"type": str}, "wtz": {"type": str}}},
    "list_key": {
        "type": list,
        "items": {
            "type": dict,
            "properties": {
                "nested_prop": {"type": str, "required": True},
                "nested_prop2": {"type": str},
            },
        },
    },
    "conflicting_keys": [["key1", "key2"]],
    "dependent_conditional_keys": {"list_key": ["key1", "key2"]},
}

input_json = {
    "key1": {"prop": "1", "test": "2"},
    "list_key": [
        # {"nested_prop2": "nested1"},
        {"nested_prop": "nested1", "nested_prop2": "nested2"}
    ],
}

input_json2 = {
    "key2": {"prop": "1", "test": "2"},
    "list_key": [
        {"nested_prop2": "nested1", "nested_prop": "1"},
        {"nested_prop": "nested1", "nested_prop2": "nested2"},
    ],
}

input_json3 = {"key1": {"prop": "1", "test": "2"}, "key2": {"prop": "1", "test": "2"}}

input_json4 = {
    "list_key": [
        {"nested_prop2": "nested1", "nested_prop": "1"},
        {"nested_prop": "nested1", "nested_prop2": "nested2"},
    ]
}

input_json5 = {
    "key1": {"prop": "1", "test": "2"},
    "list_key": [
        {"nested_prop2": "nested1"},
        {"nested_prop": "nested1", "nested_prop2": "nested2"},
    ],
}


cliexpress_schema = {
    "annotation": {
        "type": dict,
        "properties": {
            "feature_name": {"type": list, "required": False},
            "feature_id": {"type": list, "required": False},
        },
        "conflicting_keys": [["feature_name", "feature_id"]],
    },
    "expression": {
        "type": dict,
        "properties": {"min_value": {"type": str}, "max_value": {"type": str}},
    },
    "location": {
        "type": list,
        "items": {
            "type": dict,
            "properties": {
                "chromosome": {"type": str, "required": True},
                "starting_position": {"type": str, "required": True},
                "ending_position": {"type": str, "required": True},
            },
        },
    },
    "sample_id": {
        "type": list,
    },
    "conflicting_keys": [["location", "annotation"]],
    "dependent_conditional_keys": {"expression": ["annotation", "location"]},
}

cliexpress_input_json_help = {
    "location": [
        {"chromosome": "1", "starting_position": "10000", "ending_position": "20000"},
        {"chromosome": "X", "starting_position": "500", "ending_position": "1700"},
    ],
    "expression": {"min_value": "10.2", "max_value": "10000"},
    "annotation": {
        "feature_name": ["BRCA2"],
        "feature_id": ["ENSG0000001", "ENSG00000002"],
    },
    "sample_id": ["Sample1", "Sample2"],
}

cliexpress_input_json_1 = {
    "location": [
        {"chromosome": "1", "starting_position": "10000", "ending_position": "20000"},
        {"chromosome": "X", "starting_position": "500", "ending_position": "1700"},
    ],
    "expression": {"min_value": "10.2", "max_value": "10000"},
}

cliexpress_input_json_2 = {"sample_id": ["123", "12"]}

cliexpress_input_json_3 = {"sample_id": "1"}

cliexpress_input_json_4 = {"expression": {"min_value": "1", "max_value": "10000"}}

cliexpress_input_json_5 = {
    "annotation": {
        "feature_name": ["BRCA2"],
        "feature_id": ["ENSG0000001", "ENSG00000002"],
    }
}

schema2 = {
    "key1": {
        "type": dict,
        "properties": {
            "name": {"type": str, "required": True},
            "attribute": {"type": str},
        },
    },
    "key2": {
        "type": dict,
        "properties": {"xyz": {"type": str}, "abc": {"type": list}},
        "conflicting_keys": [["xyz", "abc"]],
    },
    "list_key_example": {
        "type": list,
        "items": {
            "type": dict,
            "properties": {
                "nested_key1": {"type": str, "required": True},
                "nested_key2": {"type": str},
            },
        },
    },
    "id": {
        "type": list,
    },
    "conflicting_keys": [["key1", "key2"]],
    "dependent_conditional_keys": {"list_key_example": ["key1", "key2"]},
}

input_schema_2 = {
    "key1": {
        "name": "test",
    },
    "list_key_example": [
        {"nested_key1": "1", "nested_key2": "2"},
        {"nested_key1": "3", "nested_key2": "4"},
        {"nested_key1": "5"},
    ],
    "id": ["id_1", "id_2"],
}

# Create an instance of the JSONValidator class and validate the input JSON
validator = JSONValidator(schema)

try:
    validator.validate(input_json)
except ValueError as e:
    print(e)

try:
    validator.validate(input_json2)
except ValueError as e:
    print(e)

try:
    validator.validate(input_json3)
except ValueError as e:
    print(e)

try:
    validator.validate(input_json4)
except ValueError as e:
    print(e)

try:
    validator.validate(input_json5)
except ValueError as e:
    print(e)


validator = JSONValidator(cliexpress_schema)
try:
    validator.validate(cliexpress_input_json_1)
except ValueError as e:
    print(e)

try:
    validator.validate(cliexpress_input_json_2)
except ValueError as e:
    print(e)

try:
    validator.validate(cliexpress_input_json_3)
except ValueError as e:
    print(e)

try:
    validator.validate(cliexpress_input_json_4)
except ValueError as e:
    print(e)

try:
    validator.validate(cliexpress_input_json_5)
except ValueError as e:
    print(e)

validator = JSONValidator(schema2)
try:
    validator.validate(input_schema_2)
except ValueError as e:
    print(e)
