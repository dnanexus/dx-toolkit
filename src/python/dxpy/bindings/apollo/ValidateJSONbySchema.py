from ...exceptions import err_exit


class JSONValidator(object):
    """
    JSON validator class to validate a JSON against a schema.

    The schema is a dictionary with the following structure:

    {
        "key1": {
            "type": dict,
            "properties": {
                "name": {"type": str, "required": True},
                "attribute": {"type": str}
            }
        },
        "key2": {
            "type": dict,
            "properties": {
                "xyz": {"type": str},
                "abc": {"type": list}
            },
            "conflicting_keys": [["xyz", "abc"]]
        },
        "list_key_example": {
            "type": list,
            "items": {
                "type": dict,
                "properties": {
                    "nested_key1": {"type": str, "required": True},
                    "nested_key2": {"type": str}
                }
            }
        },
        "id": {
            "type": list,
        },
        "conflicting_keys": [["key1", "key2"]],
        "dependent_conditional_keys": {
            "list_key_example": ["key1", "key2"]
        }
    }

    Key types can be defined via the "type" key

    Required keys can be defined via the "required" key

    Conflicting keys can be defined via the "conflicting_keys" key, where the value is a list of lists of keys that are mutually exclusive.
    Conflicting keys can be defined at the level of the schema for the entire JSON or at key-level.
    In the example above, within "key2" the keys "xyz" and "abc" are mutually exclusive and cannot be present together.
    Similarly, at the level of the whole JSON, "key1" and "key2" are mutually exclusive and cannot be present together.

    Conditional key combinations can be defined via the dependent_conditional_keys at the level of the schema. These should be defined as a dict where the value is a list.
    In the example above, dependent_conditional_keys is defined as {"list_key_example": ["key1", "key2"]}, which means whenever list_key_example is present, either "key1"
    or "key2" has to be also present.

    Currently the maximum level of nestedness supported for validation is as deep as shown in the example above via the "list_key_example" key.

    This is an example of a valid JSON that satisfies all the conditions defined in the schema above:

    {
        "key1": {
            "name": "test",
        },
        "list_key_example": [
            {"nested_key1": "1", "nested_key2": "2"},
            {"nested_key1": "3", "nested_key2": "4"},
            {"nested_key1": "5"}
        ],
        "id": ["id_1", "id_2"]
    }

    """

    def __init__(self, schema):
        self.schema = schema

    def validate(self, input_json):
        for key, value in self.schema.items():
            if key in input_json:
                self.validate_properties(value.get("properties", {}), input_json[key])

                if value.get("type") == list:
                    self.validate_list_items(
                        value.get("items", {}), input_json[key], key
                    )

                # Check for incompatible/conflicting subkeys defined at the key-level
                if value.get("conflicting_keys"):
                    self.check_incompatible_subkeys(input_json[key], key)

        self.check_incompatible_keys(input_json)
        self.check_dependent_key_combinations(input_json)

    def validate_properties(self, properties, input_dict):
        for key, value in properties.items():
            if key not in input_dict and value.get("required"):
                err_exit(
                    "Required key '{}' was not found in the input JSON.".format(key)
                )
            if key in input_dict and not isinstance(input_dict[key], value.get("type")):
                err_exit(
                    "Key '{}' has an invalid type. Expected {} but got {}".format(
                        key, value.get("type"), type(input_dict[key])
                    )
                )

    def validate_list_items(self, item_schema, input_list, key_name):
        item_type = item_schema.get("type")
        if item_type:
            for item in input_list:
                self.validate_properties(item_schema.get("properties", {}), item)
        else:
            if not isinstance(input_list, list):
                err_exit(
                    "Expected list but got {} for {}".format(type(input_list), key_name)
                )
            for item in input_list:
                if not isinstance(item, str):
                    err_exit(
                        "Expected list items to be of type string for {}.".format(
                            key_name
                        )
                    )

    def check_incompatible_subkeys(self, input_json, current_key):
        for keys in self.schema.get(current_key, {}).get("conflicting_keys", []):
            if all(k in input_json for k in keys):
                err_exit(
                    "Conflicting keys {} cannot be present together.".format(
                        " and ".join(keys)
                    )
                )

    def check_incompatible_keys(self, input_json):
        for keys in self.schema.get("conflicting_keys", []):
            if all(key in input_json for key in keys):
                err_exit(
                    "Conflicting keys {} cannot be present together.".format(
                        " and ".join(keys)
                    )
                )

    def check_dependent_key_combinations(
        self, input_json, enforce_one_associated_key=False
    ):
        mandatory_combinations = self.schema.get("dependent_conditional_keys", {})
        for main_key, associated_keys in mandatory_combinations.items():
            if main_key in input_json:
                present_associated_keys = [
                    key for key in associated_keys if key in input_json
                ]
                if len(present_associated_keys) == 0:
                    err_exit(
                        "When {} is present, one of the following keys must be also present: {}.".format(
                            main_key, ", ".join(associated_keys)
                        )
                    )
                if len(present_associated_keys) > 1 and enforce_one_associated_key:
                    err_exit(
                        "Only one of the associated keys {} can be present for main key {}".format(
                            ", ".join(associated_keys)
                        )
                    )
