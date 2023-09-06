class InputJSONFiltersValidator(object):
    """
    A specialized class that parsers input JSON according to a schema to prepare vizserver-compliant raw_filters.
    """

    def __init__(self, input_json, schema, error_handler=print):
        self.input_json = input_json
        self.schema = schema
        self.error_handler = error_handler
        self.condition_function_mapping = {
            "between": self.BETWEEN,
            "in": self.IN,
        }

    def parse(self):
        self.is_valid_json(self.schema)
        if self.get_schema_version() == "1.0":
            self.parse_v1()
        else:
            raise NotImplementedError

    def parse_v1(self):
        """
        v1 parser is currently only intended for raw_filters
        """
        vizserver_compound_filters = {
            "logic": self.get_toplevel_filtering_logic(),
            "compound": [],
        }

        all_filters = self.collect_filtering_conditions(self.schema)

        for filter_key, filter_values in self.input_json.items():
            if filter_key not in all_filters:
                self.error_handler(
                    "No filtering condition was defined for {} in the schema.".format(
                        filter_key
                    )
                )

            # Look at the input_json
            # go through each key: location, annotation, expression, sample.

            # Get the filtering conditions for the current key
            current_filters = all_filters[filter_key]
            current_properties = current_filters.get("properties")

            # Validate max number of allowed items if max_item_limit is defined at the top level within key
            # It will be later validated for each property as well
            self.validate_max_item_limit(current_filters, filter_values, filter_key)

            # There are several ways filtering_conditions can be defined
            # 1. Basic use-case: no properties, just condition
            # 2. Properties defined as dict of dicts (relatively straightforward use-case)
            # 3. Properties defined as list of dicts (more advanced, special use-case with complex conditional logics that need interpretation)
            if isinstance(type(current_properties), list):
                # multi-condition if list

                # must be compounded because more than one filter
                # must be recursed if more than one item in location

                xt = {
                    "logic": current_filters.get("filters_combination_operator"),
                    "compound": [
                        {},
                        {},
                    ]  # as many dicts inside as there are key conditions
                    # so count the location.properties for this
                }

                full = {
                    "logic": current_filters.get("items_combination_operator"),
                    "compound": [{xt}, {}]  # as many dicts as there are "items"
                    # count json list len for this
                }

                # multi-condition if dict within dict, if properties is list

                # remember to iterate over all values in input_json
                # remember that chr and genobin should be in one dict
                for item in current_properties:
                    if item.get("key"):
                        ### define this as a generic filter_builder function
                        filters = {
                            "filters": {
                                item.get("table_column"): [
                                    {
                                        "condition": item.get("condition"),
                                        "values": filter_values.get(item.get("key")),
                                    }
                                ]
                            }
                        }

                    if (
                        item.get("keys")
                        and item.get("condition") == "genobin_partial_overlap"
                    ):
                        self.build_partial_overlap_genobin_filters(
                            item, filter_values[0]
                        )  # change to iterate over all list items0
                        # just append it to the list of dicts in the compound

                # simple if just dict
                ...

            if isinstance(type(current_properties), dict):
                for k, v in current_properties.items():
                    self.validate_max_item_limit(v, filter_values[k], k)

                # now need to check if min_value and max_value map to the same column
                # consider changing min_/max_ to "keys": ["min_value", "max_value"]

            else:
                # no properties, so just apply conditions
                filters = {
                    "filters": {
                        current_properties.get("table_column"): [
                            {
                                "condition": current_properties.get(
                                    "condition"
                                ),  # special condition or not?
                                "values": filter_values.get(item.get("key")),
                            }
                        ]
                    }
                }
                return filters
                # .append
                ...

    def collect_input_filters():
        ...

    def get_toplevel_filtering_logic(self):
        return self.schema.get("filters_combination_operator")

    def collect_output_fields_mappings(self, schema):
        return schema.get("output_fields_mapping")

    def collect_filtering_conditions(self, schema):
        return schema.get("filtering_conditions")

    def validate_max_item_limit(self, current, input_json_values, field_name):
        if not current.get("max_item_limit"):
            pass

        if len(input_json_values) > current.get("max_item_limit"):
            self.error_handler(
                "Too many items given in field {}, maximum is {}.".format(
                    field_name, current.get("max_item_limit")
                )
            )

    def translate_conditions():
        def IN():
            ...

        def BETWEEN():
            ...

    def is_valid_json(self, schema):
        if not isinstance(schema, dict) or not schema:
            self.error_handler("Schema must be a non-empty dict.")

    def get_schema_version(self, schema):
        return schema.get("version")

    def get_number_of_filters(self):
        return len(self.input_json)

    def build_filters():
        ...

    def build_raw_filters():
        Ellipsis

    def get_general_filter_schema():
        ...

    def build_one_key_generic_filter(table_column, condition, values):
        ...
        {
            "filters": {
                "expr_annotation$chr": [{"condition": "between", "values": [1, 3]}]
            },
            "logic": "and",
        }

    def build_two_key_generic_filter(table_columns, condition, values):
        ...
        {
            "filters": {
                "expr_annotation$start": [{"condition": "between", "values": [1, 3]}],
                "expr_annotation$end": [{"condition": "between", "values": [5, 7]}],
            },
            "logic": "or",
        }

    def build_two_key_multi_condition_filter(
        table_column, first_key_condition, second_key_condition, values
    ):
        ...
        {
            "filters": {
                "expr_annotation$start": [{"condition": "less-than", "values": 100}],
                "expr_annotation$end": [{"condition": "greater-than", "values": 500}],
            },
            "logic": "and",
        }

    def build_partial_overlap_genobin_filters(
        self, filtering_condition, input_json_item
    ):
        if (
            "starting_position" not in filtering_condition["keys"]
            or "ending_position" not in filtering_condition["keys"]
        ):
            self.error_handler(
                "Error in location filtering. starting_position and ending_position must be defined in filtering conditions"
            )

        db_table_column_start = filtering_condition["table_column"]["starting_position"]
        db_table_column_end = filtering_condition["table_column"]["ending_position"]

        input_start_value = input_json_item["starting_position"]
        input_end_value = input_json_item["ending_position"]

        filter = {
            "logic": "or",
            "compound": [
                {
                    "filters": {
                        db_table_column_start: [
                            {
                                "condition": "between",
                                "values": [input_start_value, input_end_value],
                            }
                        ],
                        db_table_column_end: [
                            {
                                "condition": "between",
                                "values": [input_start_value, input_end_value],
                            }
                        ],
                    },
                    "logic": "or",
                },
                {
                    "filters": {
                        db_table_column_start: [
                            {"condition": "less-than", "values": input_start_value}
                        ],
                        db_table_column_end: [
                            {"condition": "greater-than", "values": input_end_value}
                        ],
                    },
                    "logic": "and",
                },
            ],
        }
