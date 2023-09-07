from __future__ import print_function


class InputJSONFiltersValidator(object):
    """
    A specialized class that parses input JSON according to a schema to prepare vizserver-compliant compound filters.
    """

    def __init__(self, input_json, schema, error_handler=print):
        self.input_json = input_json
        self.schema = schema
        self.error_handler = error_handler
        self.condition_function_mapping = {
            "genobin_partial_overlap": self.build_partial_overlap_genobin_filters,
        }
        self.SUPPORTED_VIZSERVER_CONDITIONS = [
            "contains",
            "exists",
            "not-exists",
            "any",
            "not-any",
            "all",
            "not-empty",
            "in",
            "not-in",
            "is",
            "is-not",
            "greater-than",
            "less-than",
            "greater-than-eq",
            "less-than-eq",
            "between",
            "between-ex",
            "between-left-inc",
            "between-right-inc",
            "not-between",
            "not-between-ex",
            "not-between-left-inc",
            "not-between-right-inc",
            "compare-before",
            "compare-after",
            "compare-within",
        ]

    def parse(self):
        self.is_valid_json(self.schema)
        if self.get_schema_version(self.schema) == "1.0":
            self.parse_v1()
        else:
            raise NotImplementedError

    def parse_v1(self):
        """ """
        vizserver_compound_filters = {
            "logic": self.get_toplevel_filtering_logic(),
            "compound": [],
        }

        # Get 'filtering_conditions' from the schema
        all_filters = self.collect_filtering_conditions(self.schema)

        # Looking at the input_json
        # go through each key: location, annotation, expression, sample.
        for filter_key, filter_values in self.input_json.items():
            # filter_key -> location
            # filter_values -> list of dicts
            if filter_key not in all_filters:
                self.error_handler(
                    "No filtering condition was defined for {} in the schema.".format(
                        filter_key
                    )
                )

            # Filtering conditions for the current "key" in input_json
            current_filters = all_filters[filter_key]
            current_properties = current_filters.get("properties")

            # Validate max number of allowed items if max_item_limit is defined at the top level within key
            # It will be later validated for each property as well
            self.validate_max_item_limit(current_filters, filter_values, filter_key)

            # must apply to keys within properties too
            # self.validate_max_item_limit(current_properties, filter_values, filter_key)

            # There are several ways filtering_conditions can be defined
            # 1. Basic use-case: no properties, just condition (see 'sample_id' in 'EXTRACT_ASSAY_EXPRESSION_FILTERING_CONDITIONS')
            # 2. Properties defined as dict of dicts (see 'annotation' and 'expression')
            # 3. Properties defined as list of dicts (more advanced, special use-case with complex conditional logics that needs translation)
            if isinstance(type(current_properties), list) and isinstance(
                type(filter_values), list
            ):
                # multi-condition if list of dicts
                # must be compounded because more than one filter
                # must be recursed if more than one item in location

                # check number of properties

                base_filter_for_each_item = {
                    "logic": current_filters.get("filters_combination_operator"),
                    "compound": [
                        # {},
                        # {},
                    ]  # as many dicts inside as there are key conditions
                    # so count the location.properties for this
                }

                full_filter_for_all_items = {
                    "logic": current_filters.get("items_combination_operator"),
                    "compound": [
                        {base_filter_for_each_item},
                        {},
                    ]  # as many dicts as there are "items"
                    # count json list len for this
                }

                ### current_properties is a list of dicts and each dict is a filter
                ### therefore, we need to iterate over each dict and build a filter for each one
                ### However, also remember than inside filter_values from input_json we have a list of dicts
                ### therefore there might be more than one element within that list
                ### so we need to build the filters for each element in the list as well
                ### and then append them to the compound list of dicts
                ### Here is the full code to do that:

                # Build filters for each item in the list (input_json.filter_values)
                for current_list_item in filter_values:
                    # CONSIDER keeping track of input_json keys and properties so far verified and used

                    current_compound_filter = base_filter_for_each_item.copy()
                    ## now if key is found in properties['key'] then it's simple
                    ## but if key is found in properties['keys'] then it's more complex
                    ## collect filter_values keys
                    ## just look at properties, get key or keys and get values from current_list_item
                    ## also check if key is defined in current_list_item

                    for item in current_properties:
                        if item.get("key"):
                            # might need a separate check for list type
                            temp_filter = self.build_one_key_generic_filter(
                                item["table_column"],
                                item[
                                    "condition"
                                ],  # consider verifying conditions according to a predefined list
                                current_list_item[item.get("key")],
                            )

                            current_compound_filter["compound"].append(temp_filter)

                        if item.get("keys"):
                            if len(item.get("keys")) == 2:
                                if (
                                    item.get("condition")
                                    not in self.SUPPORTED_VIZSERVER_CONDITIONS
                                ):
                                    special_filtering_function = (
                                        self.condition_function_mapping[
                                            item.get("condition")
                                        ]
                                    )
                                    special_filtering_function(item, current_list_item)

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

                # check if min_ and max_ are both provided
                # check if there's a single key

            if current_properties is None:
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

                # just use the generic filter_builder function
                self.build_one_key_generic_filter(table_column, condition, values)
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
        max_item_limit = current.get("max_item_limit")
        if not max_item_limit:
            pass

        if len(input_json_values) > max_item_limit:
            self.error_handler(
                "Too many items given in field {}, maximum is {}.".format(
                    field_name, max_item_limit
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

    def build_one_key_generic_filter(table_column_mapping, condition, value):
        """
        {
                "filters": {"expr_annotation$chr": [{"condition": "is", "values": 5}]},
        }
        """
        return {
            "filters": {
                table_column_mapping: [{"condition": condition, "values": value}]
            }
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
            or "starting_position" not in input_json_item
            or "ending_position" not in input_json_item
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
