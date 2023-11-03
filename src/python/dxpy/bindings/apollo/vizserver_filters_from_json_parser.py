from __future__ import print_function


class JSONFiltersValidator(object):
    """
    A specialized class that parses user input JSON according to a schema to prepare vizserver-compliant compound filters.

    See assay_filtering_conditions.py for current schemas.

    Filters must be defined in schema["filtering_conditions"]

    There are currently three ways to define filtering_conditions when version is 1.0:
    1. Basic use-case: no "properties" are defined. Only "condition", "table_column" and optionally "max_item_limit" are defined.
                       In this case, there are no sub-keys for the current key in input_json.
                       (see 'sample_id' in 'assay_filtering_conditions.EXTRACT_ASSAY_EXPRESSION_FILTERING_CONDITIONS')
    2. Properties defined as dict of dicts (see 'annotation' and 'expression' in EXTRACT_ASSAY_EXPRESSION_FILTERING_CONDITIONS)
        2.1. If filters_combination_operator is not defined, the assumption is that there is only one sub-key in input_json

    3. Complex use-case: Properties defined as list of dicts (more advanced, special use-case with complex conditional logics that needs translation)
        Filtering conditions defined this way indicate that input_json may contain more than one item for the current key, and elements are stored in a list.
        In this case, the schema must define "items_combination_operator" and "filters_combination_operator".
        items_combination_operator: how to combine filters for list items in input_json
        filters_combination_operator: how to combine filters within each item
        (see 'location' in EXTRACT_ASSAY_EXPRESSION_FILTERING_CONDITIONS)

        Within 'properties' if "key" is defined, then a generic one-key filter is built.
        If "keys" is defined, then more complex use-cases are handled via special conditions that are defined in condition_function_mapping.

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
            return self.parse_v1()
        else:
            raise NotImplementedError

    def parse_v1(self):
        """
        Parse input_json according to schema version 1.0, and build vizserver compound filters.
        """

        try:
            # Get the general structure of vizserver-compliant compound filter dict
            vizserver_compound_filters = self.get_vizserver_basic_filter_structure()

            # Get 'filtering_conditions' from the schema
            all_filters = self.collect_filtering_conditions(self.schema)

            # Go through the input_json (iterate through keys and values in user input JSON)
            for filter_key, filter_values in self.input_json.items():
                # Example: if schema is EXTRACT_ASSAY_EXPRESSION_FILTERING_CONDITIONS
                # Then input JSON would probably contain location or annotation
                # In this case:
                # filter_key -> location
                # filter_values -> list of dicts (where each dict contains "chromosome", "starting_position", "ending_position")
                if filter_key not in all_filters:
                    self.error_handler(
                        "No filtering condition was defined for {} in the schema.".format(
                            filter_key
                        )
                    )

                # Get the filtering conditions for the current "key" in input_json
                current_filters = all_filters[filter_key]

                # Get the properties if any (this might be None,
                # if so we will just execute the basic use case defined in the docstring of the class)
                current_properties = current_filters.get("properties")

                # Validate max number of allowed items
                # if max_item_limit is defined at the top level within this key
                # It will be later validated for each property as well

                self.validate_max_item_limit(current_filters, filter_values, filter_key)

                # There are several ways filtering_conditions can be defined
                # 1. Basic use-case: no properties, just condition (see 'sample_id' in 'EXTRACT_ASSAY_EXPRESSION_FILTERING_CONDITIONS')
                # 2. Properties defined as dict of dicts (see 'annotation' and 'expression')
                # 3. Properties defined as list of dicts (more advanced, special use-case with complex conditional logics that needs translation)
                # For more information see the docstring of the class
                # The following if conditions will go through each of the aforementioned scenarios

                if isinstance(current_properties, list) and isinstance(
                    filter_values, list
                ):
                    filters = self.parse_list_v1(
                        current_filters,
                        filter_values,
                        current_properties,
                    )

                    if filters is not None:
                        vizserver_compound_filters["compound"].append(filters)
                        filters = None

                if isinstance(current_properties, dict):
                    for k, v in current_properties.items():
                        if k in filter_values:
                            self.validate_max_item_limit(v, filter_values[k], k)

                    filters = self.parse_dict_v1(
                        current_filters, filter_values, current_properties
                    )
                    if filters is not None:
                        vizserver_compound_filters["compound"].append(filters)
                        filters = None

                if current_properties is None:
                    # no properties, so just apply conditions
                    # In other words .get("properties") returns None
                    # Therefore we are dealing with a basic use-case scenario
                    # (See 'sample_id' in 'EXTRACT_ASSAY_EXPRESSION_FILTERING_CONDITIONS' for an example)
                    filters = self.build_one_key_generic_filter(
                        current_filters.get("table_column"),
                        current_filters.get("condition"),
                        filter_values,
                    )

                    if filters is not None:
                        vizserver_compound_filters["compound"].append(filters)
                        filters = None

            return vizserver_compound_filters

        except Exception as e:
            self.error_handler(str(e))

    def parse_list_v1(self, current_filters, filter_values, current_properties):
        if current_properties is None:
            self.error_handler("Expected properties to be defined within schema.")

        # There are two important aspects:
        # Filters (if more than one) must be compounded for each item with filters_combination_operator logic
        # All items must be compounded together as one large compounded filter with the logic defined in items_combination_operator

        full_filter_for_all_items = {
            "logic": current_filters.get("items_combination_operator"),
            "compound": [
                # {base_filter_for_each_item_1},
                # {base_filter_for_each_item_2},
                # {etc.}
                # as many dicts as there are "items" in input_json[filter_key]
            ],
        }

        ### current_properties is a list of dicts and each dict is a filter
        ### therefore, we need to iterate over each dict and build a filter for each one
        ### However, also remember than inside filter_values from input_json we have a list of dicts
        ### therefore there might be more than one element within that list
        ### so we need to build the filters for each element in the list as well
        ### and then append them to the compound list of dicts

        # Build filters for each item in the list (input_json[filter_values])
        for current_list_item in filter_values:
            # Consider keeping track of input_json keys and properties evaluated and used so far

            current_compound_filter = {
                "logic": current_filters.get("filters_combination_operator"),
                "compound": [
                    # {},
                    # {},
                    # here there will be as many dicts inside as there are qualifying [key] conditions
                ],
            }

            ## properties['key'] -> simple case
            ## properties['keys'] -> reserved for complex, special cases
            ## specifically for those cases where SUPPORTED_VIZSERVER_CONDITIONS is not sufficient

            for item in current_properties:
                if item.get("key"):
                    # Consider a separate check for list type
                    # Currently not necessary
                    temp_filter = self.build_one_key_generic_filter(
                        item["table_column"],
                        item["condition"],
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
                                self.condition_function_mapping[item.get("condition")]
                            )
                            temp_filter = special_filtering_function(
                                item, current_list_item
                            )
                            current_compound_filter["compound"].append(temp_filter)

            # Consider checking if current_compound_filter contains any new elements before appending
            full_filter_for_all_items["compound"].append(current_compound_filter)

        return full_filter_for_all_items

    def parse_dict_v1(self, current_filters, filter_values, current_properties):
        if current_properties is None:
            self.error_handler("Expected properties to be defined within schema.")

        # if no filters_combination_operator is specified, then single filter assumed

        filtering_logic = current_filters.get("filters_combination_operator")

        # if filtering_logic isn't defined at this level then there must be only one 'key' to filter on
        if filtering_logic:
            if len(filter_values) == 1:
                temp_key = next(iter(filter_values.keys()))
                matched_filter = current_properties[temp_key]
                temp_filter = self.build_one_key_generic_filter(
                    matched_filter.get("table_column"),
                    matched_filter.get("condition"),
                    filter_values.get(temp_key),
                )
                return temp_filter

            if len(filter_values) == 2:
                temp_keys = list(filter_values.keys())
                first_key = current_properties[temp_keys[0]]
                second_key = current_properties[temp_keys[1]]

                # check if there are two filtering conditions that need to be applied on the same table_column
                # check if both of those are defined in input_json
                # an example of such a case is 'expression' in EXTRACT_ASSAY_EXPRESSION_FILTERING_CONDITIONS
                # where we might have a `max_value` and a `min_value`
                # however providing both is not mandatory

                if first_key.get("table_column") == second_key.get("table_column"):
                    if filtering_logic == "and":
                        # where possible we will use "between" operator
                        # instead of defining two separate conditions with greater-than AND less-than
                        temp_condition = self.convert_to_between_operator(
                            [
                                first_key.get("condition"),
                                second_key.get("condition"),
                            ]
                        )
                        if temp_condition in ["between", "between-ex"]:
                            temp_values = [
                                filter_values.get(temp_keys[0]),
                                filter_values.get(temp_keys[1]),
                            ]
                            temp_values.sort()
                            temp_filter = self.build_one_key_generic_filter(
                                first_key.get("table_column"),
                                temp_condition,
                                temp_values,
                            )
                            return temp_filter

                        else:
                            # A more complex conversion scenario
                            # that may not involve greater-than/less-than operators
                            # This is currently neither implemented nor needed
                            raise NotImplementedError

                    if filtering_logic == "or":
                        # A special edge case that should probably never be used
                        # It is also not supported by vizserver
                        # In other words, 'or' logic is not supported for
                        # values for the same key in the filter.
                        # If "or" logic needs to be applied to two filters
                        # both with the same 'key', consider constructing two separate
                        # filters and then compounding them with "or" logic
                        raise NotImplementedError

                else:
                    # This is currently not needed anywhere
                    raise NotImplementedError

        else:
            # There's no filtering logic, in other words, filters_combination_operator is not defined at this level
            # (see 'annotation' in 'EXTRACT_ASSAY_EXPRESSION_FILTERING_CONDITIONS' for an example)
            if len(current_properties) > 1:
                if len(filter_values) > 1:
                    # if there are also more than 1 in input_json
                    self.error_handler(
                        "More than one filter found at this level, but no filters_combination_operator was specified."
                    )
                else:
                    temp_key = next(iter(filter_values.keys()))
                    matched_filter = current_properties[temp_key]
                    temp_filter = self.build_one_key_generic_filter(
                        matched_filter.get("table_column"),
                        matched_filter.get("condition"),
                        filter_values.get(temp_key),
                    )
                return temp_filter

    def get_vizserver_basic_filter_structure(self):
        return {
            "logic": self.get_toplevel_filtering_logic(),
            "compound": [],
        }

    def collect_input_filters(self):
        return self.input_json.keys()

    def get_toplevel_filtering_logic(self):
        return self.schema.get("filters_combination_operator")

    def collect_output_fields_mappings(self, schema):
        return schema.get("output_fields_mapping")

    def collect_filtering_conditions(self, schema):
        return schema.get("filtering_conditions")

    def validate_max_item_limit(self, current, input_json_values, field_name):
        max_item_limit = current.get("max_item_limit")
        if not max_item_limit:
            return True

        if len(input_json_values) > max_item_limit:
            self.error_handler(
                "Too many items given in field {}, maximum is {}.".format(
                    field_name, max_item_limit
                )
            )

    def is_valid_json(self, schema):
        if not isinstance(schema, dict) or not schema:
            self.error_handler("Schema must be a non-empty dict.")

    def get_schema_version(self, schema):
        return schema.get("version")

    def get_number_of_filters(self):
        return len(self.input_json)

    def convert_to_between_operator(self, operators_list):
        if len(operators_list) != 2:
            self.error_handler("Expected exactly two operators")

        if set(operators_list) == {"greater-than-eq", "less-than-eq"}:
            return "between"

        if set(operators_list) == {"greater-than", "less-than"}:
            return "between-ex"

    def build_one_key_generic_filter(
        self, table_column_mapping, condition, value, return_complete_filter=True
    ):
        """
        {
                "filters": {"expr_annotation$chr": [{"condition": "is", "values": 5}]},
        }
        """
        base_filter = {
            table_column_mapping: [{"condition": condition, "values": value}]
        }

        if return_complete_filter:
            return {"filters": base_filter}
        else:
            return base_filter

    def build_partial_overlap_genobin_filters(
        self, filtering_condition, input_json_item
    ):
        """

        Returns a nested compound filter in the following format:

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

        """
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

        input_start_value = int(input_json_item["starting_position"])
        input_end_value = int(input_json_item["ending_position"])

        start_filter = self.build_one_key_generic_filter(
            db_table_column_start,
            "between",
            [input_start_value, input_end_value],
            return_complete_filter=False,
        )
        end_filter = self.build_one_key_generic_filter(
            db_table_column_end,
            "between",
            [input_start_value, input_end_value],
            return_complete_filter=False,
        )
        compound_start_filter = self.build_one_key_generic_filter(
            db_table_column_start,
            "less-than-eq",
            input_start_value,
            return_complete_filter=False,
        )

        compound_end_filter = self.build_one_key_generic_filter(
            db_table_column_end,
            "greater-than-eq",
            input_end_value,
            return_complete_filter=False,
        )

        filter_structure = {
            "logic": "or",
            "compound": [
                {
                    "filters": {},
                    "logic": "or",
                },
                {
                    "filters": {},
                    "logic": "and",
                },
            ],
        }

        filter_structure["compound"][0]["filters"].update(start_filter)
        filter_structure["compound"][0]["filters"].update(end_filter)
        filter_structure["compound"][1]["filters"].update(compound_start_filter)
        filter_structure["compound"][1]["filters"].update(compound_end_filter)

        # In Python 3, this can be simply done via:

        # return {
        #     "logic": "or",
        #     "compound": [
        #         {
        #             "filters": {
        #                 **start_filter,
        #                 **end_filter,
        #             },
        #             "logic": "or",
        #         },
        #         {
        #             "filters": {
        #                 **compound_start_filter,
        #                 **compound_end_filter,
        #             },
        #             "logic": "and",
        #         },
        #     ],
        # }

        return filter_structure
