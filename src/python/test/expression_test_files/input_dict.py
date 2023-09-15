CLIEXPRESS_TEST_INPUT = {
    "malformed": {
        "location_end_before_start": {
            "location": [
                {"chromosome": "1", "starting_position": "200", "ending_position": "1"}
            ]
        },
        "expression_type": {
            "expression": ["shouldbedict"],
            "annotation": {"feature_id": ["ENSG0000001", "ENSG00000002"]},
        },
        "location_max_width": {
            "location": [
                {
                    "chromosome": "1",
                    "starting_position": "1",
                    "ending_position": "260000000",
                },
                {
                    "chromosome": "X",
                    "starting_position": "500",
                    "ending_position": "1700",
                },
            ],
            "expression": {"min_value": "10.2", "max_value": "10000"},
        },
        "bad_toplevel_key": {"not_real_key": "1", "sample_id": ["sample1"]},
        "location_missing_end": {
            "location": [{"chromosome": "1", "starting_position": "1"}]
        },
        "annotation_name_maxitem": {"annotation": {"feature_name": ["item"] * 101}},
        "conflicting_toplevel": {
            "location": [
                {
                    "chromosome": "1",
                    "starting_position": "10000",
                    "ending_position": "20000",
                },
                {
                    "chromosome": "X",
                    "starting_position": "500",
                    "ending_position": "1700",
                },
            ],
            "expression": {"min_value": "10.2", "max_value": "10000"},
            "annotation": {
                "feature_name": ["BRCA2"],
                "feature_id": ["ENSG0000001", "ENSG00000002"],
            },
        },
        "location_chrom_type": {
            "location": [
                {"chromosome": 1, "starting_position": "1", "ending_position": "200"}
            ]
        },
        "expression_max_type": {"expression": {"max_value": 200}},
        "expression_empty_dict": {"expression": {}},
        "annotation_name_type": {
            "annotation": {"feature_name": {"shouldnot": "bedict"}}
        },
        "annotation_type": {"annotation": ["list instead of dict"]},
        "location_end_type": {
            "location": [
                {"chromosome": "1", "starting_position": "1", "ending_position": 200}
            ]
        },
        "location_missing_start": {
            "location": [{"chromosome": "1", "ending_position": "200"}]
        },
        "annotation_conflicting_keys": {
            "annotation": {
                "feature_name": ["BRCA2"],
                "feature_id": ["ENSG0000001", "ENSG00000002"],
            }
        },
        "sample_id_maxitem": {"sample_id": ["item"] * 101},
        "expression_min_type": {"expression": {"min_value": 1}},
        "location_type": {"location": {"shouldbe": "alist"}},
        "annotation_id_maxitem": {"annotation": {"feature_id": ["item"] * 101}},
        "empty_dict": {},
        "location_missing_chr": {
            "location": [{"starting_position": "1", "ending_position": "200"}]
        },
        "bad_dependent_conditional": {
            "expression": {"min_value": "10.2", "max_value": "10000"}
        },
        "sample_id_type": {"sample_id": {"shouldbe": "alist"}},
        "annotation_id_type": {"annotation": {"feature_id": {"shouldnot": "bedict"}}},
        "location_start_type": {
            "location": [
                {"chromosome": "1", "starting_position": 1, "ending_position": "200"}
            ]
        },
        "location_item_type": {"location": [["shouldbedict"]]},
    },
    "valid": {
        "multi_location": {
            "location": [
                {"chromosome": "1", "starting_position": "1", "ending_position": "200"},
                {"chromosome": "2", "starting_position": "1", "ending_position": "500"},
                {
                    "chromosome": "10",
                    "starting_position": "1000",
                    "ending_position": "20000000",
                },
            ]
        },
        "annotation_feature_id": {
            "annotation": {"feature_id": ["ENSG0000001", "ENSG00000002"]}
        },
        "expression_min_only": {
            "expression": {"min_value": "1"},
            "annotation": {"feature_id": ["ENSG0000001", "ENSG00000002"]},
        },
        "expression_min_and_max": {
            "expression": {"min_value": "1", "max_value": "200"},
            "annotation": {"feature_id": ["ENSG0000001", "ENSG00000002"]},
        },
        "single_location": {
            "location": [
                {"chromosome": "1", "starting_position": "1", "ending_position": "200"}
            ]
        },
        "annotation_feature_name": {"annotation": {"feature_name": ["BRCA2"]}},
        "dependent_conditional_annotation": {
            "expression": {"min_value": "10.2", "max_value": "10000"},
            "annotation": {"feature_name": ["BRCA2"]},
        },
        "dependent_conditional_location": {
            "location": [
                {
                    "chromosome": "1",
                    "starting_position": "10000",
                    "ending_position": "20000",
                },
                {
                    "chromosome": "X",
                    "starting_position": "500",
                    "ending_position": "1700",
                },
            ],
            "expression": {"min_value": "10.2", "max_value": "10000"},
        },
        "expression_max_only": {
            "expression": {"max_value": "200"},
            "annotation": {"feature_id": ["ENSG0000001", "ENSG00000002"]},
        },
    },
}
