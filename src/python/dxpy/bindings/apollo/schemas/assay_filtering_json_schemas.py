EXTRACT_ASSAY_EXPRESSION_JSON_SCHEMA = {
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
        "properties": {"min_value": {"type": (int, float)}, "max_value": {"type": (int, float)}},
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
