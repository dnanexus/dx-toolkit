EXTRACT_ASSAY_EXPRESSION_FILTERING_CONDITIONS_1_0 = {
    "version": "1.0",
    "output_fields_mapping": {
        "default": [
            {"feature_id": "expression$feature_id"},
            {"sample_id": "expression$sample_id"},
            {"expression": "expression$value"},
        ],
        "additional": [
            {"feature_name": "expr_annotation$gene_name"},
            {"chrom": "expr_annotation$chr"},
            {"start": "expr_annotation$start"},
            {"end": "expr_annotation$end"},
            {"strand": "expr_annotation$strand"},
        ],
    },
    "filtering_conditions": {
        "location": {
            "items_combination_operator": "or",
            "filters_combination_operator": "and",
            "max_item_limit": 10,
            "properties": [
                {
                    "key": "chromosome",
                    "condition": "is",
                    "table_column": "expr_annotation$chr",
                },
                {
                    "keys": ["starting_position", "ending_position"],
                    "condition": "genobin_partial_overlap",
                    "max_range": "250",
                    "table_column": {
                        "starting_position": "expr_annotation$start",
                        "ending_position": "expr_annotation$end",
                    },
                },
            ],
        },
        "annotation": {
            "properties": {
                "feature_id": {
                    "max_item_limit": 100,
                    "condition": "in",
                    "table_column": "expr_annotation$feature_id",
                },
                "feature_name": {
                    "max_item_limit": 100,
                    "condition": "in",
                    "table_column": "expr_annotation$gene_name",
                },
            }
        },
        "expression": {
            "filters_combination_operator": "and",
            "properties": {
                "min_value": {
                    "condition": "greater-than-eq",
                    "table_column": "expression$value",
                },
                "max_value": {
                    "condition": "less-than-eq",
                    "table_column": "expression$value",
                },
            },
        },
        "sample_id": {
            "max_item_limit": 100,
            "condition": "in",
            "table_column": "expression$sample_id",
        },
    },
    "filters_combination_operator": "and",
    "order_by": [
        {"feature_id": "asc"},
        {"sample_id": "asc"},
    ],
}

# EXTRACT_ASSAY_EXPRESSION_FILTERING_CONDITIONS_1_1 does not handle optimization of location filters
EXTRACT_ASSAY_EXPRESSION_FILTERING_CONDITIONS_1_1 = {
    "version": "1.1",
    "output_fields_mapping": {
        "default": [
            {"feature_id": "expression_read_optimized$feature_id"},
            {"sample_id": "expression_read_optimized$sample_id"},
            {"expression": "expression_read_optimized$value"},
        ],
        "additional": [
            {"feature_name": "expression_read_optimized$gene_name"},
            {"chrom": "expression_read_optimized$CHROM"},
            {"start": "expression_read_optimized$start"},
            {"end": "expression_read_optimized$end"},
            {"strand": "expression_read_optimized$strand"},
        ],
    },
    "filtering_conditions": {
        "annotation": {
            "properties": {
                "feature_id": {
                    "max_item_limit": 100,
                    "condition": "in",
                    "table_column": "expression_read_optimized$feature_id",
                },
                "feature_name": {
                    "max_item_limit": 100,
                    "condition": "in",
                    "table_column": "expression_read_optimized$gene_name",
                },
            }
        },
        "expression": {
            "filters_combination_operator": "and",
            "properties": {
                "min_value": {
                    "condition": "greater-than-eq",
                    "table_column": "expression_read_optimized$value",
                },
                "max_value": {
                    "condition": "less-than-eq",
                    "table_column": "expression_read_optimized$value",
                },
            },
        },
        "sample_id": {
            "max_item_limit": 100,
            "condition": "in",
            "table_column": "expression_read_optimized$sample_id",
        },
    },
    "filters_combination_operator": "and",
    "order_by": [
        {"feature_id": "asc"},
        {"sample_id": "asc"},
    ],
}

EXTRACT_ASSAY_EXPRESSION_FILTERING_CONDITIONS_1_1_non_optimized = {
    "version": "1.1",
    "output_fields_mapping": {
        "default": [
            {"feature_id": "expression$feature_id"},
            {"sample_id": "expression$sample_id"},
            {"expression": "expression$value"},
        ],
        "additional": [
            {"feature_name": "expr_annotation$gene_name"},
            {"chrom": "expr_annotation$CHROM"},
            {"start": "expr_annotation$start"},
            {"end": "expr_annotation$end"},
            {"strand": "expr_annotation$strand"},
        ],
    },
    "filtering_conditions": {
        "location": {
            "items_combination_operator": "or",
            "filters_combination_operator": "and",
            "max_item_limit": 10,
            "properties": [
                {
                    "key": "chromosome",
                    "condition": "is",
                    "table_column": "expr_annotation$CHROM",
                },
                {
                    "keys": ["starting_position", "ending_position"],
                    "condition": "genobin_partial_overlap",
                    "max_range": "250",
                    "table_column": {
                        "starting_position": "expr_annotation$start",
                        "ending_position": "expr_annotation$end",
                    },
                },
            ],
        },
        "annotation": {
            "properties": {
                "feature_id": {
                    "max_item_limit": 100,
                    "condition": "in",
                    "table_column": "expr_annotation$feature_id",
                },
                "feature_name": {
                    "max_item_limit": 100,
                    "condition": "in",
                    "table_column": "expr_annotation$gene_name",
                },
            }
        },
        "expression": {
            "filters_combination_operator": "and",
            "properties": {
                "min_value": {
                    "condition": "greater-than-eq",
                    "table_column": "expression$value",
                },
                "max_value": {
                    "condition": "less-than-eq",
                    "table_column": "expression$value",
                },
            },
        },
        "sample_id": {
            "max_item_limit": 100,
            "condition": "in",
            "table_column": "expression$sample_id",
        },
    },
    "filters_combination_operator": "and",
    "order_by": [
        {"feature_id": "asc"},
        {"sample_id": "asc"},
    ],
}
