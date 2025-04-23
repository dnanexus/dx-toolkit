### Example of queries, schema name and respective payloads

# location (EXTRACT_ASSAY_EXPRESSION_FILTERING_CONDITIONS_1_1_non_optimized)
{
    """dx extract_assay expression record-GzFzfXj09Y2YJ1FxBvKv4BQ8 --retrieve-expression -j '{"location":[{"chromosome": "1","starting_position": "1","ending_position": "193723800"}]}' -o -""": {
        "project_context": "project-GyzGq4009Y2ZvJVy4qjBPbZB",
        "fields": [
            {"feature_id": "expression$feature_id"},
            {"sample_id": "expression$sample_id"},
            {"expression": "expression$value"},
        ],
        "return_query": False,
        "raw_filters": {
            "assay_filters": {
                "name": "test_assay",
                "id": "8007ba88-c964-4395-bbc4-a7e3c4a2059f",
                "logic": "and",
                "compound": [
                    {
                        "logic": "or",
                        "compound": [
                            {
                                "logic": "and",
                                "compound": [
                                    {
                                        "filters": {
                                            "expr_annotation$CHROM": [
                                                {"condition": "is", "values": "1"}
                                            ]
                                        }
                                    },
                                    {
                                        "logic": "or",
                                        "compound": [
                                            {
                                                "filters": {
                                                    "expr_annotation$start": [
                                                        {
                                                            "condition": "between",
                                                            "values": [1, 193723800],
                                                        }
                                                    ],
                                                    "expr_annotation$end": [
                                                        {
                                                            "condition": "between",
                                                            "values": [1, 193723800],
                                                        }
                                                    ],
                                                },
                                                "logic": "or",
                                            },
                                            {
                                                "filters": {
                                                    "expr_annotation$start": [
                                                        {
                                                            "condition": "less-than-eq",
                                                            "values": 1,
                                                        }
                                                    ],
                                                    "expr_annotation$end": [
                                                        {
                                                            "condition": "greater-than-eq",
                                                            "values": 193723800,
                                                        }
                                                    ],
                                                },
                                                "logic": "and",
                                            },
                                        ],
                                    },
                                ],
                            }
                        ],
                    }
                ],
            }
        },
        "order_by": [{"feature_id": "asc"}, {"sample_id": "asc"}],
    }
}

# location, sample_id, additional fields - strand (EXTRACT_ASSAY_EXPRESSION_FILTERING_CONDITIONS_1_1_non_optimized)
{
    """dx extract_assay expression record-GzFzfXj09Y2YJ1FxBvKv4BQ8 --retrieve-expression -j '{"location":[{"chromosome": "1","starting_position": "1","ending_position": "193723800"}],"sample_id":["sample_1"]}' -o - --additional-fields strand""": {
        "project_context": "project-GyzGq4009Y2ZvJVy4qjBPbZB",
        "fields": [
            {"feature_id": "expression$feature_id"},
            {"sample_id": "expression$sample_id"},
            {"expression": "expression$value"},
            {"strand": "expr_annotation$strand"},
        ],
        "return_query": False,
        "raw_filters": {
            "assay_filters": {
                "name": "test_assay",
                "id": "8007ba88-c964-4395-bbc4-a7e3c4a2059f",
                "logic": "and",
                "compound": [
                    {
                        "logic": "or",
                        "compound": [
                            {
                                "logic": "and",
                                "compound": [
                                    {
                                        "filters": {
                                            "expr_annotation$CHROM": [
                                                {"condition": "is", "values": "1"}
                                            ]
                                        }
                                    },
                                    {
                                        "logic": "or",
                                        "compound": [
                                            {
                                                "filters": {
                                                    "expr_annotation$start": [
                                                        {
                                                            "condition": "between",
                                                            "values": [1, 193723800],
                                                        }
                                                    ],
                                                    "expr_annotation$end": [
                                                        {
                                                            "condition": "between",
                                                            "values": [1, 193723800],
                                                        }
                                                    ],
                                                },
                                                "logic": "or",
                                            },
                                            {
                                                "filters": {
                                                    "expr_annotation$start": [
                                                        {
                                                            "condition": "less-than-eq",
                                                            "values": 1,
                                                        }
                                                    ],
                                                    "expr_annotation$end": [
                                                        {
                                                            "condition": "greater-than-eq",
                                                            "values": 193723800,
                                                        }
                                                    ],
                                                },
                                                "logic": "and",
                                            },
                                        ],
                                    },
                                ],
                            }
                        ],
                    },
                    {
                        "filters": {
                            "expression$sample_id": [
                                {"condition": "in", "values": ["sample_1"]}
                            ]
                        }
                    },
                ],
            }
        },
        "order_by": [{"feature_id": "asc"}, {"sample_id": "asc"}],
    }
}

# annotation, feature_id, additional fields - chrom (EXTRACT_ASSAY_EXPRESSION_FILTERING_CONDITIONS_1_1)
{
    """ dx extract_assay expression record-GzFzfXj09Y2YJ1FxBvKv4BQ8 --retrieve-expression -j '{"annotation":{"feature_id": ["ENSG00000160180"]}}' -o - --additional-fields chrom """: {
        "project_context": "project-GyzGq4009Y2ZvJVy4qjBPbZB",
        "fields": [
            {"feature_id": "expression_read_optimized$feature_id"},
            {"sample_id": "expression_read_optimized$sample_id"},
            {"expression": "expression_read_optimized$value"},
        ],
        "return_query": False,
        "raw_filters": {
            "assay_filters": {
                "name": "test_assay",
                "id": "8007ba88-c964-4395-bbc4-a7e3c4a2059f",
                "logic": "and",
                "compound": [
                    {
                        "filters": {
                            "expression_read_optimized$feature_id": [
                                {"condition": "in", "values": ["ENSG00000160180"]}
                            ]
                        }
                    }
                ],
            }
        },
        "order_by": [{"feature_id": "asc"}, {"sample_id": "asc"}],
    }
}

# annotation, feature_name (EXTRACT_ASSAY_EXPRESSION_FILTERING_CONDITIONS_1_1)
{
    """ dx extract_assay expression record-Gz7VfJ009Y2fgkjpfVZFQB1J --retrieve-expression -j '{"annotation":{"feature_name":["CMTM1","CES1P2"]}}' -o - """: {
        "project_context": "project-GyzGq4009Y2ZvJVy4qjBPbZB",
        "fields": [
            {"feature_id": "expression_read_optimized$feature_id"},
            {"sample_id": "expression_read_optimized$sample_id"},
            {"expression": "expression_read_optimized$value"},
        ],
        "return_query": False,
        "raw_filters": {
            "assay_filters": {
                "name": "test_assay",
                "id": "8007ba88-c964-4395-bbc4-a7e3c4a2059f",
                "logic": "and",
                "compound": [
                    {
                        "filters": {
                            "expression_read_optimized$gene_name": [
                                {"condition": "in", "values": ["CMTM1", "CES1P2"]}
                            ]
                        }
                    }
                ],
            }
        },
        "order_by": [{"feature_id": "asc"}, {"sample_id": "asc"}],
    }
}


vizserver_raw_filters = {
    "logic": "and",
    "compound": [
        {
            "filters": {
                "expr_annotation$gene_name": [
                    {"condition": "in", "values": ["CMTM1", "CES1P2"]}
                ]
            }
        }
    ],
}
