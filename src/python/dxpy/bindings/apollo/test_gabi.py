official_payload = (
    {
        "fields": {"column": "dx_patients$dx_subject_id"},
        "filters": {
            "pheno_filters": {
                "compound": [
                    {
                        "name": "phenotype",
                        "logic": "and",
                        "filters": {
                            "dx_patients$dx_gender": [
                                {"condition": "in", "values": ["F"]}
                            ]
                        },
                        "entity": {
                            "logic": "and",
                            "name": "dx_patients",
                            "operator": "exists",
                            "children": [],
                        },
                    }
                ],
                "logic": "and",
            },
            "assay_filters": {"compound": [], "logic": "and"},
            "logic": "and",
        },
        "project_context": "project-GkFY71j0ZgZfYyQykP9fVvPF",
    },
)

vizserver_compound_filters = {
    "logic": "and",
    "compound": [
        {
            "filters": {
                "feature_lookup$feature_id": [
                    {"condition": "in", "values": ["ENSG00000160180"]}
                ]
            }
        }
    ],
}

vizserver_compound_filters = {"logic": "and", "compound": []}


all_filters = {
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
                "table_column": "feature_lookup$feature_id",
            },
            "feature_name": {
                "max_item_limit": 100,
                "condition": "in",
                "table_column": "feature_lookup$synonyms",
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
}

sovq = {
    "fields": {"column": "sample$sample_id"},
    "filters": {
        "pheno_filters": {
            "compound": [
                {
                    "name": "phenotype",
                    "logic": "and",
                    "filters": {},
                    "entity": {
                        "logic": "and",
                        "name": "sample",
                        "operator": "exists",
                        "children": [],
                    },
                }
            ],
            "logic": "and",
        },
        "assay_filters": {
            "compound": [
                {
                    "id": "ea47603a-d820-415c-b287-b5ff1dcbb56e",
                    "group_condition": "in",
                    "compound": [
                        {
                            "name": "somatic@93ea100a-6116-4994-af33-b2df9ab2db22",
                            "logic": "and",
                            "filters": {
                                "variant_read_optimized$SYMBOL": [
                                    {
                                        "condition": "any",
                                        "values": ["BRCA1"],
                                        "geno_bins": [
                                            {
                                                "chr": "17",
                                                "start": "43044295",
                                                "end": "43170245",
                                            }
                                        ],
                                    }
                                ],
                                "variant_read_optimized$variant_type": [
                                    {"condition": "in", "values": ["SNP"]}
                                ],
                            },
                        }
                    ],
                    "logic": "and",
                }
            ],
            "logic": "and",
            "filter_version": "2.0",
        },
        "logic": "and",
    },
    "raw_filters": {
        "assay_filters": {
            "id": "ea47603a-d820-415c-b287-b5ff1dcbb56e",
            "name": "assay_automated_test",
            "filters": {
                "variant_read_optimized$tumor_normal": [
                    {"condition": "in", "values": ["tumor"]}
                ]
            },
            "logic": "and",
        },
        "logic": "and",
    },
    "project_context": "project-G5Bzk5806j8V7PXB678707bv",
}


vizserver_payload = {
    "project_context": "project-GyzGq4009Y2ZvJVy4qjBPbZB",
    "fields": [
        {"feature_id": "expression_read_optimized$feature_id"},
        {"sample_id": "expression_read_optimized$sample_id"},
        {"expression": "expression_read_optimized$value"},
        {"start": "expression_read_optimized$start"},
    ],
    "return_query": False,
    "raw_filters": {
        "assay_filters": {
            "name": "test_assay",
            "id": "143ac7ab-b47a-4737-a6e1-e2d02a9047a5",
            "logic": "and",
            "compound": [
                {
                    "filters": {
                        "feature_lookup$feature_id": [
                            {"condition": "in", "values": ["ENSG00000160180"]}
                        ]
                    }
                }
            ],
        }
    },
    "order_by": [{"feature_id": "asc"}, {"sample_id": "asc"}],
}


sovq_intergenic_brca1_payload = {
    "fields": {"column": "sample$sample_id"},
    "filters": {
        "pheno_filters": {
            "compound": [
                {
                    "name": "phenotype",
                    "logic": "and",
                    "filters": {},
                    "entity": {
                        "logic": "and",
                        "name": "sample",
                        "operator": "exists",
                        "children": [],
                    },
                }
            ],
            "logic": "and",
        },
        "assay_filters": {
            "compound": [
                {
                    "id": "ea47603a-d820-415c-b287-b5ff1dcbb56e",
                    "group_condition": "in",
                    "compound": [
                        {
                            "name": "somatic@08fe0381-f9fb-4776-9360-b1cc974580a8",
                            "logic": "and",
                            "filters": {
                                "variant_read_optimized$canonical_consequences": [
                                    {
                                        "condition": "match-any",
                                        "values": ["^BRCA1.*(intergenic_variant)"],
                                        "geno_bins": [
                                            {
                                                "chr": "17",
                                                "start": "43044295",
                                                "end": "43170245",
                                            }
                                        ],
                                    }
                                ]
                            },
                        }
                    ],
                    "logic": "and",
                }
            ],
            "logic": "and",
            "filter_version": "2.0",
        },
        "logic": "and",
    },
    "raw_filters": {
        "assay_filters": {
            "id": "ea47603a-d820-415c-b287-b5ff1dcbb56e",
            "name": "assay_automated_test",
            "filters": {
                "variant_read_optimized$tumor_normal": [
                    {"condition": "in", "values": ["tumor"]}
                ]
            },
            "logic": "and",
        },
        "logic": "and",
    },
    "project_context": "project-G5Bzk5806j8V7PXB678707bv",
}


vizserver_payload = {
    "project_context": "project-GyzGq4009Y2ZvJVy4qjBPbZB",
    "fields": [
        {"feature_id": "expression_read_optimized$feature_id"},
        {"sample_id": "expression_read_optimized$sample_id"},
        {"expression": "expression_read_optimized$value"},
        {"start": "expression_read_optimized$start"},
    ],
    "return_query": False,
    "raw_filters": {
        "assay_filters": {
            "name": "test_assay",
            "id": "143ac7ab-b47a-4737-a6e1-e2d02a9047a5",
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

from typing import Final, List, Mapping, Optional


VALID_CHROMOSOME_SYMBOLS: Final[list[str]] = [str(i) for i in range(1, 23)] + [
    "X",
    "Y",
    "MT",
]
CHROMOSOME_MAPPING: Final[Mapping[str, str]] = (
    {v: v for v in VALID_CHROMOSOME_SYMBOLS}
    | {f"chr{i}": i for i in VALID_CHROMOSOME_SYMBOLS}
    | {"chrM": "MT"}
    | {f"Chr{i}": i for i in VALID_CHROMOSOME_SYMBOLS}
    | {"ChrM": "MT"}
)


a = {
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
                    "logic": "or",
                    "compound": [
                        {
                            "logic": "and",
                            "compound": [
                                {
                                    "filters": {
                                        "expression_read_optimized$CHROM": [
                                            {"condition": "is", "values": "1"}
                                        ]
                                    }
                                },
                                {
                                    "logic": "or",
                                    "compound": [
                                        {
                                            "filters": {
                                                "expression_read_optimized$start": [
                                                    {
                                                        "condition": "between",
                                                        "values": [10000, 20000],
                                                    }
                                                ],
                                                "expression_read_optimized$end": [
                                                    {
                                                        "condition": "between",
                                                        "values": [10000, 20000],
                                                    }
                                                ],
                                            },
                                            "logic": "or",
                                        },
                                        {
                                            "filters": {
                                                "expression_read_optimized$start": [
                                                    {
                                                        "condition": "less-than-eq",
                                                        "values": 10000,
                                                    }
                                                ],
                                                "expression_read_optimized$end": [
                                                    {
                                                        "condition": "greater-than-eq",
                                                        "values": 20000,
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


vizserver_payload_mult = {
    "project_context": "project-GyzGq4009Y2ZvJVy4qjBPbZB",
    "fields": [
        {"feature_id": "expression_read_optimized$feature_id"},
        {"sample_id": "expression_read_optimized$sample_id"},
        {"expression": "expression_read_optimized$value"},
        {"strand": "expression_read_optimized$strand"},
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
                        "expression_read_optimized$sample_id": [
                            {"condition": "in", "values": ["sample_1"]}
                        ]
                    }
                },
            ],
        }
    },
    "order_by": [{"feature_id": "asc"}, {"sample_id": "asc"}],
}


gene_name = {
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
