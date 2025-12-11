VIZPAYLOADERBUILDER_EXPECTED_OUTPUT = {
    "test_vizpayloadbuilder_location_cohort": {
        "expected_data_output": [
            {
                "feature_id": "ENST00000456328",
                "sample_id": "sample_1",
                "expression": 23,
            },
            {
                "feature_id": "ENST00000456328",
                "sample_id": "sample_2",
                "expression": 90,
            },
            {
                "feature_id": "ENST00000456328",
                "sample_id": "sample_3",
                "expression": 58,
            },
        ],
        "expected_sql_output": [
            "SELECT `expression_1`.`feature_id` AS `feature_id`, `expression_1`.`sample_id` AS `sample_id`, `expression_1`.`value` AS `expression` FROM `database_gzky7400vgpyzy621q43gkkf__molecular_expression1_db`.`expression` AS `expression_1` LEFT OUTER JOIN `database_gzky7400vgpyzy621q43gkkf__molecular_expression1_db`.`expr_annotation` AS `expr_annotation_1` ON `expression_1`.`feature_id` = `expr_annotation_1`.`feature_id` LEFT OUTER JOIN `database_gzky7400vgpyzy621q43gkkf__molecular_expression1_db`.`sample` AS `sample_1` ON `expression_1`.`sample_id` = `sample_1`.`sample_id` WHERE `expr_annotation_1`.`chr` = '1' AND ((`expr_annotation_1`.`start` BETWEEN 10000 AND 12000 OR `expr_annotation_1`.`end` BETWEEN 10000 AND 12000) OR (`expr_annotation_1`.`start` <= 10000 AND `expr_annotation_1`.`end` >= 12000)) AND `sample_1`.`sample_id` IN (SELECT `cohort_query`.`sample_id` FROM (SELECT `sample_1`.`sample_id` AS `sample_id` FROM `database_gzky7400vgpyzy621q43gkkf__molecular_expression1_db`.`sample` AS `sample_1` WHERE `sample_1`.`sample_id` IN (SELECT `sample_id` FROM (SELECT `sample_1`.`sample_id` AS `sample_id` FROM `database_gzky7400vgpyzy621q43gkkf__molecular_expression1_db`.`sample` AS `sample_1` UNION SELECT `sample_1`.`sample_id` AS `sample_id` FROM `database_gzky7400vgpyzy621q43gkkf__molecular_expression1_db`.`sample` AS `sample_1`))) AS `cohort_query`) ORDER BY `feature_id` ASC, `sample_id` ASC",
            "SELECT `expression_1`.`feature_id` AS `feature_id`, `expression_1`.`sample_id` AS `sample_id`, `expression_1`.`value` AS `expression` FROM `database_gzky7400vgpyzy621q43gkkf__molecular_expression1_db`.`expression` AS `expression_1` LEFT OUTER JOIN `database_gzky7400vgpyzy621q43gkkf__molecular_expression1_db`.`expr_annotation` AS `expr_annotation_1` ON `expression_1`.`feature_id` = `expr_annotation_1`.`feature_id` LEFT OUTER JOIN `database_gzky7400vgpyzy621q43gkkf__molecular_expression1_db`.`sample` AS `sample_1` ON `expression_1`.`sample_id` = `sample_1`.`sample_id` WHERE `expr_annotation_1`.`chr` = '1' AND ((`expr_annotation_1`.`end` BETWEEN 10000 AND 12000 OR `expr_annotation_1`.`start` BETWEEN 10000 AND 12000) OR (`expr_annotation_1`.`end` >= 12000 AND `expr_annotation_1`.`start` <= 10000)) AND `sample_1`.`sample_id` IN (SELECT `cohort_query`.`sample_id` FROM (SELECT `sample_1`.`sample_id` AS `sample_id` FROM `database_gzky7400vgpyzy621q43gkkf__molecular_expression1_db`.`sample` AS `sample_1` WHERE `sample_1`.`sample_id` IN (SELECT `sample_id` FROM (SELECT `sample_1`.`sample_id` AS `sample_id` FROM `database_gzky7400vgpyzy621q43gkkf__molecular_expression1_db`.`sample` AS `sample_1` UNION SELECT `sample_1`.`sample_id` AS `sample_id` FROM `database_gzky7400vgpyzy621q43gkkf__molecular_expression1_db`.`sample` AS `sample_1`))) AS `cohort_query`) ORDER BY `feature_id` ASC, `sample_id` ASC",
           ],
        "expected_sql_output_1_1": [
            "SELECT `expression_1`.`feature_id` AS `feature_id`, `expression_1`.`sample_id` AS `sample_id`, `expression_1`.`value` AS `expression` FROM `database_j29xxf80vgpgzv8x60bvfzzv__molecular_expression_1_1`.`expression` AS `expression_1` LEFT OUTER JOIN `database_j29xxf80vgpgzv8x60bvfzzv__molecular_expression_1_1`.`expr_annotation` AS `expr_annotation_1` ON `expression_1`.`feature_id` = `expr_annotation_1`.`feature_id` WHERE `expr_annotation_1`.`CHROM` = '1' AND ((`expr_annotation_1`.`end` BETWEEN 10000 AND 12000 OR `expr_annotation_1`.`start` BETWEEN 10000 AND 12000) OR (`expr_annotation_1`.`end` >= 12000 AND `expr_annotation_1`.`start` <= 10000)) ORDER BY `feature_id` ASC, `sample_id` ASC",
            "SELECT `expression_1`.`feature_id` AS `feature_id`, `expression_1`.`sample_id` AS `sample_id`, `expression_1`.`value` AS `expression` FROM `database_j29xxf80vgpgzv8x60bvfzzv__molecular_expression_1_1`.`expression` AS `expression_1` LEFT OUTER JOIN `database_j29xxf80vgpgzv8x60bvfzzv__molecular_expression_1_1`.`expr_annotation` AS `expr_annotation_1` ON `expression_1`.`feature_id` = `expr_annotation_1`.`feature_id` WHERE `expr_annotation_1`.`CHROM` = '1' AND ((`expr_annotation_1`.`start` BETWEEN 10000 AND 12000 OR `expr_annotation_1`.`end` BETWEEN 10000 AND 12000) OR (`expr_annotation_1`.`start` <= 10000 AND `expr_annotation_1`.`end` >= 12000)) ORDER BY `feature_id` ASC, `sample_id` ASC"
        ],
    },
    "test_vizpayloadbuilder_location_multiple": {
        "expected_data_output": [
            {
                "feature_id": "ENST00000327669",
                "sample_id": "sample_1",
                "expression": 11,
            },
            {
                "feature_id": "ENST00000327669",
                "sample_id": "sample_2",
                "expression": 78,
            },
            {
                "feature_id": "ENST00000327669",
                "sample_id": "sample_3",
                "expression": 23,
            },
            {
                "feature_id": "ENST00000456328",
                "sample_id": "sample_3",
                "expression": 58,
            },
            {
                "feature_id": "ENST00000456328",
                "sample_id": "sample_2",
                "expression": 90,
            },
            {
                "feature_id": "ENST00000456328",
                "sample_id": "sample_1",
                "expression": 23,
            },
        ],
        "expected_sql_output": [
            "SELECT `expression_1`.`feature_id` AS `feature_id`, `expression_1`.`sample_id` AS `sample_id`, `expression_1`.`value` AS `expression` FROM `database_gzky7400vgpyzy621q43gkkf__molecular_expression1_db`.`expression` AS `expression_1` LEFT OUTER JOIN `database_gzky7400vgpyzy621q43gkkf__molecular_expression1_db`.`expr_annotation` AS `expr_annotation_1` ON `expression_1`.`feature_id` = `expr_annotation_1`.`feature_id` WHERE (`expr_annotation_1`.`chr` = '1' AND ((`expr_annotation_1`.`start` BETWEEN 10000 AND 12000 OR `expr_annotation_1`.`end` BETWEEN 10000 AND 12000) OR (`expr_annotation_1`.`start` <= 10000 AND `expr_annotation_1`.`end` >= 12000))) OR (`expr_annotation_1`.`chr` = '2' AND ((`expr_annotation_1`.`start` BETWEEN 30000 AND 40000 OR `expr_annotation_1`.`end` BETWEEN 30000 AND 40000) OR (`expr_annotation_1`.`start` <= 30000 AND `expr_annotation_1`.`end` >= 40000))) ORDER BY `feature_id` ASC, `sample_id` ASC",
            "SELECT `expression_1`.`feature_id` AS `feature_id`, `expression_1`.`sample_id` AS `sample_id`, `expression_1`.`value` AS `expression` FROM `database_gzky7400vgpyzy621q43gkkf__molecular_expression1_db`.`expression` AS `expression_1` LEFT OUTER JOIN `database_gzky7400vgpyzy621q43gkkf__molecular_expression1_db`.`expr_annotation` AS `expr_annotation_1` ON `expression_1`.`feature_id` = `expr_annotation_1`.`feature_id` WHERE (`expr_annotation_1`.`chr` = '1' AND ((`expr_annotation_1`.`end` BETWEEN 10000 AND 12000 OR `expr_annotation_1`.`start` BETWEEN 10000 AND 12000) OR (`expr_annotation_1`.`end` >= 12000 AND `expr_annotation_1`.`start` <= 10000))) OR (`expr_annotation_1`.`chr` = '2' AND ((`expr_annotation_1`.`end` BETWEEN 30000 AND 40000 OR `expr_annotation_1`.`start` BETWEEN 30000 AND 40000) OR (`expr_annotation_1`.`end` >= 40000 AND `expr_annotation_1`.`start` <= 30000))) ORDER BY `feature_id` ASC, `sample_id` ASC",
        ],
    },
    "test_vizpayloadbuilder_annotation_feature_name": {
        "expected_data_output": [
            {
                "feature_id": "ENST00000318560",
                "sample_id": "sample_3",
                "expression": 56,
            },
            {
                "feature_id": "ENST00000318560",
                "sample_id": "sample_2",
                "expression": 24,
            },
            {
                "feature_id": "ENST00000318560",
                "sample_id": "sample_1",
                "expression": 39,
            },
            {
                "feature_id": "ENST00000372348",
                "sample_id": "sample_3",
                "expression": 90,
            },
            {
                "feature_id": "ENST00000372348",
                "sample_id": "sample_2",
                "expression": 40,
            },
            {
                "feature_id": "ENST00000372348",
                "sample_id": "sample_1",
                "expression": 87,
            },
            {
                "feature_id": "ENST00000393293",
                "sample_id": "sample_2",
                "expression": 11,
            },
            {
                "feature_id": "ENST00000393293",
                "sample_id": "sample_3",
                "expression": 40,
            },
            {
                "feature_id": "ENST00000393293",
                "sample_id": "sample_1",
                "expression": 17,
            },
        ],
        "expected_sql_output": "SELECT `expression_1`.`feature_id` AS `feature_id`, `expression_1`.`sample_id` AS `sample_id`, `expression_1`.`value` AS `expression` FROM `database_gzky7400vgpyzy621q43gkkf__molecular_expression1_db`.`expression` AS `expression_1` LEFT OUTER JOIN `database_gzky7400vgpyzy621q43gkkf__molecular_expression1_db`.`expr_annotation` AS `expr_annotation_1` ON `expression_1`.`feature_id` = `expr_annotation_1`.`feature_id` WHERE `expr_annotation_1`.`gene_name` IN ('ABL1') ORDER BY `feature_id` ASC, `sample_id` ASC",
        "expected_sql_output_1_1": "SELECT `expression_read_optimized_1`.`feature_id` AS `feature_id`, `expression_read_optimized_1`.`sample_id` AS `sample_id`, `expression_read_optimized_1`.`value` AS `expression` FROM `database_j29xxf80vgpgzv8x60bvfzzv__molecular_expression_1_1`.`expression_read_optimized` AS `expression_read_optimized_1` WHERE `expression_read_optimized_1`.`gene_name` IN ('ABL1') AND `expression_read_optimized_1`.`chr` = '9' AND `expression_read_optimized_1`.`bin` IN (52) ORDER BY `feature_id` ASC, `sample_id` ASC",
    },
    "test_vizpayloadbuilder_annotation_feature_id": {
        "expected_data_output": [
            {
                "feature_id": "ENST00000327669",
                "sample_id": "sample_1",
                "expression": 11,
            },
            {
                "feature_id": "ENST00000327669",
                "sample_id": "sample_2",
                "expression": 78,
            },
            {
                "feature_id": "ENST00000327669",
                "sample_id": "sample_3",
                "expression": 23,
            },
            {
                "feature_id": "ENST00000456328",
                "sample_id": "sample_3",
                "expression": 58,
            },
            {
                "feature_id": "ENST00000456328",
                "sample_id": "sample_2",
                "expression": 90,
            },
            {
                "feature_id": "ENST00000456328",
                "sample_id": "sample_1",
                "expression": 23,
            },
        ],
        "expected_sql_output": "SELECT `expression_1`.`feature_id` AS `feature_id`, `expression_1`.`sample_id` AS `sample_id`, `expression_1`.`value` AS `expression` FROM `database_gzky7400vgpyzy621q43gkkf__molecular_expression1_db`.`expression` AS `expression_1` LEFT OUTER JOIN `database_gzky7400vgpyzy621q43gkkf__molecular_expression1_db`.`expr_annotation` AS `expr_annotation_1` ON `expression_1`.`feature_id` = `expr_annotation_1`.`feature_id` WHERE `expr_annotation_1`.`feature_id` IN ('ENST00000327669', 'ENST00000456328') ORDER BY `feature_id` ASC, `sample_id` ASC",
    },
    "test_vizpayloadbuilder_expression_min": {
        "expected_data_output": [
            {
                "feature_id": "ENST00000327669",
                "sample_id": "sample_2",
                "expression": 78,
            },
            {
                "feature_id": "ENST00000456328",
                "sample_id": "sample_2",
                "expression": 90,
            },
        ],
        "expected_sql_output": [
            "SELECT `expression_1`.`feature_id` AS `feature_id`, `expression_1`.`sample_id` AS `sample_id`, `expression_1`.`value` AS `expression` FROM `database_gzky7400vgpyzy621q43gkkf__molecular_expression1_db`.`expression` AS `expression_1` LEFT OUTER JOIN `database_gzky7400vgpyzy621q43gkkf__molecular_expression1_db`.`expr_annotation` AS `expr_annotation_1` ON `expression_1`.`feature_id` = `expr_annotation_1`.`feature_id` WHERE `expression_1`.`value` >= 70 AND `expr_annotation_1`.`feature_id` IN ('ENST00000327669', 'ENST00000456328') ORDER BY `feature_id` ASC, `sample_id` ASC",
            "SELECT `expression_1`.`feature_id` AS `feature_id`, `expression_1`.`sample_id` AS `sample_id`, `expression_1`.`value` AS `expression` FROM `database_gzky7400vgpyzy621q43gkkf__molecular_expression1_db`.`expression` AS `expression_1` LEFT OUTER JOIN `database_gzky7400vgpyzy621q43gkkf__molecular_expression1_db`.`expr_annotation` AS `expr_annotation_1` ON `expression_1`.`feature_id` = `expr_annotation_1`.`feature_id` WHERE `expr_annotation_1`.`feature_id` IN ('ENST00000327669', 'ENST00000456328') AND `expression_1`.`value` >= 70 ORDER BY `feature_id` ASC, `sample_id` ASC",
        ],
    },
    "test_vizpayloadbuilder_expression_max": {
        "expected_data_output": [
            {"feature_id": "ENST00000666593", "sample_id": "sample_2", "expression": 3}
        ],
        "expected_sql_output": [
            "SELECT `expression_1`.`feature_id` AS `feature_id`, `expression_1`.`sample_id` AS `sample_id`, `expression_1`.`value` AS `expression` FROM `database_gzky7400vgpyzy621q43gkkf__molecular_expression1_db`.`expression` AS `expression_1` LEFT OUTER JOIN `database_gzky7400vgpyzy621q43gkkf__molecular_expression1_db`.`expr_annotation` AS `expr_annotation_1` ON `expression_1`.`feature_id` = `expr_annotation_1`.`feature_id` WHERE `expression_1`.`value` <= 10 AND `expr_annotation_1`.`gene_name` IN ('BRCA2') ORDER BY `feature_id` ASC, `sample_id` ASC",
            "SELECT `expression_1`.`feature_id` AS `feature_id`, `expression_1`.`sample_id` AS `sample_id`, `expression_1`.`value` AS `expression` FROM `database_gzky7400vgpyzy621q43gkkf__molecular_expression1_db`.`expression` AS `expression_1` LEFT OUTER JOIN `database_gzky7400vgpyzy621q43gkkf__molecular_expression1_db`.`expr_annotation` AS `expr_annotation_1` ON `expression_1`.`feature_id` = `expr_annotation_1`.`feature_id` WHERE `expr_annotation_1`.`gene_name` IN ('BRCA2') AND `expression_1`.`value` <= 10 ORDER BY `feature_id` ASC, `sample_id` ASC",
        ],
    },
    "test_vizpayloadbuilder_expression_mixed": {
        "expected_data_output": [
            {"feature_id": "ENST00000456328", "sample_id": "sample_3", "expression": 58}
        ],
        "expected_sql_output": [
            "SELECT `expression_1`.`feature_id` AS `feature_id`, `expression_1`.`sample_id` AS `sample_id`, `expression_1`.`value` AS `expression` FROM `database_gzky7400vgpyzy621q43gkkf__molecular_expression1_db`.`expression` AS `expression_1` LEFT OUTER JOIN `database_gzky7400vgpyzy621q43gkkf__molecular_expression1_db`.`expr_annotation` AS `expr_annotation_1` ON `expression_1`.`feature_id` = `expr_annotation_1`.`feature_id` WHERE `expression_1`.`value` BETWEEN 30 AND 60 AND `expr_annotation_1`.`chr` = '1' AND ((`expr_annotation_1`.`start` BETWEEN 10000 AND 12000 OR `expr_annotation_1`.`end` BETWEEN 10000 AND 12000) OR (`expr_annotation_1`.`start` <= 10000 AND `expr_annotation_1`.`end` >= 12000)) ORDER BY `feature_id` ASC, `sample_id` ASC",
            "SELECT `expression_1`.`feature_id` AS `feature_id`, `expression_1`.`sample_id` AS `sample_id`, `expression_1`.`value` AS `expression` FROM `database_gzky7400vgpyzy621q43gkkf__molecular_expression1_db`.`expression` AS `expression_1` LEFT OUTER JOIN `database_gzky7400vgpyzy621q43gkkf__molecular_expression1_db`.`expr_annotation` AS `expr_annotation_1` ON `expression_1`.`feature_id` = `expr_annotation_1`.`feature_id` WHERE `expression_1`.`value` BETWEEN 30 AND 60 AND `expr_annotation_1`.`chr` = '1' AND ((`expr_annotation_1`.`end` BETWEEN 10000 AND 12000 OR `expr_annotation_1`.`start` BETWEEN 10000 AND 12000) OR (`expr_annotation_1`.`end` >= 12000 AND `expr_annotation_1`.`start` <= 10000)) ORDER BY `feature_id` ASC, `sample_id` ASC",
        ],
    },
    "test_vizpayloadbuilder_sample": {
        "expected_sql_output": "SELECT `expression_1`.`feature_id` AS `feature_id`, `expression_1`.`sample_id` AS `sample_id`, `expression_1`.`value` AS `expression` FROM `database_gzky7400vgpyzy621q43gkkf__molecular_expression1_db`.`expression` AS `expression_1` WHERE `expression_1`.`sample_id` IN ('sample_1') ORDER BY `feature_id` ASC, `sample_id` ASC"
    },
    "test_vizpayloadbuilder_location_sample_expression": {
        "expected_data_output": [
            {
                "feature_id": "ENST00000450305",
                "sample_id": "sample_1",
                "expression": 76,
            },
            {
                "feature_id": "ENST00000619216",
                "sample_id": "sample_1",
                "expression": 59,
            },
        ],
        "expected_sql_output": [
            "SELECT `expression_1`.`feature_id` AS `feature_id`, `expression_1`.`sample_id` AS `sample_id`, `expression_1`.`value` AS `expression` FROM `database_gzky7400vgpyzy621q43gkkf__molecular_expression1_db`.`expression` AS `expression_1` LEFT OUTER JOIN `database_gzky7400vgpyzy621q43gkkf__molecular_expression1_db`.`expr_annotation` AS `expr_annotation_1` ON `expression_1`.`feature_id` = `expr_annotation_1`.`feature_id` WHERE `expr_annotation_1`.`chr` = '1' AND ((`expr_annotation_1`.`end` BETWEEN 10000 AND 20000 OR `expr_annotation_1`.`start` BETWEEN 10000 AND 20000) OR (`expr_annotation_1`.`end` >= 20000 AND `expr_annotation_1`.`start` <= 10000)) AND `expression_1`.`value` BETWEEN 25 AND 80 AND `expression_1`.`sample_id` IN ('sample_1') ORDER BY `feature_id` ASC, `sample_id` ASC",
            "SELECT `expression_1`.`feature_id` AS `feature_id`, `expression_1`.`sample_id` AS `sample_id`, `expression_1`.`value` AS `expression` FROM `database_gzky7400vgpyzy621q43gkkf__molecular_expression1_db`.`expression` AS `expression_1` LEFT OUTER JOIN `database_gzky7400vgpyzy621q43gkkf__molecular_expression1_db`.`expr_annotation` AS `expr_annotation_1` ON `expression_1`.`feature_id` = `expr_annotation_1`.`feature_id` WHERE `expr_annotation_1`.`chr` = '1' AND ((`expr_annotation_1`.`end` BETWEEN 10000 AND 20000 OR `expr_annotation_1`.`start` BETWEEN 10000 AND 20000) OR (`expr_annotation_1`.`end` >= 20000 AND `expr_annotation_1`.`start` <= 10000)) AND `expression_1`.`sample_id` IN ('sample_1') AND `expression_1`.`value` BETWEEN 25 AND 80 ORDER BY `feature_id` ASC, `sample_id` ASC",
            "SELECT `expression_1`.`feature_id` AS `feature_id`, `expression_1`.`sample_id` AS `sample_id`, `expression_1`.`value` AS `expression` FROM `database_gzky7400vgpyzy621q43gkkf__molecular_expression1_db`.`expression` AS `expression_1` LEFT OUTER JOIN `database_gzky7400vgpyzy621q43gkkf__molecular_expression1_db`.`expr_annotation` AS `expr_annotation_1` ON `expression_1`.`feature_id` = `expr_annotation_1`.`feature_id` WHERE `expr_annotation_1`.`chr` = '1' AND ((`expr_annotation_1`.`start` BETWEEN 10000 AND 20000 OR `expr_annotation_1`.`end` BETWEEN 10000 AND 20000) OR (`expr_annotation_1`.`start` <= 10000 AND `expr_annotation_1`.`end` >= 20000)) AND `expression_1`.`value` BETWEEN 25 AND 80 AND `expression_1`.`sample_id` IN ('sample_1') ORDER BY `feature_id` ASC, `sample_id` ASC",
        ],
    },
    "test_vizpayloadbuilder_sample_annotation_expression_sample_id": {
        "expected_data_output": [
            {
                "feature_id": "ENST00000450305",
                "sample_id": "sample_2",
                "expression": 50,
            },
            {
                "feature_id": "ENST00000456328",
                "sample_id": "sample_1",
                "expression": 23,
            },
            {
                "feature_id": "ENST00000456328",
                "sample_id": "sample_3",
                "expression": 58,
            },
        ],
        "expected_sql_output": [
            "SELECT `expression_1`.`feature_id` AS `feature_id`, `expression_1`.`sample_id` AS `sample_id`, `expression_1`.`value` AS `expression` FROM `database_gzky7400vgpyzy621q43gkkf__molecular_expression1_db`.`expression` AS `expression_1` LEFT OUTER JOIN `database_gzky7400vgpyzy621q43gkkf__molecular_expression1_db`.`expr_annotation` AS `expr_annotation_1` ON `expression_1`.`feature_id` = `expr_annotation_1`.`feature_id` WHERE `expression_1`.`value` BETWEEN 20 AND 70 AND `expr_annotation_1`.`feature_id` IN ('ENST00000450305', 'ENST00000456328') AND `expression_1`.`sample_id` IN ('sample_1', 'sample_2', 'sample_3', 'sample_4') ORDER BY `feature_id` ASC, `sample_id` ASC",
            "SELECT `expression_1`.`feature_id` AS `feature_id`, `expression_1`.`sample_id` AS `sample_id`, `expression_1`.`value` AS `expression` FROM `database_gzky7400vgpyzy621q43gkkf__molecular_expression1_db`.`expression` AS `expression_1` LEFT OUTER JOIN `database_gzky7400vgpyzy621q43gkkf__molecular_expression1_db`.`expr_annotation` AS `expr_annotation_1` ON `expression_1`.`feature_id` = `expr_annotation_1`.`feature_id` WHERE `expression_1`.`sample_id` IN ('sample_1', 'sample_2', 'sample_3', 'sample_4') AND `expression_1`.`value` BETWEEN 20 AND 70 AND `expr_annotation_1`.`feature_id` IN ('ENST00000450305', 'ENST00000456328') ORDER BY `feature_id` ASC, `sample_id` ASC",
            "SELECT `expression_1`.`feature_id` AS `feature_id`, `expression_1`.`sample_id` AS `sample_id`, `expression_1`.`value` AS `expression` FROM `database_gzky7400vgpyzy621q43gkkf__molecular_expression1_db`.`expression` AS `expression_1` LEFT OUTER JOIN `database_gzky7400vgpyzy621q43gkkf__molecular_expression1_db`.`expr_annotation` AS `expr_annotation_1` ON `expression_1`.`feature_id` = `expr_annotation_1`.`feature_id` WHERE `expr_annotation_1`.`feature_id` IN ('ENST00000450305', 'ENST00000456328') AND `expression_1`.`value` BETWEEN 20 AND 70 AND `expression_1`.`sample_id` IN ('sample_1', 'sample_2', 'sample_3', 'sample_4') ORDER BY `feature_id` ASC, `sample_id` ASC",
            "SELECT `expression_1`.`feature_id` AS `feature_id`, `expression_1`.`sample_id` AS `sample_id`, `expression_1`.`value` AS `expression` FROM `database_gzky7400vgpyzy621q43gkkf__molecular_expression1_db`.`expression` AS `expression_1` LEFT OUTER JOIN `database_gzky7400vgpyzy621q43gkkf__molecular_expression1_db`.`expr_annotation` AS `expr_annotation_1` ON `expression_1`.`feature_id` = `expr_annotation_1`.`feature_id` WHERE `expression_1`.`sample_id` IN ('sample_1', 'sample_2', 'sample_3', 'sample_4') AND `expr_annotation_1`.`feature_id` IN ('ENST00000450305', 'ENST00000456328') AND `expression_1`.`value` BETWEEN 20 AND 70 ORDER BY `feature_id` ASC, `sample_id` ASC",
            "SELECT `expression_1`.`feature_id` AS `feature_id`, `expression_1`.`sample_id` AS `sample_id`, `expression_1`.`value` AS `expression` FROM `database_gzky7400vgpyzy621q43gkkf__molecular_expression1_db`.`expression` AS `expression_1` LEFT OUTER JOIN `database_gzky7400vgpyzy621q43gkkf__molecular_expression1_db`.`expr_annotation` AS `expr_annotation_1` ON `expression_1`.`feature_id` = `expr_annotation_1`.`feature_id` WHERE `expr_annotation_1`.`feature_id` IN ('ENST00000450305', 'ENST00000456328') AND `expression_1`.`sample_id` IN ('sample_1', 'sample_2', 'sample_3', 'sample_4') AND `expression_1`.`value` BETWEEN 20 AND 70 ORDER BY `feature_id` ASC, `sample_id` ASC",
            "SELECT `expression_1`.`feature_id` AS `feature_id`, `expression_1`.`sample_id` AS `sample_id`, `expression_1`.`value` AS `expression` FROM `database_gzky7400vgpyzy621q43gkkf__molecular_expression1_db`.`expression` AS `expression_1` LEFT OUTER JOIN `database_gzky7400vgpyzy621q43gkkf__molecular_expression1_db`.`expr_annotation` AS `expr_annotation_1` ON `expression_1`.`feature_id` = `expr_annotation_1`.`feature_id` WHERE `expression_1`.`sample_id` IN ('sample_1', 'sample_2', 'sample_3', 'sample_4') AND `expr_annotation_1`.`feature_id` IN ('ENST00000450305', 'ENST00000456328') AND `expression_1`.`value` BETWEEN 20 AND 70 ORDER BY `feature_id` ASC, `sample_id` ASC",
            "SELECT `expression_1`.`feature_id` AS `feature_id`, `expression_1`.`sample_id` AS `sample_id`, `expression_1`.`value` AS `expression` FROM `database_gzky7400vgpyzy621q43gkkf__molecular_expression1_db`.`expression` AS `expression_1` LEFT OUTER JOIN `database_gzky7400vgpyzy621q43gkkf__molecular_expression1_db`.`expr_annotation` AS `expr_annotation_1` ON `expression_1`.`feature_id` = `expr_annotation_1`.`feature_id` WHERE `expression_1`.`value` BETWEEN 20 AND 70 AND `expression_1`.`sample_id` IN ('sample_1', 'sample_2', 'sample_3', 'sample_4') AND `expr_annotation_1`.`feature_id` IN ('ENST00000450305', 'ENST00000456328') ORDER BY `feature_id` ASC, `sample_id` ASC",
        ],
        "expected_sql_output_1_1": [
            "SELECT `expression_read_optimized_1`.`feature_id` AS `feature_id`, `expression_read_optimized_1`.`sample_id` AS `sample_id`, `expression_read_optimized_1`.`value` AS `expression` FROM `database_j29xxf80vgpgzv8x60bvfzzv__molecular_expression_1_1`.`expression_read_optimized` AS `expression_read_optimized_1` WHERE (`expression_read_optimized_1`.`feature_id` IN ('ENST00000450305') AND `expression_read_optimized_1`.`chr` = '1' AND `expression_read_optimized_1`.`bin` IN (0) AND `expression_read_optimized_1`.`value` BETWEEN 20 AND 70 AND `expression_read_optimized_1`.`sample_id` IN ('sample_1', 'sample_2', 'sample_3', 'sample_4')) OR (`expression_read_optimized_1`.`feature_id` IN ('ENST00000456328') AND `expression_read_optimized_1`.`chr` = '1' AND `expression_read_optimized_1`.`bin` IN (0) AND `expression_read_optimized_1`.`value` BETWEEN 20 AND 70 AND `expression_read_optimized_1`.`sample_id` IN ('sample_1', 'sample_2', 'sample_3', 'sample_4')) ORDER BY `feature_id` ASC, `sample_id` ASC"
        ],
    },
    "test_vizpayloadbuilder_annotation_sample_expression": {
        "expected_data_output": [
            {"feature_id": "ENST00000327669", "sample_id": "sample_1", "expression": 11}
        ],
        "expected_sql_output": [
            "SELECT `expression_1`.`feature_id` AS `feature_id`, `expression_1`.`sample_id` AS `sample_id`, `expression_1`.`value` AS `expression` FROM `database_gzky7400vgpyzy621q43gkkf__molecular_expression1_db`.`expression` AS `expression_1` LEFT OUTER JOIN `database_gzky7400vgpyzy621q43gkkf__molecular_expression1_db`.`expr_annotation` AS `expr_annotation_1` ON `expression_1`.`feature_id` = `expr_annotation_1`.`feature_id` WHERE `expression_1`.`value` <= 20 AND `expr_annotation_1`.`feature_id` IN ('ENST00000327669', 'ENST00000456328') AND `expression_1`.`sample_id` IN ('sample_1') ORDER BY `feature_id` ASC, `sample_id` ASC",
            "SELECT `expression_1`.`feature_id` AS `feature_id`, `expression_1`.`sample_id` AS `sample_id`, `expression_1`.`value` AS `expression` FROM `database_gzky7400vgpyzy621q43gkkf__molecular_expression1_db`.`expression` AS `expression_1` LEFT OUTER JOIN `database_gzky7400vgpyzy621q43gkkf__molecular_expression1_db`.`expr_annotation` AS `expr_annotation_1` ON `expression_1`.`feature_id` = `expr_annotation_1`.`feature_id` WHERE `expression_1`.`sample_id` IN ('sample_1') AND `expression_1`.`value` <= 20 AND `expr_annotation_1`.`feature_id` IN ('ENST00000327669', 'ENST00000456328') ORDER BY `feature_id` ASC, `sample_id` ASC",
            "SELECT `expression_1`.`feature_id` AS `feature_id`, `expression_1`.`sample_id` AS `sample_id`, `expression_1`.`value` AS `expression` FROM `database_gzky7400vgpyzy621q43gkkf__molecular_expression1_db`.`expression` AS `expression_1` LEFT OUTER JOIN `database_gzky7400vgpyzy621q43gkkf__molecular_expression1_db`.`expr_annotation` AS `expr_annotation_1` ON `expression_1`.`feature_id` = `expr_annotation_1`.`feature_id` WHERE `expr_annotation_1`.`feature_id` IN ('ENST00000327669', 'ENST00000456328') AND `expression_1`.`value` <= 20 AND `expression_1`.`sample_id` IN ('sample_1') ORDER BY `feature_id` ASC, `sample_id` ASC",
            "SELECT `expression_1`.`feature_id` AS `feature_id`, `expression_1`.`sample_id` AS `sample_id`, `expression_1`.`value` AS `expression` FROM `database_gzky7400vgpyzy621q43gkkf__molecular_expression1_db`.`expression` AS `expression_1` LEFT OUTER JOIN `database_gzky7400vgpyzy621q43gkkf__molecular_expression1_db`.`expr_annotation` AS `expr_annotation_1` ON `expression_1`.`feature_id` = `expr_annotation_1`.`feature_id` WHERE `expression_1`.`sample_id` IN ('sample_1') AND `expr_annotation_1`.`feature_id` IN ('ENST00000327669', 'ENST00000456328') AND `expression_1`.`value` <= 20 ORDER BY `feature_id` ASC, `sample_id` ASC",
            "SELECT `expression_1`.`feature_id` AS `feature_id`, `expression_1`.`sample_id` AS `sample_id`, `expression_1`.`value` AS `expression` FROM `database_gzky7400vgpyzy621q43gkkf__molecular_expression1_db`.`expression` AS `expression_1` LEFT OUTER JOIN `database_gzky7400vgpyzy621q43gkkf__molecular_expression1_db`.`expr_annotation` AS `expr_annotation_1` ON `expression_1`.`feature_id` = `expr_annotation_1`.`feature_id` WHERE `expr_annotation_1`.`feature_id` IN ('ENST00000327669', 'ENST00000456328') AND `expression_1`.`sample_id` IN ('sample_1') AND `expression_1`.`value` <= 20 ORDER BY `feature_id` ASC, `sample_id` ASC",
            "SELECT `expression_1`.`feature_id` AS `feature_id`, `expression_1`.`sample_id` AS `sample_id`, `expression_1`.`value` AS `expression` FROM `database_gzky7400vgpyzy621q43gkkf__molecular_expression1_db`.`expression` AS `expression_1` LEFT OUTER JOIN `database_gzky7400vgpyzy621q43gkkf__molecular_expression1_db`.`expr_annotation` AS `expr_annotation_1` ON `expression_1`.`feature_id` = `expr_annotation_1`.`feature_id` WHERE `expression_1`.`sample_id` IN ('sample_1') AND `expr_annotation_1`.`feature_id` IN ('ENST00000327669', 'ENST00000456328') AND `expression_1`.`value` <= 20 ORDER BY `feature_id` ASC, `sample_id` ASC",
            "SELECT `expression_1`.`feature_id` AS `feature_id`, `expression_1`.`sample_id` AS `sample_id`, `expression_1`.`value` AS `expression` FROM `database_gzky7400vgpyzy621q43gkkf__molecular_expression1_db`.`expression` AS `expression_1` LEFT OUTER JOIN `database_gzky7400vgpyzy621q43gkkf__molecular_expression1_db`.`expr_annotation` AS `expr_annotation_1` ON `expression_1`.`feature_id` = `expr_annotation_1`.`feature_id` WHERE `expression_1`.`value` <= 20 AND `expression_1`.`sample_id` IN ('sample_1') AND `expr_annotation_1`.`feature_id` IN ('ENST00000327669', 'ENST00000456328') ORDER BY `feature_id` ASC, `sample_id` ASC",
            "SELECT `expression_1`.`feature_id` AS `feature_id`, `expression_1`.`sample_id` AS `sample_id`, `expression_1`.`value` AS `expression` FROM `database_gzky7400vgpyzy621q43gkkf__molecular_expression1_db`.`expression` AS `expression_1` LEFT OUTER JOIN `database_gzky7400vgpyzy621q43gkkf__molecular_expression1_db`.`expr_annotation` AS `expr_annotation_1` ON `expression_1`.`feature_id` = `expr_annotation_1`.`feature_id` WHERE `expression_1`.`value` <= 20 AND `expr_annotation_1`.`feature_id` IN ('ENST00000327669', 'ENST00000456328') AND `expression_1`.`sample_id` IN ('sample_1') ORDER BY `feature_id` ASC, `sample_id` ASC",
            "SELECT `expression_1`.`feature_id` AS `feature_id`, `expression_1`.`sample_id` AS `sample_id`, `expression_1`.`value` AS `expression` FROM `database_gzky7400vgpyzy621q43gkkf__molecular_expression1_db`.`expression` AS `expression_1` LEFT OUTER JOIN `database_gzky7400vgpyzy621q43gkkf__molecular_expression1_db`.`expr_annotation` AS `expr_annotation_1` ON `expression_1`.`feature_id` = `expr_annotation_1`.`feature_id` WHERE `expression_1`.`sample_id` IN ('sample_1') AND `expression_1`.`value` <= 20 AND `expr_annotation_1`.`feature_id` IN ('ENST00000327669', 'ENST00000456328') ORDER BY `feature_id` ASC, `sample_id` ASC",
        ],
    },
}
