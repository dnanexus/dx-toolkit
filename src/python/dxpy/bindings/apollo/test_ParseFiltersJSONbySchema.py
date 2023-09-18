# Basic dev-stage tests. To-be-removed once unit tests are in place

import dxpy

from vizserver_filters_from_json_parser import JSONFiltersValidator
from assay_filtering_conditions import EXTRACT_ASSAY_EXPRESSION_FILTERING_CONDITIONS

input1 = {
    "expression": {
        "min_value": 0.5,
    },
    "sample_id": ['sample_1', "sample_2", "sample_3"],
}

input2 = {
    "expression": {
        "min_value": 0.5,
    },
}

input3 = {
    "location": [
        {
            "chromosome": "1",
            "starting_position": 10000,
            "ending_position": 20000
        }
    ],
}

input4 = {
    "location": [
        {
            "chromosome": "1",
            "starting_position": 10000,
            "ending_position": 20000
        },
        {
            "chromosome": "2",
            "starting_position": 30000,
            "ending_position": 40000
        }
    ],
    "sample_id": ['sample_1', "sample_2", "sample_3", "sample_4", "sample_5"],
    "expression": {
        "min_value": 0.5,
    }
}

input5 = {
    "location": [
        {
            "chromosome": "1",
            "starting_position": 10000,
            "ending_position": 20000
        },
        {
            "chromosome": "2",
            "starting_position": 30000,
            "ending_position": 40000
        }
    ],
}

schema = EXTRACT_ASSAY_EXPRESSION_FILTERING_CONDITIONS

for i in [input1, input2, input3, input4]:
    print(i)
    print("\n")
    c = JSONFiltersValidator(i, schema)
    print(c.parse())
    

c = JSONFiltersValidator(input4, schema)
filters = c.parse()

payload = {
    "project_context": "project-uuuuuu",
    "fields": [
        {"fid": "expr_annotation$feature_id"},
        {"f_id": "expression$feature_id"},
        {"chr": "expr_annotation$chr"},
        {"start": "expr_annotation$start"},
    ],
    "limit": 500,
    "raw_filters": {
        "assay_filters": {
            "name": "...",
            "id": "...",
            #**filters
        }
    },
}

payload["raw_filters"]["assay_filters"].update(filters)

# sql_query = dxpy.DXHTTPRequest(".../record-yyyy/raw-query", payload, prepend_srv=False)
