from ..utils.printing import fill


EXTRACT_ASSAY_EXPRESSION_JSON_TEMPLATE = """
{
  "location": [
    {
      "chromosome": "1",
      "starting_position": "10000",
      "ending_position": "20000"
    },
    {
      "chromosome": "X",
      "starting_position": "500",
      "ending_position": "1700"
    }
  ],
  "expression": {
    "min_value": 10.2,
    "max_value": 10000
  },
  "annotation": {
    "feature_name": ["BRCA2"],
    "feature_id": ["ENSG0000001", "ENSG00000002"]
  },
  "sample_id": ["Sample1", "Sample2"]
}
"""

EXTRACT_ASSAY_EXPRESSION_JSON_HELP = (
    fill(
        "# Additional descriptions of filtering keys and permissible values",
    )
    + EXTRACT_ASSAY_EXPRESSION_JSON_TEMPLATE
)
