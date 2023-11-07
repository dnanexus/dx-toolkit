# -*- coding: utf-8 -*-

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

EXTRACT_ASSAY_EXPRESSION_ADDITIONAL_FIELDS_HELP = """
The following fields will always be returned by default:

      NAME               TITLE                                                        DESCRIPTION
 sample_id           Sample ID                                 A unique identifier for the sample
feature_id          Feature ID                            An unique identification of the feature
     value    Expression Value    Expression value for the sample ID of the respective feature ID


The following fields may be added to the output by using option --additional-fields.
If multiple fields are specified, use a comma to separate each entry. For example, “chrom,feature_name”


        NAME             TITLE                                                             DESCRIPTION
feature_name      Feature Name                                                     Name of the feature
       chrom        Chromosome                                               Chromosome of the feature
       start    Start Position                                Genomic starting position of the feature
         end      End Position                                  Genomic ending position of the feature
      strand            Strand    Orientation of the feature with respect to forward or reverse strand 

"""