import json
import dxdata

dataset = dxdata.load_dataset(id="record-GFPZfk80f4Xk8gyQG8vj5p4G")

genotype_field_list = [
    x.column_name for x in dataset.assays[0].entities["genotype"].__dict__["fields"]
]
allele_field_list = [
    x.column_name for x in dataset.assays[0].entities["allele"].__dict__["fields"]
]
annotation_field_list = [
    x.column_name for x in dataset.assays[0].entities["annotation"].__dict__["fields"]
]

formatted_geno_fields = [
    {"{}${}".format("genotype", x): "{}${}".format("genotype", x)}
    for x in genotype_field_list
]
formatted_allele_fields = [
    {"{}${}".format("allele", x): "{}${}".format("allele", x)}
    for x in allele_field_list
]
formatted_annotation_fields = [
    {"{}${}".format("annotation", x): "{}${}".format("annotation", x)}
    for x in annotation_field_list
]

formatted_combined_fields = (
    formatted_geno_fields + formatted_allele_fields + formatted_annotation_fields
)

all_fields = json.dumps(formatted_combined_fields)
