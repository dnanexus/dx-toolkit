def generate_pheno_filter(values, entity, field, cohort_filter):
    values_filter = {
        "name": "phenotype",
        "logic": "and",
        "filters": {
            "$".join([entity, field]): [
                {
                    "condition": "in",
                    "values": values,
                }
            ]
        },
        "entity": {
            "logic": "and",
            "name": entity,
            "operator": "exists",
            "children": []
        }
    }
    if "pheno_filters" in cohort_filter:
        raise NotImplementedError("Pre-existing phenotype filters are not implemented yet.")
        cohort_filter["pheno_filters"] = {"compound": [cohort_filter["pheno_filters"], values_filter], "logic": "and"}
    else:
        cohort_filter["pheno_filters"] = values_filter

    return cohort_filter


def cohort_final_payload(values, entity, field, cohort_filter, project_context):
    final_payload = {}
    final_payload["project_context"] = project_context
    final_payload["filters"] = generate_pheno_filter(values, entity, field, cohort_filter)

    return final_payload
