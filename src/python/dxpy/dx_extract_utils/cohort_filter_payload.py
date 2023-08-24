import copy

from ..exceptions import InvalidInput


def generate_pheno_filter(values, entity, field, cohort_filter):
    cohort_filter = copy.deepcopy(cohort_filter)

    if "pheno_filters" not in cohort_filter:
        # No pheno_filter in filters so create a compound template
        cohort_filter["pheno_filters"] = {"compound": [], "logic": "and"}

    if "compound" not in cohort_filter["pheno_filters"]:
        # No compound filter in pheno_filter so move the existing pheno_fitler into a compound filter
        cohort_filter["pheno_filters"] = {"compound": [cohort_filter["pheno_filters"]], "logic": "and"}
    elif "logic" in cohort_filter["pheno_filters"] and cohort_filter["pheno_filters"]["logic"] != "and":
        print("WARNING: pheno_filters logic is not \"and\", creating a nested compound filter")
        # The compound filter in the pheno_filter is not "and" so move the existing pheno_fitler into a compound filter
        cohort_filter["pheno_filters"] = {"compound": [cohort_filter["pheno_filters"]], "logic": "and"}

    entity_field = "$".join([entity, field])
    entity_field_filter = {"condition": "in", "values": values}

    # Search for an existing filter for the entity and field
    for compound_filter in cohort_filter["pheno_filters"]["compound"]:
        try:
            if "logic" in compound_filter and compound_filter["logic"] != "and":
                # The entity and field filter does not have "and" logic so fall though
                print("WARNING: found entity field filter, but logic is not \"and\"")
            else:
                # The entity and field filter is valid for addition of the "in" values condition
                compound_filter["filters"][entity_field].append(entity_field_filter)
                return cohort_filter
        except KeyError:
            pass

    # Search for an existing filter with the entity since no entity and field filter was found
    for compound_filter in cohort_filter["pheno_filters"]["compound"]:
        if "entity" not in compound_filter or "name" not in compound_filter["entity"]:
            continue
        if compound_filter["entity"]["name"] == entity:
            if "logic" in compound_filter and compound_filter["logic"] != "and":
                # The entity filter does not have "and" logic so fall though
                print("WARNING: found entity filter, but logic is not \"and\"")
            else:
                # The entity filter is valid for addition of field filter
                compound_filter["filters"][entity_field] = [entity_field_filter]
                return cohort_filter

    # Search for an existing filter with the entity in a filter with a different field since no entity was found
    for compound_filter in cohort_filter["pheno_filters"]["compound"]:
        if "filters" not in compound_filter:
            continue
        for other_entity_field in compound_filter["filters"]:
            if other_entity_field.split("$")[0] == entity:
                if "logic" in compound_filter and compound_filter["logic"] != "and":
                    # The entity filter does not have "and" logic so fall though
                    print("WARNING: found entity filter, but logic is not \"and\"")
                else:
                    # The entity filter is valid for addition of field filter
                    compound_filter["filters"][entity_field] = [entity_field_filter]
                    return cohort_filter

    # No existing filters for the entity were foudn so create the entity filter
    cohort_filter["pheno_filters"]["compound"].append({
        "name": "phenotype",
        "logic": "and",
        "filters": {
            entity_field: [entity_field_filter],
        },
    })

    return cohort_filter


def cohort_final_payload(values, entity, field, cohort_filter, project_context):
    if "logic" in cohort_filter and cohort_filter["logic"] != "and":
        raise NotImplementedError("cannot filter cohort when top-level logic is not \"and\"")
    final_payload = {}
    final_payload["project_context"] = project_context
    final_payload["filters"] = generate_pheno_filter(values, entity, field, cohort_filter)
    if "logic" not in final_payload["filters"]:
        final_payload["filters"]["logic"] = "and"

    return final_payload
