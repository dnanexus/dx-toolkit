import copy


def generate_pheno_filter(values, entity, field, filters):
    filters = copy.deepcopy(filters)

    if "pheno_filters" not in filters:
        # Create a pheno_filter if none exists. This will be a compound filter
        # even though only be one filter defined for the entity field values.
        filters["pheno_filters"] = {"compound": [], "logic": "and"}

    if "compound" not in filters["pheno_filters"]:
        # Only compound pheno_filters are supported, and the existing
        # pheno_filter is not a compound filter. Move the pheno_filter into a
        # compound filter so a filter for the primary entity field values can
        # be added.
        filters["pheno_filters"] = {"compound": [filters["pheno_filters"]], "logic": "and"}
    elif "logic" in filters["pheno_filters"] and filters["pheno_filters"]["logic"] != "and":
        # pheno_filter is a compound filter, but the logic is not "and" so
        # entity field values cannot be selected by adding a filter to the
        # existing compound pheno_filter. Move the existing pheno_filter into a
        # new pheno_filter level compound filter.
        raise ValueError("Invalid input cohort. Cohorts must have “and” logic on the primary entity and field.")
        filters["pheno_filters"] = {"compound": [filters["pheno_filters"]], "logic": "and"}

    entity_field = "$".join([entity, field])
    entity_field_filter = {"condition": "in", "values": values}

    # Search for an existing filter for the entity and field
    for compound_filter in filters["pheno_filters"]["compound"]:
        if "filters" not in compound_filter or entity_field not in compound_filter["filters"]:
            continue
        if "logic" in compound_filter and compound_filter["logic"] != "and":
            raise ValueError("Invalid input cohort. Cohorts must have “and” logic on the primary entity and field.")
        # The entity field filter is valid for addition of the "in" values condition
        # Add to an existing "in" condition if one exists for the entity and field
        for condition in compound_filter["filters"][entity_field]:
            if condition["condition"] == "in":
                for value in values:
                    if value in condition["values"]:
                        continue
                    condition["values"].append(value)
                return filters
        # Create a new "in" condition for the entity and field
        compound_filter["filters"][entity_field].append(entity_field_filter)
        return filters

    # Search for an existing filter with the entity since no entity and field filter was found
    for compound_filter in filters["pheno_filters"]["compound"]:
        if "entity" not in compound_filter or "name" not in compound_filter["entity"]:
            continue
        if compound_filter["entity"]["name"] != entity:
            continue
        if "logic" in compound_filter and compound_filter["logic"] != "and":
            raise ValueError("Invalid input cohort. Cohorts must have “and” logic on the primary entity and field.")
        # The entity filter is valid for addition of field filter
        compound_filter["filters"][entity_field] = [entity_field_filter]
        return filters

    # Search for an existing filter with the entity in a filter with a different field since no entity was found
    for compound_filter in filters["pheno_filters"]["compound"]:
        if "filters" not in compound_filter:
            continue
        for other_entity_field in compound_filter["filters"]:
            if other_entity_field.split("$")[0] != entity:
                continue
            if "logic" in compound_filter and compound_filter["logic"] != "and":
                continue
            # Filter with the entity is valid for addition of field filter
            compound_filter["filters"][entity_field] = [entity_field_filter]
            return filters

    # No existing filters for the entity were found so create the entity filter
    filters["pheno_filters"]["compound"].append({
        "name": "phenotype",
        "logic": "and",
        "filters": {
            entity_field: [entity_field_filter],
        },
    })

    return filters


def cohort_final_payload(values, entity, field, filters, project_context):
    if "logic" in filters and filters["logic"] != "and":
        raise ValueError("Invalid input cohort. Cohorts must have “and” logic on the primary entity and field.")
    final_payload = {}
    final_payload["project_context"] = project_context
    final_payload["filters"] = generate_pheno_filter(values, entity, field, filters)
    if "logic" not in final_payload["filters"]:
        final_payload["filters"]["logic"] = "and"

    return final_payload
