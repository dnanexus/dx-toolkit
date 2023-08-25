import copy


def generate_pheno_filter(values, entity, field, cohort_filter):
    cohort_filter = copy.deepcopy(cohort_filter)

    if "pheno_filters" not in cohort_filter:
        # Create a pheno_filter if none exists. This will be a compound filter
        # even though only be one filter defined for the entity field values.
        cohort_filter["pheno_filters"] = {"compound": [], "logic": "and"}

    if "compound" not in cohort_filter["pheno_filters"]:
        # Only compound pheno_filters are supported, and the existing
        # pheno_filter is not a compound filter. Move the pheno_filter into a
        # compound filter so a filter for the primary entity field values can
        # be added.
        cohort_filter["pheno_filters"] = {"compound": [cohort_filter["pheno_filters"]], "logic": "and"}
    elif "logic" in cohort_filter["pheno_filters"] and cohort_filter["pheno_filters"]["logic"] != "and":
        # pheno_filter is a compound filter, but the logic is not "and" so
        # entity field values cannot be selected by adding a filter to the
        # existing compound pheno_filter. Move the existing pheno_filter into a
        # new pheno_filter level compound filter.
        print("WARNING: pheno_filter compound filter logic is not \"and\", creating a nested compound filter."
              " The Cohort Browser may not work as expected with this cohort.")
        cohort_filter["pheno_filters"] = {"compound": [cohort_filter["pheno_filters"]], "logic": "and"}

    entity_field = "$".join([entity, field])
    entity_field_filter = {"condition": "in", "values": values}

    # Search for an existing filter for the entity and field
    for compound_filter in cohort_filter["pheno_filters"]["compound"]:
        try:
            if "logic" in compound_filter and compound_filter["logic"] != "and":
                # The entity field filter does not have "and" logic so fall though
                print("WARNING: found filter for entity \"{}\", field \"{}\" but logic is not \"and\","
                      " skipping".format(entity, field))
            else:
                # The entity field filter is valid for addition of the "in" values condition
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
                print("WARNING: found filter for entity \"{}\" but logic is not \"and\", skipping".format(entity))
            else:
                # The entity filter is valid for addition of field filter
                compound_filter["filters"][entity_field] = [entity_field_filter]
                return cohort_filter

    # Search for an existing filter with the entity in a filter with a different field since no entity was found
    for compound_filter in cohort_filter["pheno_filters"]["compound"]:
        if "filters" not in compound_filter:
            continue
        for other_entity_field in compound_filter["filters"]:
            print(compound_filter)
            if other_entity_field.split("$")[0] == entity:
                if "logic" in compound_filter and compound_filter["logic"] != "and":
                    # The filter with entity does not have "and" logic so fall though
                    print("WARNING: found filter with entity \"{}\" but logic is not \"and\", skipping".format(entity))
                else:
                    # Filter with the entity is valid for addition of field filter
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
