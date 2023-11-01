import copy
from functools import reduce


def generate_pheno_filter(values, entity, field, filters, lambda_for_list_conv):

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
        # existing compound pheno_filter.
        raise ValueError("Invalid input cohort. Cohorts must have 'and' logic on the primary entity and field.")

    entity_field = "$".join([entity, field])

    # Search for an existing filter for the entity and field
    for compound_filter in filters["pheno_filters"]["compound"]:
        if "filters" not in compound_filter or entity_field not in compound_filter["filters"]:
            continue
        if "logic" in compound_filter and compound_filter["logic"] != "and":
            raise ValueError("Invalid input cohort. Cohorts must have 'and' logic on the primary entity and field.")
        primary_filters = []
        for primary_filter in compound_filter["filters"][entity_field]:
            if primary_filter["condition"] == "exists":
                pass
            elif primary_filter["condition"] == "in":
                values = sorted(set(values).intersection(set(reduce(lambda_for_list_conv, primary_filter["values"], []))))
            elif primary_filter["condition"] == "not-in":
                values = sorted(set(values) - set(reduce(lambda_for_list_conv, primary_filter["values"], [])))
            else:
                raise ValueError("Invalid input cohort."
                                 " Cohorts cannot have conditions other than \"in\", \"not-in\", or \"exists\" on the primary entity and field.")
        primary_filters.append({"condition": "in", "values": values})
        compound_filter["filters"][entity_field] = primary_filters
        return filters

    entity_field_filter = {"condition": "in", "values": values}

    # Search for an existing filter with the entity since no entity and field filter was found
    for compound_filter in filters["pheno_filters"]["compound"]:
        if "entity" not in compound_filter or "name" not in compound_filter["entity"]:
            continue
        if compound_filter["entity"]["name"] != entity:
            continue
        if "logic" in compound_filter and compound_filter["logic"] != "and":
            raise ValueError("Invalid input cohort. Cohorts must have 'and' logic on the primary entity and field.")
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


def cohort_filter_payload(values, entity, field, filters, project_context, lambda_for_list_conv, base_sql=None):
    if "logic" in filters and filters["logic"] != "and":
        raise ValueError("Invalid input cohort. Cohorts must have 'and' logic on the primary entity and field.")
    filter_payload = {}
    filter_payload["filters"] = generate_pheno_filter(values, entity, field, filters, lambda_for_list_conv)
    if "logic" not in filter_payload["filters"]:
        filter_payload["filters"]["logic"] = "and"
    filter_payload["project_context"] = project_context
    if base_sql is not None:
        filter_payload["base_sql"] = base_sql

    return filter_payload


def cohort_combined_payload(combined):
    combined = copy.copy(combined)
    source = []
    for source_dict in combined["source"]:
        source.append({
            "$dnanexus_link": {
                "id": source_dict["id"],
                "project": source_dict["project"],
            }
        })
    combined["source"] = source

    return combined


def cohort_final_payload(name, folder, project, databases, dataset, schema, filters, sql, base_sql=None, combined=None):
    details = {
        "databases": databases,
        "dataset": {"$dnanexus_link": dataset},
        "description": "",
        "filters": filters,
        "schema": schema,
        "sql": sql,
        "version": "3.0",
    }
    if base_sql is not None:
        details["baseSql"] = base_sql
    if combined is not None:
        details["combined"] = cohort_combined_payload(combined)

    final_payload = {
        "name": name,
        "folder": folder,
        "project": project,
        "types": ["DatabaseQuery", "CohortBrowser"],
        "details": details,
        "close": True,
    }
    if combined is not None:
        final_payload["types"].append("CombinedDatabaseQuery")

    return final_payload
