class VizPayloadBuilder(object):
    """
    'filters' and/or 'raw_filters' can be built with the help of vizserver_filters_from_json_parser.JSONFiltersValidator

    """

    def __init__(
        self,
        project_context,
        output_fields_mapping,
        raw_filters,
        filters,
        limit,
        base_sql=None,
        is_cohort=False,
        return_query=False,
    ):
        self.project_context = project_context
        self.output_fields_mapping = output_fields_mapping
        self.raw_filters = raw_filters
        self.filters = filters
        self.limit = limit
        self.base_sql = base_sql
        self.is_cohort = is_cohort
        self.return_query = return_query

    def build(self):
        if self.is_cohort and self.base_sql:
            ...

        if self.base_sql and not self.is_cohort:
            ...

    def assemble_raw_filters(self, assay_name, assay_id, filters):
        """
        filters may be a dict with the following structure:
        {
            "logic": "or",
            "compound: [
                {
                    ...
                }
            ]
        }
        """
        return {
            "raw_filters": {
                "assay_filters": {"name": assay_name, "id": assay_id, **filters}
            }
        }

    def get_vizserver_payload_structure(self):
        return {
            "project_context": self.project_context,
            "fields": self.output_fields_mapping,
            "limit": self.limit if self.limit else None,
            "return_query": self.return_query,
        }
