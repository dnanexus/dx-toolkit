class VizPayloadBuilder(object):
    """

    'filters' and/or 'raw_filters' can be built with the help of vizserver_filters_from_json_parser.JSONFiltersValidator

    assemble_raw_filters is a helper method to build a complete raw_filters structure for a single assay,
    if raw_filters does not already have this information.

    Example usage:

    payload = VizPayloadBuilder(
        "project-xyz",
        {
            "feature_id": "expr_annotation$feature_id",
            "sample_id": "expression$sample_id",
            "expression": "expression$value",
        },
        error_handler=print,
    )

    payload.assemble_raw_filters(
        assay_name="xyz",
        assay_id="a-b1-2c-f-xyz-test",
        filters={
            "logic": "or",
            "compound": [
                {
                    "test": "1",
                },
                {"exmaple": 3},
            ],
        },
    )

    final_payload = payload.build()


    """

    def __init__(
        self,
        project_context,
        output_fields_mapping,
        raw_filters=None,
        filters=None,
        limit=None,
        base_sql=None,
        is_cohort=False,
        return_query=False,
        error_handler=None,
    ):
        self.project_context = project_context
        self.output_fields_mapping = output_fields_mapping
        self.raw_filters = raw_filters
        self.filters = filters
        self.limit = limit
        self.base_sql = base_sql
        self.is_cohort = is_cohort
        self.return_query = return_query
        self.error_handler = error_handler

        if self.error_handler is None:
            raise Exception("error_handler must be defined")

    def build(self):
        
        payload = self.get_vizserver_payload_structure()

        if self.is_cohort and self.base_sql:
            self.validate_base_sql()
            payload.update({"base_sql": self.base_sql, "is_cohort": self.is_cohort})

        if self.base_sql and not self.is_cohort:
            self.error_handler(
                "base_sql is only allowed for cohorts. is_cohort must be set to True"
            )
        
        if self.limit:
            self.validate_returned_records_limit()
            payload.update({"limit": self.limit})

        if self.raw_filters:
            payload.update(self.raw_filters)

        if self.filters:
            payload.update(self.filters)

        return payload

    def assemble_raw_filters(self, assay_name, assay_id, filters):
        """
        Helper method to build a complete raw_filters structure for a single assay 
        if raw_filters does not already have this information.

        filters may be a dict with the following structure:
        {
            "logic": "or",
            "compound": [
                {
                    ...
                }
            ]
        }

        or any other structure that is accepted by vizserver within assay_filters, e.g.
        {
            "filters": {
                ...
            }
        }
        """
        raw_filters = {
            "raw_filters": {
                "assay_filters": {"name": assay_name, "id": assay_id,}
            }
        }

        raw_filters["raw_filters"]["assay_filters"].update(filters)

        self.raw_filters = raw_filters

    def get_vizserver_payload_structure(self):
        return {
            "project_context": self.project_context,
            "fields": self.output_fields_mapping,
            "return_query": self.return_query,
        }
    
    def validate_base_sql(self):
        if not isinstance(self.base_sql, str) or "".__eq__(self.base_sql):
            self.error_handler("base_sql is either not a string or is empty")

    def validate_returned_records_limit(self):
        if not isinstance(self.limit, int) or self.limit <= 0:
            self.error_handler("limit must be a positive integer")