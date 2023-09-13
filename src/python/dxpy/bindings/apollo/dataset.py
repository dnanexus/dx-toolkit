from dxpy import DXHTTPRequest


class Dataset:
    def __init__(self, record_id, project):
        self.record_id = record_id
        self.project = project

        self.populate_http_request()

    def populate_http_request(self):
        self.http_request_info = DXHTTPRequest(
            "/" + self.record_id + "/visualize",
            {"project": self.project, "cohortBrowser": False},
        )

    @property
    def record_name(self):
        return self.http_request_info.get("recordName")

    @property
    def record_types(self):
        return self.http_request_info.get("recordTypes")

    @property
    def base_sql(self):
        return self.http_request_info.get("recordTypes")

    @property
    def cohort_flag(self):
        return True if "CohortBrowser" in self.record_types else False

    @property
    def cohort_filter(self):
        if "CohortBrowser" in self.record_types:
            return self.http_request_info.get("filters")

    @property
    def cohort_sql_query(self):
        if "CohortBrowser" in self.record_types:
            return self.http_request_info.get("sql")

    @property
    def dataset_id(self):
        return self.http_request_info.get("dataset")

    @property
    def dataset_name(self):
        return self.http_request_info.get("datasetName")

    @property
    def database_id(self):
        return self.http_request_info.get("databases")[0]

    @property
    def vizserver_url(self):
        return self.http_request_info.get("url")
