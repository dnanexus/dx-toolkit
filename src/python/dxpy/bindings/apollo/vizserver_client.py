import dxpy

class VizClient(object):
    def __init__(self, record_id, project, error_handler=print) -> None:
        self.visualize_response = dxpy.DXHTTPRequest(
                "/" + record_id + "/visualize",
                {"project": project, "cohortBrowser": False},
            )
        self.error_handler = error_handler

    def get_data(self,payload):
        resource_val = self.visualize_response["url"] + "/data/3.0/" + self.visualize_response["dataset"] + "/raw"
        try:    
            raw_response = dxpy.DXHTTPRequest(
                resource=resource_val, data=payload, prepend_srv=False
            )
            if "error" in raw_response.keys():
                if raw_response["error"]["type"] == "InvalidInput":
                    err_message = "Insufficient permissions due to the project policy.\n" + raw_response["error"]["message"]
                elif raw_response["error"]["type"] == "QueryBuilderError" and raw_response["error"]["details"] == "rsid exists in request filters without rsid entries in rsid_lookup_table.":
                    err_message = "At least one rsID provided in the filter is not present in the provided dataset or cohort"
                else:
                    err_message = raw_response["error"]
                self.error_handler(str(err_message))
        except Exception as details:
            self.error_handler(str(details))

    def get_raw_sql(self,payload):
        resource_val = self.visualize_response["url"] + "/viz-query/3.0/" + self.visualize_response["dataset"] + "/raw-query"
        try:    
            raw_query_response = dxpy.DXHTTPRequest(
                resource=resource_val, data=payload, prepend_srv=False
            )
            if "error" in raw_query_response.keys():
                if raw_query_response["error"]["type"] == "InvalidInput":
                    err_message = "Insufficient permissions due to the project policy.\n" + raw_query_response["error"]["message"]
                elif raw_query_response["error"]["type"] == "QueryBuilderError" and raw_query_response["error"]["details"] == "rsid exists in request filters without rsid entries in rsid_lookup_table.":
                    err_message = "At least one rsID provided in the filter is not present in the provided dataset or cohort"
                else:
                    err_message = raw_query_response["error"]
                self.error_handler(str(err_message))
        except Exception as details:
            self.error_handler(str(details))
