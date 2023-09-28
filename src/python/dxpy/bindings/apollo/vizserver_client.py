import dxpy

class VizClient(object):
    def __init__(self, url,project_id,record_id, error_handler=print) -> None:
        self.url = url
        self.project_id = project_id
        self.record_id = record_id
        self.error_handler = error_handler

    def get_data(self,payload):
        resource_val = "{}/data/3.0/{}/raw".format(self.url,self.record_id)
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
        return(raw_response)

    def get_raw_sql(self,payload):
        resource_val = "{}/viz-query/3.0/{}/raw-query".format(self.url,self.record_id)
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
        return(raw_query_response)

