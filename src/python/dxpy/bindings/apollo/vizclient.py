from __future__ import print_function
import dxpy

class VizClient(object):
    def __init__(self, url, project_id, error_handler=print):
        self.url = url
        self.project_id = project_id
        self.error_handler = error_handler

    def _get_response(self, payload, resource_url):
        try:
            response = dxpy.DXHTTPRequest(
                resource=resource_url, data=payload, prepend_srv=False
            )
            if "error" in response:
                if response["error"]["type"] == "InvalidInput":
                    err_message = (
                        "Insufficient permissions due to the project policy.\n"
                        + response["error"]["message"]
                    )
                elif response["error"]["type"] == "QueryTimeOut":
                    err_message = "Please consider using --sql option to generate the SQL query and execute query via a private compute cluster."
                else:
                    err_message = response["error"]
                self.error_handler(str(err_message))
            return response
        except Exception as details:
            self.error_handler(str(details))

    def get_data(self, payload, record_id):
        resource_url = "{}/data/3.0/{}/raw".format(self.url, record_id)
        return self._get_response(payload, resource_url)

    def get_raw_sql(self, payload, record_id):
        resource_url = "{}/viz-query/3.0/{}/raw-query".format(self.url, record_id)
        return self._get_response(payload, resource_url)
