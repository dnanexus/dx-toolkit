import dxpy


class VizClient(object):
    def __init__(self, url, project_id, record_id, error_handler=print) -> None:
        self.url = url
        self.project_id = project_id
        self.record_id = record_id
        self.error_handler = error_handler

    def get_response(self,payload,sql=False):
        if sql:
            resource_val = "{}/viz-query/3.0/{}/raw-query".format(self.url, self.record_id)
        else:
            resource_val = "{}/data/3.0/{}/raw".format(self.url, self.record_id)

        try:
            response = dxpy.DXHTTPRequest(
                resource=resource_val, data=payload, prepend_srv=False
            )
            if "error" in response:
                if response["error"]["type"] == "InvalidInput":
                    err_message = (
                        "Insufficient permissions due to the project policy.\n"
                        + response["error"]["message"]
                    )
                else:
                    err_message = response["error"]
                self.error_handler(str(err_message))
        except Exception as details:
            self.error_handler(str(details))
        return response
