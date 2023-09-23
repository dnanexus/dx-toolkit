import dxpy

class VizClient(object):
    def __init__(self) -> None:
        pass
    def get_data(payload,record_id):
        resource_val = resp["url"] + "/data/3.0/" + resp["dataset"] + "/raw"
        try:    
            resp_raw = dxpy.DXHTTPRequest(
                resource=resource_val, data=payload, prepend_srv=False
            )
        except:
            pass
    def get_raw_sql(payload,record_id):
        pass