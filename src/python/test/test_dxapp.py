#!/usr/bin/env python

import dxpy.bindings as dxpy

def main(json_dxid):
    json = dxpy.DXJSON(json_dxid)
    json.set({"appsuccess": True})
