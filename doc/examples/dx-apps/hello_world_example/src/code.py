#!/usr/bin/env python
# hello_world_example 0.0.1

import dxpy

@dxpy.entry_point('main')
def main(name):
    return {'greeting': 'Hello, %s!' % (name,)}

dxpy.run()
