#!/usr/bin/env python
#
# Copyright (C) 2013 DNAnexus, Inc.
#
# This file is part of dx-toolkit (DNAnexus platform client libraries).
#
#   Licensed under the Apache License, Version 2.0 (the "License"); you may not
#   use this file except in compliance with the License. You may obtain a copy
#   of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#   WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#   License for the specific language governing permissions and limitations
#   under the License.

import os, sys, json, pprint
from optparse import OptionParser
import dxpy

description = "Convert a pipeline (local JSON file or pipeline DXRecord id) to a program directory (to be uploaded with dx_build_program)."
parser = OptionParser(usage="%prog pipeline -o program_dir [options]", description=description)
parser.add_option("-o", "--output", help="directory to output the resulting program into")
(opts, args) = parser.parse_args()
pipeline_json = args[0]

if len(args) != 1:
    parser.print_help()
    parser.error("Incorrect number of arguments")

if opts.output is None:
    opts.output = pipeline_json+"_dxprogram"

pipeline = json.load(open(pipeline_json)) if os.path.exists(pipeline_json) else dxpy.DXRecord(pipeline_json).get_details()['details']

#program_spec = {'name': 'Compiled from '+pipeline['name'],
#                'dxapi': '1.03rc',
#                'run': {'file': 'compiled_pipeline.py',
#                        'interpreter': 'python2.7'}}

code_fh = sys.stdout

preamble = '''#!/usr/bin/env python
import dxpy

programs={}
jobs={}
'''

print >>code_fh, preamble

class dictWithLiterals:
    def __init__(self, input):
        self.input = input

    def __repr__(self):
        if isinstance(self.input, dict) and 'job' in self.input:
            strrep = "{"
            for key, value in self.input.iteritems():
                if key == 'job':
                    strrep += "'%s': %s, " % (str(key), str(value))
                else:
                    strrep += "'%s': '%s', " % (str(key), str(value))
            strrep += "}"
            return strrep
        return str(self.input)

pp = pprint.PrettyPrinter(depth=9, indent=4)

for stage in pipeline['stages']:
    inputs = stage['inputs']
    for input, value in list(inputs.iteritems()):
        if isinstance(value, dict) and '$pipeline_link' in value:
            link = value['$pipeline_link']
            inputs[input] = dictWithLiterals({'job': "jobs['%s']" % link['stage'], 'field': link['output']})
    print >>code_fh, "programs['{stage_id}'] = dxpy.DXProgram({program})".format(stage_id=stage['id'], program=stage['program'])
    print >>code_fh, ""
    print >>code_fh, "inputs = "+pp.pformat(inputs)
    print >>code_fh, ""    
    print >>code_fh, "jobs['{stage_id}'] = programs['{stage_id}'].run(inputs)".format(stage_id=stage['id'])
    print >>code_fh, ""
