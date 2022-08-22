import os
import dxpy

def get_template_dir():
    return os.path.join(os.path.dirname(dxpy.__file__), 'templating', 'templates', 'nextflow')