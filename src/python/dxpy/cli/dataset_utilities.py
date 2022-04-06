from ..bindings import DXRecord
from ..utils.resolver import resolve_existing_path
from ..bindings.dxdataobject_functions import is_dxlink
from ..bindings.dxfile import DXFile
from ..utils.file_handle import as_handle
import sys
import collections
import json

def extract_dataset(args):
    project, path, entity_result = resolve_existing_path(args.path)
    rec = DXDataset(entity_result['id'],project=project)
    rec.get_dxdescriptor()

class DXDataset(DXRecord):
    """
    Generalized object model for DNAnexus datasets.
    Attributes:
    """

    _record_type = "Dataset"

    def __init__(self, dxid=None, project=None):
        DXRecord.__init__(self,dxid=dxid)
        self.describe(default_fields=True, fields={'properties', 'details'})
        assert DXDataset._record_type in self.types
        assert 'descriptor' in self.details
        if is_dxlink(self.details['descriptor']):
           self.descriptor = DXFile(self.details['descriptor'], mode='rb')
        # else:
        #     raise DXError(TODO: )
        self.dxdescriptor = None
        self.name = self.details.get('name')
        self.description = self.details.get('description')
        self.schema = self.details.get('schema')
        self.version = self.details.get('version')
    
    def get_dxdescriptor(self):
        if self.dxdescriptor is None:
            self.dxdescriptor = DXDescriptor(self.descriptor,schema=self.schema)

        return self.dxdescriptor

class DXDescriptor():

    def __init__(self, dxfile, **kwargs):
        python3_5_x = sys.version_info.major == 3 and sys.version_info.minor == 5

        with as_handle(dxfile, is_gzip=True, **kwargs) as f:
            if python3_5_x:
                jsonstr = f.read()
                if type(jsonstr) != str:
                    jsonstr = jsonstr.decode("utf-8")

                obj = json.loads(jsonstr, object_pairs_hook=collections.OrderedDict)
            else:
                obj = json.load(f, object_pairs_hook=collections.OrderedDict)

        self.schema = kwargs.get('schema')
