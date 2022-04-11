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
    rec_json = rec.get_descriptor()
    rec_dict = rec.get_dictionary()

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
           self.descriptor_dxfile = DXFile(self.details['descriptor'], mode='rb')
        # else:
        #     raise DXError(TODO: )
        self.descriptor = None
        self.name = self.details.get('name')
        self.description = self.details.get('description')
        self.schema = self.details.get('schema')
        self.version = self.details.get('version')
    
    def get_descriptor(self):
        if self.descriptor is None:
            self.descriptor = DXDatasetDescriptor(self.descriptor_dxfile,schema=self.schema)

        return self.descriptor

    def get_dictionary(self):
        if self.descriptor is None:
            self.get_descriptor()

        return self.descriptor.get_dictionary()

class DXDatasetDescriptor():

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
        
        for key in obj:
            setattr(self,key, obj[key])
        self.schema = kwargs.get('schema')

    def get_dictionary(self):
        return DXDatasetDictionary(self)

class DXDatasetDictionary():
    def __init__(self, descriptor):
        self.data_dictionary =  self.load_data_dictionary(descriptor)
        self.entity_dictionary = self.load_entity_dictionary(descriptor)
    
    def load_data_dictionary(self,descriptor):
        pass

    def load_entity_dictionary(self, descriptor):
        entity_dictionary = collections.OrderedDict()
        for entity_name in descriptor.model['entities']:
            entity = descriptor.model['entities'][entity_name]
            entity_dictionary[entity_name] = {
                "entity": entity_name,
                "entity_title": entity.get('entity_title'),
                "entity_label_singular": entity.get('entity_label_singular'),
                "entity_label_plural": entity.get('entity_label_plural'),
                "entity_description": entity.get('entity_description')
            }
        return entity_dictionary
