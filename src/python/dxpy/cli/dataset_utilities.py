# Copyright (C) 2013-2022 DNAnexus, Inc.
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

'''
This module contains functions and classes used for work with datasets
in the dx command-line client.
'''

def extract_dataset(args):
    """
        TODO: Add proper docstring
    """
    # Code for dx extract_dataset command, without parser


class DXDataset(DXRecord):
"""Generalized object model for DNAnexus datasets.

    Attributes:

    """

    _record_type = "Dataset"

    def __init__(self, dxid=None, project=None):
        DXRecord.__init__(self)
        self.describe({default_fields=True, fields={'properties', 'details'}})
        assert cls._record_type in self.types
        assert 'descriptor' in self.details
        if is_dxlink(details['descriptor']):
            self.descriptor = DXFile(details['descriptor'], mode='rb')
        else:
            raise DXError(TODO: )
        self.dxdescriptor = None
        self.name = self.details.get('name')
        self.description = self.details.get('description')
        self.schema = self.details.get('schema')
        self.version = self.details.get('version')

    def get_dxdescriptor(self):
        if self.dxdescriptor is None:
            self.dxdescriptor = DXDescriptor(self.descriptor, {schema=self.schema})

        return self.dxdescriptor

    def get_dxdictionary(self):
        if self.dxdescriptor is None:
            get_dxdescriptor()

        return self.dxdescriptor.get_dxdictionary()

class DXDescriptor():

    def __init__(self, dxfile, **kwargs):
        # TODO: read dxfile and parse it as JSON object,
        # similar load_from_json function in dxdata
        self = cls(**obj)
        self.schema = kwargs.get('schema')


    def get_dxdictionary(self):
        return DXDictionary()

class DXDictionary():

    def __init__(self, descriptor):
        self.data_dictionary =  load_data_dictionary(descriptor)
        self.coding_dictionary = load_coding_dictionary(descriptor)
        self.entity_dictionary = load_entity_dictionary(descriptor)


    def load_data_dictionary(self, descriptor):
        # similar to DataDictionary.from_dataset from dxdata


    def load_coding_dictionary(self, descriptor):
        # similar to DataCoding.from_dataset from dxdata

    def load_entity_dictionary(self, descriptor):
        # similar to EntityDictionary.from_dataset from dxdata
        entity_dictionary = OrderedDict()
        for entity_name in descriptor.model['entities']:
            entity = descriptor.model['entities'][entity_name]
            entity_dictionary[entity_name] = {
                "entity": entity_name,
                "entity_title": entity.get('entity_title'),
                "entity_label_singular": entity.get('entity_label_singular'),
                "entity_label_plural": entity.get('entity_label_plural'),
                "entity_description": entity.get('entity_description')
            }
