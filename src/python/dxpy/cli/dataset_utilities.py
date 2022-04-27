import sys
import collections
import json
import pandas as pd
import os
import re
import logging
from ..bindings import DXRecord
from ..bindings.dxdataobject_functions import is_dxlink
from ..bindings.dxfile import DXFile
from ..utils.resolver import resolve_existing_path
from ..utils.file_handle import as_handle
from ..exceptions import DXError

_logger = logging.getLogger(__name__)

database_unique_name_regex = re.compile('^database_\w{24}__\w+$')

def extract_dataset(args):
    project, path, entity_result = resolve_existing_path(args.path)
    rec = DXDataset(entity_result['id'],project=project)
    rec_json = rec.get_descriptor()
    rec_dict = rec.get_dictionary().write(output_path="")



    
class DXDataset(DXRecord):
    """
    Generalized object model for DNAnexus datasets.
    Attributes:
    """

    _record_type = "Dataset"

    def __init__(self, dxid=None, project=None):
        super(DXDataset, self).__init__(dxid, project)
        self.describe(default_fields=True, fields={'properties', 'details'})
        assert self._record_type in self.types
        assert 'descriptor' in self.details
        if is_dxlink(self.details['descriptor']):
           self.descriptor_dxfile = DXFile(self.details['descriptor'], mode='rb')
        else:
            raise DXError('Invalid link: %r' % self.details['descriptor'])
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
    """
        A class to represent data, coding and entity dictionaries based on the descriptor. 
        All 3 dictionaries will have the same internal representation as dictionaries of string to pandas dataframe.
        Write function writes the 3 dataframes to output.
    """
    def __init__(self, descriptor):
        self.data_dictionary =  self.load_data_dictionary(descriptor)
        self.coding_dictionary = self.load_coding_dictionary(descriptor)
        self.entity_dictionary = self.load_entity_dictionary(descriptor)
    
    def load_data_dictionary(self, descriptor):
        """
            Processes data dictionary from descriptor
        """
        eblocks = collections.OrderedDict()
        join_path_to_entity_field = collections.OrderedDict()
        for entity_name in descriptor.model['entities']:
            eblocks[entity_name] = self.create_entity_dframe(descriptor.model['entities'][entity_name], 
                                        is_primary_entity=(entity_name==descriptor.model["global_primary_key"]["entity"]),
                                        global_primary_key=(descriptor.model["global_primary_key"]))

            join_path_to_entity_field.update(self.get_join_path_to_entity_field_map(descriptor.model['entities'][entity_name]))

        _EXCLUDE_EDGES_FOR_TABLES = ("raw_file_metadata")
        edges = []
        for ji in descriptor.join_info:
            skip_edge = False
            
            for path in [ji["joins"][0]["to"], ji["joins"][0]["from"]]:
                if path not in join_path_to_entity_field:
                    skip_edge = True
                    db_name,table_name,col_name = path.split("$")
                    if table_name in _EXCLUDE_EDGES_FOR_TABLES:
                        continue
                    db_tb_name = "{}${}".format(db_name, table_name)
                    print("ji:", ji)
                    if db_tb_name not in join_path_to_entity_field:
                        _logger.warning("Skipping edge for : " + db_tb_name)
                        continue
                    else:
                        logging.debug("{} present in join_path_to_entity_field. But skip adding corresponding {} \
                                      to join_path_to_entity_field".format(db_tb_name, path))
                        continue

            if not skip_edge:
                edges.append(self.create_edge(ji, join_path_to_entity_field))

        for edge in edges:
            source_eblock = eblocks.get(edge["source_entity"])
            if not source_eblock.empty:
                eb_row_idx = (source_eblock["name"] == edge["source_field"])
                if eb_row_idx.sum() != 1:
                    raise ValueError("Invalid edge: " + str(edge))

                ref = source_eblock["referenced_entity_field"].values
                rel = source_eblock["relationship"].values
                ref[eb_row_idx] = "{}:{}".format(edge["destination_entity"], edge["destination_field"])
                rel[eb_row_idx] = edge["relationship"]

                source_eblock = source_eblock.assign(relationship=rel, referenced_entity_field=ref)
            else:
                print("No entity for: ", edge["source_entity"], " for edge: ", edge)
        return eblocks

    def create_entity_dframe(self, entity, is_primary_entity, global_primary_key):
        """
            Returns DataDictionary pandas DataFrame for an entity.
        """
        required_columns = [
            "entity", 
            "name", 
            "type", 
            "primary_key_type"
        ]

        extra_cols = [
            "coding_name",
            "concept",
            "description",
            "folder_path",
            "is_multi_select",
            "is_sparse_coding",
            "linkout",
            "longitudinal_axis_type",
            "referenced_entity_field",
            "relationship",
            "title",
            "units",
        ]
        dcols = {col: [] for col in required_columns + extra_cols}
        dcols["entity"] = [entity["name"]] * len(entity["fields"])
        dcols["referenced_entity_field"] = [""] * len(entity["fields"])
        dcols["relationship"] = [""] * len(entity["fields"])

        for field in entity["fields"]:
            # Field-level parameters
            field_dict = entity["fields"][field]
            dcols["name"].append(field_dict["name"])
            dcols["type"].append(field_dict["type"])
            dcols["primary_key_type"].append(
                ("global" if is_primary_entity else "local")
                if (global_primary_key["field"] and field_dict["name"] == global_primary_key["field"])
                else "")
            # Optional cols to be filled in with blanks regardless
            dcols["coding_name"].append(field_dict["coding_name"] if field_dict["coding_name"] else "")
            dcols["concept"].append(field_dict["concept"])
            dcols["description"].append(field_dict["description"])
            dcols["folder_path"].append(
                " > ".join(field_dict["folder_path"])
                if ("folder_path" in field_dict.keys() and field_dict["folder_path"])
                else "")
            dcols["is_multi_select"].append("yes" if field_dict["is_multi_select"] else "")
            dcols["is_sparse_coding"].append("yes" if field_dict["is_sparse_coding"] else "")
            dcols["linkout"].append(field_dict["linkout"])
            dcols["longitudinal_axis_type"].append(field_dict["longitudinal_axis_type"] 
                                                    if field_dict["longitudinal_axis_type"] else "")
            dcols["title"].append(field_dict["title"])
            dcols["units"].append(field_dict["units"])

        try:
            dframe = pd.DataFrame(dcols)
        except ValueError as exc:
            raise exc

        return dframe

    def get_join_path_to_entity_field_map(self, entity):
        """
            Returns map with "database$table$column", "unique_database$table$column",
            as keys and values are (entity, field)
        """
        join_path_to_entity_field = collections.OrderedDict()
        for field in entity["fields"]:
            field_value = entity["fields"][field]["mapping"]
            db_tb_col_path = "{}${}${}".format(field_value["database_name"], field_value["table"], field_value["column"])
            join_path_to_entity_field[db_tb_col_path] = (entity["name"],field)

            if field_value["database_unique_name"] and database_unique_name_regex.match(field_value["database_unique_name"]):  
                unique_db_tb_col_path = "{}${}${}".format(field_value["database_unique_name"], field_value["table"], field_value["column"])
                join_path_to_entity_field[unique_db_tb_col_path] = (entity["name"], field)
        return join_path_to_entity_field

    def create_edge(self, join_info_joins, join_path_to_entity_field):
        """
        Convert join_info to Edge[]
        """
        edge = collections.OrderedDict()
        column_to = join_info_joins["joins"][0]["to"]
        column_from = join_info_joins["joins"][0]["from"]
        edge["source_entity"], edge["source_field"] = join_path_to_entity_field[column_to]
        edge["destination_entity"], edge["destination_field"] = join_path_to_entity_field[column_from]
        edge["relationship"] = join_info_joins["relationship"]
        return edge

    def load_coding_dictionary(self, descriptor):
        """
            Processes coding dictionary from descriptor
        """
        cblocks = collections.OrderedDict()
        for entity in descriptor.model['entities']:
            for field in descriptor.model['entities'][entity]["fields"]:
                coding_name_value = descriptor.model['entities'][entity]["fields"][field]["coding_name"]
                if coding_name_value and coding_name_value not in cblocks:
                    cblocks[coding_name_value] = self.create_coding_name_dframe(descriptor.model, entity, field, coding_name_value)
        return cblocks

    def create_coding_name_dframe(self, model, entity, field, code):
        dcols = {}
        if model['entities'][entity]["fields"][field]["is_hierarchical"]:
            def unpack_hierarchy(nodes, parent_code):
                """Serialize the node hierarchy by depth-first traversal.

                Yields: tuples of (code, parent_code)
                """
                for node in nodes:
                    if isinstance(node, dict):
                        next_parent_code, child_nodes = next(iter(node.items()))
                        # internal: unpack recursively
                        yield next_parent_code, parent_code
                        for deep_node, deep_parent in unpack_hierarchy(child_nodes,
                                next_parent_code):
                            yield (deep_node, deep_parent)
                    else:
                        # terminal: serialize
                        yield (node, parent_code)

            all_codes, parents = zip(*unpack_hierarchy(model["codings"][code]["display"], ""))
            dcols.update({
                "code": all_codes,
                "parent_code": parents,
                "meaning": [model["codings"][code]["codes_to_meanings"][c] for c in all_codes],
            })
        else:
            # No hierarchy; just unpack the codes dictionary
            codes, meanings = zip(*model["codings"][code]["codes_to_meanings"].items())
            dcols.update({"code": codes, "meaning": meanings})

        dcols["coding_name"] = [code] * len(dcols["code"])
        
        try:
            dframe = pd.DataFrame(dcols)
        except ValueError as exc:
            raise exc

        return dframe

    def load_entity_dictionary(self, descriptor):
        """
            Processes entity dictionary from descriptor
        """
        entity_dictionary = collections.OrderedDict()
        for entity_name in descriptor.model['entities']:
            entity = descriptor.model['entities'][entity_name]
            entity_dictionary[entity_name] = pd.DataFrame.from_dict([{
                "entity": entity_name,
                "entity_title": entity.get('entity_title'),
                "entity_label_singular": entity.get('entity_label_singular'),
                "entity_label_plural": entity.get('entity_label_plural'),
                "entity_description": entity.get('entity_description')
            }])
        return entity_dictionary

    def write(self, output_path="", sep=","):
        """Create CSV files with the contents of the dictionaries.
        """
        csv_opts = dict(
            sep=sep,
            header=True,
            index=False,
            na_rep="",
        )
        def sort_dataframe_columns(dframe, required_columns):
            """Sort dataframe columns alphabetically but with `required_columns` first."""
            extra_cols = dframe.columns.difference(required_columns)
            sorted_cols = list(required_columns) + extra_cols.sort_values().tolist()
            return dframe.loc[:, sorted_cols]
        
        def as_dataframe(ord_dict_of_df, required_columns):
            """Join all blocks into a pandas DataFrame."""
            df = pd.concat([b for b in ord_dict_of_df.values()], sort=False)
            return sort_dataframe_columns(df, required_columns)

        coding_dframe = as_dataframe(self.data_dictionary, required_columns = ["entity", "name", "type", "primary_key_type"])
        output_file = os.path.join(output_path,"data_dictionary.csv")
        coding_dframe.to_csv(output_file, **csv_opts)

        coding_dframe = as_dataframe(self.coding_dictionary, required_columns=["coding_name", "code", "meaning"])
        output_file = os.path.join(output_path,"coding_dictionary.csv")
        coding_dframe.to_csv(output_file, **csv_opts)
        
        entity_dframe = as_dataframe(self.entity_dictionary, required_columns=["entity", "entity_title"])
        output_file = os.path.join(output_path,"entity_dictionary.csv")
        entity_dframe.to_csv(output_file, **csv_opts)
