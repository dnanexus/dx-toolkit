import sys
import collections
import json
import pandas as pd
import os
import re
import csv
import dxpy
from ..utils.printing import (fill)
from ..bindings import DXRecord
from ..bindings.dxdataobject_functions import is_dxlink
from ..bindings.dxfile import DXFile
from ..utils.resolver import resolve_existing_path
from ..utils.file_handle import as_handle
from ..exceptions import DXError, err_exit

database_unique_name_regex = re.compile('^database_\w{24}__\w+$')
database_id_regex = re.compile('^database-\\w{24}$')

def extract_dataset(args):
    if not args.dump_dataset_dictionary and args.fields is None:
        raise DXError('Atleast one of the arguments `-ddd/--dump_dataset_dictionary` or `--fields` expected')
    delimiter = ','
    if args.delim is not None:
        if len(args.delim) == 1:
            delimiter = args.delim
        else:
            raise DXError('Invalid delimiter specified')

    project, path, entity_result = resolve_existing_path(args.path)

    resp = dxpy.DXHTTPRequest('/' + entity_result['id'] + '/visualize',
                                        {"project": project, "cohortBrowser": False} )
    
    if "Dataset" in resp['recordTypes']:
        pass
    elif "CohortBrowser" in resp['recordTypes']:
        project = resp['datasetRecordProject']
    else:
        raise DXError('Invalid record type: %r' % resp['recordTypes'])

    dataset_id = resp['dataset']
    out_directory = ""
    field_file_name = resp['recordName'] + '.txt'
    sql_file_name = resp['recordName'] + '.data.sql'
    print_to_stdout = False
    
    if args.output is None:
        out_directory = os.getcwd()
    elif args.output == '-':
        print_to_stdout = True 
    elif args.dump_dataset_dictionary:
        if os.path.isdir(args.output):
            out_directory = args.output
        else:
            err_exit(fill("Error: {path} is a file. Only directories can be provided with dump-dataset-dictionary".format(path=args.output)))
    else:
        if os.path.exists(args.output):
            if os.path.isdir(args.output):
                out_directory = args.output
            else:
                err_exit(fill("Error: {path} file already exists".format(path=args.output)))
        elif os.path.exists(os.path.dirname(args.output)):
            out_directory = os.path.dirname(args.output)
            field_file_name = os.path.basename(args.output)
            sql_file_name = os.path.basename(args.output)
        elif not os.path.dirname(args.output):
            out_directory = os.getcwd()
            field_file_name = os.path.basename(args.output)
            sql_file_name = os.path.basename(args.output)
        else:
            err_exit(fill("Error: {path} could not be found".format(path=os.path.dirname(args.output))))

    rec_descriptor = DXDataset(dataset_id,project=project).get_descriptor()
    #print(rec_descriptor.__dict__["model"]["entities"]["N_encounters"]["fields"])

    if args.fields is not None:
        fields_list = ''.join(args.fields).split(',')
        error_list = []
        for entry in fields_list:
            if '.' not in entry:
                error_list.append(entry)
            elif entry.split('.')[0] not in rec_descriptor.__dict__["model"]["entities"].keys() or \
               entry.split('.')[1] not in rec_descriptor.__dict__["model"]["entities"][entry.split('.')[0]]["fields"].keys():
               error_list.append(entry)
        
        if error_list:
            raise DXError('Invalid entity.field provided: %r' % error_list)


        payload = {"project_context":project, "fields":[{item:'$'.join(item.split('.'))} for item in fields_list]}
        if "CohortBrowser" in resp['recordTypes']:
            payload['base_sql'] = resp['sql']
            payload['filters'] = resp['filters']
        if args.sql:
            resource_val = resp['url'] + '/viz-query/' + resp['version'] + '/' + resp['dataset'] + '/raw-query'
            resp_raw_query = dxpy.DXHTTPRequest(resource=resource_val, data=payload, prepend_srv=False)
            sql_results = resp_raw_query['sql'] + ';'
            if print_to_stdout:
                print(sql_results)
            else:
                with open(os.path.join(out_directory, sql_file_name), 'w') as f:
                    print(sql_results, file=f)
        else:
            resource_val = resp['url'] + '/data/' + resp['version'] + '/' + resp['dataset'] + '/raw'
            resp_raw = dxpy.DXHTTPRequest(resource=resource_val, data=payload, prepend_srv=False)
            csv_from_json(file_directory=out_directory,file_name=field_file_name, print_to_stdout=print_to_stdout, sep=delimiter, raw_results=resp_raw['results'])

    elif args.sql:
        raise DXError('`--sql` passed without `--fields`')
        
    
    if args.dump_dataset_dictionary:
        rec = DXDataset(dataset_id,project=project)
        rec_dict = rec.get_dictionary()
        write_ot = rec_dict.write(output_path=out_directory, file_name_prefix=resp['recordName'], print_to_stdout=print_to_stdout)
    else:
        pass

def csv_from_json(file_directory="", file_name="", print_to_stdout=False, sep=',', raw_results=[]):
    if print_to_stdout:
        fields_output = sys.stdout
    else:
        out_file = os.path.join(file_directory, file_name)
        fields_output = open(out_file, 'w')
    csv_writer = csv.writer(fields_output, delimiter=sep, doublequote=True, escapechar = None, lineterminator = "\n", 
                            quotechar = '"', quoting = csv.QUOTE_MINIMAL, skipinitialspace = False, strict = False)
    count = 0
    for entry in raw_results:
        if count == 0:
            header = entry.keys()
            csv_writer.writerow(header)
            count += 1

        csv_writer.writerow(entry.values())
    if not print_to_stdout:
        fields_output.close()
    
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
                    # Print function, if and else statements below are for debug purpose. Will be removed before PR.
                    print("ji:", ji)
                    if db_tb_name not in join_path_to_entity_field:
                        print("Skipping edge for : " + db_tb_name)
                        continue
                    else:
                        print("{} present in join_path_to_entity_field. But skip adding corresponding {} \
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
            # Else statement to be removed before raising PR. Printing for debug purpose
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
            "units"
        ]
        dataset_type_to_dxdm_type = {"integer": "integer",
                                     "double": "float",
                                     "date": "date",
                                     "datetime": "datetime",
                                     "string": "string"
        }
        dcols = {col: [] for col in required_columns + extra_cols}
        dcols["entity"] = [entity["name"]] * len(entity["fields"])
        dcols["referenced_entity_field"] = [""] * len(entity["fields"])
        dcols["relationship"] = [""] * len(entity["fields"])

        for field in entity["fields"]:
            # Field-level parameters
            field_dict = entity["fields"][field]
            dcols["name"].append(field_dict["name"])
            dcols["type"].append(dataset_type_to_dxdm_type[field_dict["type"]])
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
            elif field_value["database_name"] and field_value["database_id"] and database_id_regex.match(field_value["database_name"]):
                unique_db_name = "{}__{}".format(field_value["database_id"].replace("-", "_").lower(), field_value["database_name"])
                join_path_to_entity_field[unique_db_name] = (entity["name"], field)
        return join_path_to_entity_field

    def create_edge(self, join_info_joins, join_path_to_entity_field):
        """
        Convert an item join_info to an edge. Returns ordereddict.
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
        """
            Returns CodingDictionary pandas DataFrame for an coding_name.
        """
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

    def write(self, output_path="", file_name_prefix="", print_to_stdout=False, sep=","):
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

        data_dframe = as_dataframe(self.data_dictionary, required_columns = ["entity", "name", "type", "primary_key_type"])
        coding_dframe = as_dataframe(self.coding_dictionary, required_columns=["coding_name", "code", "meaning"])
        entity_dframe = as_dataframe(self.entity_dictionary, required_columns=["entity", "entity_title"])
        
        if print_to_stdout:
            output_file_data = sys.stdout
            output_file_coding = sys.stdout
            output_file_entity = sys.stdout
        else:
            output_file_data = os.path.join(output_path, file_name_prefix + ".data_dictionary.csv")
            output_file_coding = os.path.join(output_path, file_name_prefix + ".codings.csv")
            output_file_entity = os.path.join(output_path, file_name_prefix + ".entity_dictionary.csv")
        
        data_dframe.to_csv(output_file_data, **csv_opts)
        coding_dframe.to_csv(output_file_coding, **csv_opts)
        entity_dframe.to_csv(output_file_entity, **csv_opts)
