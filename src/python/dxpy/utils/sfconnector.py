from abc import ABC

import snowflake.connector
import dxpy
import json
import sys
import os
from dxpy.exceptions import DXError


# Snowflake data connector. This is a wrapper around Snowflake Python Connector provided by Snowflake.
class DXSFConnector(ABC):
    # Default configuration
    default_config = {
        'user': "dummy_user",
        'password': "dummy_passwd",
        'account': "dummy_account",
        'warehouse': "dummy_warehouse",
        'role': "default",
        'host': "{}".format(dxpy.APISERVER_HOST),
        'port': "{}".format(dxpy.APISERVER_PORT),
        'protocol': dxpy.APISERVER_PROTOCOL
    }

    def __init__(self, config=None):
        self._ctx = None
        self.config = config or self.default_config

    # Establish connection with Snowflake
    def connect(self):
        """Use connection config and snowflake.connector provided
        by Snowflake to establish connection with snowflake.
        """
        try:
            self._ctx = snowflake.connector.connect(**self.config)
        except Exception as e:
            raise SFConnectionError(e)

    def cursor(self):
        """Return a SnowflakeCursor object
        """
        return self._ctx.cursor()

    def close(self):
        """Close the connection
        """
        if self._ctx:
            self._ctx.close()

    @staticmethod
    def createDatabase(database_name, project_id):
        """
        Utilty method to create Snowflake Database. Allowed only from a job context.
        Args:
            database_name (str): String representing a database name.
            project_id (str): String representing a project ID.
        
        Returns: 
            database_id
        """
        if os.environ.get("DX_JOB_ID") is None:
            print('Unable to create database : Creation of Snowflake backed database is available only from a job context')
            sys.exit(1)

        resp = None
        try:
            data = json.dumps({'databaseName': database_name, 'project': project_id, 'databaseType' : 'dnaxsf'})
            resp = dxpy.DXHTTPRequest('/database/newDnaxsf', data=data, jsonify_data=False)
        except Exception as e:
            print('Unable to create Snowflake backed database')
            sys.exit(1)
        return resp['id']
        
        	
    @staticmethod
    def dropDatabase(database_name, project_id):
        """
        Args:
            database_name (str): String representing a database name
            project_id (str): String representing a project ID.
        """
        try:
            data = json.dumps({'databaseName': database_name, 'databaseType' : 'dnaxsf', 'scope': {'project': project_id}})
            resp = dxpy.DXHTTPRequest('/system/findDatabases', data=data, jsonify_data=False)
            results = resp['results']
            if results and len(results) == 1:
                db_id = results[0]['id']
                remove_data = json.dumps({'objects': [db_id]})
                dxpy.DXHTTPRequest('/{}/removeObjects'.format(project_id), data=remove_data, jsonify_data=False)
            else:
                if len(results) > 1:
                    print('Error : More than one databases found')
                else:
                    print('Error : No database found')
                sys.exit(1)

        except Exception as e:
            print('Unable to drop Snowflake backed database')
            sys.exit(1)
        

class SFConnectionError(DXError):
    ''' Raised when unable to establish Snowflake connection'''

    def __init__(self, error):
        self.error = error

    def error_message(self):
        return "Snowflake Connection Error " + self.error.msg

    def __str__(self):
        return self.error_message()
