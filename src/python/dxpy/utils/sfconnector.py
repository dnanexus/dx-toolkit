from abc import ABC

import snowflake.connector
import dxpy
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
        'host': "{}/SF-ROUTES/".format(dxpy.APISERVER_HOST),
        'port': dxpy.APISERVER_PORT,
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
            raise InvalidSFAuthentication(e)

    def cursor(self):
        """Return a SnowflakeCursor object
        """
        return self._ctx.cursor()

    def close(self):
        """Close the connection
        """
        if self._ctx:
            self._ctx.close()


class InvalidSFAuthentication(DXError):
    ''' Raised when invalid snowflake credentials used for Snowflake connection. '''

    def __init__(self, error):
        self.error = error

    def error_message(self):
        return "Snowflake Authentication Error " + self.error.msg

    def __str__(self):
        return self.error_message()