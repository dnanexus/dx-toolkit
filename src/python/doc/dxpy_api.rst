:mod:`dxpy.api` Module
----------------------

This module is automatically generated from a list of available routes on the
API server. Functions in this module take a remote object ID (when appropriate)
and an optional argument *input_params* to set the request body. The request
must be a list or a dict, as it will be converted to JSON. If *input_params* is
not provided, the JSON of an empty dict will be sent. Each function returns the
Pythonized JSON (a list or dict) that is returned from the API server.

.. py:currentmodule:: dxpy.api

.. describe:: An example API call

      .. function:: classname_methodname(object_id, input_params={}, always_retry=False, **kwargs)

         :param object_id: Object ID of remote object to be manipulated
	 :type object_id: string
	 :param input_params: Request body (will be converted to JSON)
	 :type input_params: list or dict
	 :param always_retry: True if the request is idempotent and is safe to
                              retry. Note that this parameter is misleadingly
                              named; setting it to True (False, respectively)
                              does not guarantee that the request will always
                              (never) be retried. Rather, it is advisory and
                              whether the request is retried depends on the
                              specific error condition: see `the retry logic
                              specification
                              <https://github.com/dnanexus/dx-toolkit/blob/master/src/api_wrappers/README.md#http-retry-logic>`_
                              for more information.
	 :type always_retry: bool
	 :param kwargs: Additional arguments to be passed to the :class:`dxpy.DXHTTPRequest` object (such as headers)
	 :returns: Contents of response from API server (Pythonized JSON object)
	 :rtype: list or dict
	 :raises: :exc:`~dxpy.exceptions.DXAPIError` if an HTTP response code other than 200 is received from the API server.

For apps, the signature is slightly different, because apps can also be named
by their name and version (or tag), in addition to their app ID.

.. describe:: An example API call on an app instance

     .. function:: app_methodname(app_name_or_id, alias=None, input_params={}, always_retry=False, **kwargs)

         :param app_name_or_id: Either "app-NAME" or the hash ID "app-xxxx"
         :type app_name_or_id: string
         :param alias: If *app_name_or_id* is given using its name, then a version or tag string (if none is given, then the tag "default" will be used).  If *app_name_or_id* is a hash ID, this value should be :const:`None`.
         :type alias: string
         :param input_params: Request body (will be converted to JSON)
	 :type input_params: list or dict
	 :param always_retry: True if the request is idempotent and is safe to
                              retry. Note that this parameter is misleadingly
                              named; setting it to True (False, respectively)
                              does not guarantee that the request will always
                              (never) be retried. Rather, it is advisory and
                              whether the request is retried depends on the
                              specific error condition: see `the retry logic
                              specification
                              <https://github.com/dnanexus/dx-toolkit/blob/master/src/api_wrappers/README.md#http-retry-logic>`_
                              for more information.
	 :type always_retry: bool
	 :param kwargs: Additional arguments to be passed to the :class:`dxpy.DXHTTPRequest` object (such as headers)
	 :returns: Contents of response from API server (Pythonized JSON object)
	 :rtype: list or dict
	 :raises: :exc:`~dxpy.exceptions.DXAPIError` if an HTTP response code other than 200 is received from the API server.

The specific functions provided in this module are enumerated below.

.. automodule:: dxpy.api
   :members:
   :undoc-members:
   :show-inheritance:
