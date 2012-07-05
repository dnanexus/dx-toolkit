"""
This module is automatically generated from the list of available
routes on the API server.  Functions in this module will take a remote
object ID (when appropriate) and an optional argument to set the
request body.  The request must be a list or a dict, as it will be
converted to JSON.  If it is not given, the JSON of an empty dict will
be sent.  Each function will return the Pythonized JSON (a list or
dict) that is the output from the API server.

.. describe:: An example API call

      .. function:: apiCall(object_id, input_params={}, **kwargs)

         :param object_id: Object ID of remote object to be manipulated
	 :type object_id: string
	 :param input_params: Request body that will be converted to JSON
	 :type input_params: list or dict
	 :param kwargs: Additional arguments will be passed to the HTTP request (such as headers)
	 :returns: Contents of response from API server, converted from JSON
	 :rtype: list or dict
	 :raises: :exc:`dxpy.exceptions.DXAPIError` if an HTTP response code other than 200 is received from the API server.

The specific functions provided in this module are as follows.
"""
