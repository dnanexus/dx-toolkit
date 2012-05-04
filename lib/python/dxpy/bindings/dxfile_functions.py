'''
Helper Functions
****************

These two functions provide functionality for opening an existing
remote file (read-only) and creating a new remote file (write-only).
Both return a remote file handler that can be treated as a file
descriptor.  These two functions are essentially useful aliases for
executing simple download and upload operations between the local and
the remote file systems.
'''

import os
from dxpy.bindings import *

def open_dxfile(dxid, project=None, buffer_size=1024*1024*100):
    '''
    :param dxid: file ID
    :type dxid: string
    :rtype: :class:`dxpy.bindings.dxfile.DXFile`

    Given the object ID of an uploaded file, this function returns a
    remote file handler which can be treated as a read-only file
    descriptor.

    Example::

        with open_dxfile("file-xxxx") as fd:
            for line in fd:
                ...
                
    Note that this is shorthand for::

        DXFile(dxid)

    '''
    return DXFile(dxid, project=project, buffer_size=buffer_size)

def new_dxfile(keep_open=False, buffer_size=1024*1024*100, **kwargs):
    '''
    :param media_type: Internet Media Type (optional)
    :type media_type: string
    :rtype: :class:`dxpy.bindings.dxfile.DXFile`

    Additional optional parameters not listed: all those under
    :func:`dxpy.bindings.DXDataObject.new`.

    Creates a new remote file object that is ready to be written to
    and returns a DXFile object which can be treated as a write-only
    file descriptor.  Other optional parameters available (see
    :func:`dxpy.bindings.DXDataObject.new()`).

    Example::

        with new_dxfile(media_type="application/json") as fd:
            fd.write("foo\\n")

    Note that this is shorthand for::

        dxFile = DXFile()
        dxFile.new(**kwargs)

    '''
    
    dx_file = DXFile(keep_open=keep_open, buffer_size=buffer_size)
    dx_file.new(**kwargs)
    return dx_file

def download_dxfile(dxid, filename, chunksize=1024*1024*100, append=False,
                    **kwargs):
    '''
    :param dxid: Object ID of a file
    :type dxid: string
    :param filename: Local filename
    :type filename: string
    :param append: Set to true if the local filename is to be appended to
    :type append: boolean

    Downloads the remote file with object ID *dxid* and saves it to
    *filename*.

    Example::

        download_dxfile("file-xxxx", "localfilename.fastq")

    '''

    mode = 'ab' if append else 'wb'
    with DXFile(dxid) as dxfile:
        with open(filename, mode) as fd:
            while True:
                file_content = dxfile.read(chunksize, **kwargs)
                if len(file_content) == 0:
                    break
                fd.write(file_content)

def upload_local_file(filename, media_type=None, keep_open=False,
                      buffer_size=1024*1024*100, wait_on_close=False, **kwargs):
    '''
    :param filename: Local filename
    :type filename: string
    :param media_type: Internet Media Type
    :type media_type: string
    :returns: Remote file handler
    :rtype: :class:`dxpy.bindings.dxfile.DXFile`

    Additional optional parameters not listed: all those under
    :func:`dxpy.bindings.DXDataObject.new`.

    Uploads *filename* into a new file object (with media type
    *media_type* if given) and returns the associated remote file
    handler.  In addition, it will set the "name" property of the
    remote file to *filename*.

    TODO: Do I want an optional argument to indicate in what size
    chunks the file should be uploaded or in how many pieces?

    '''

    dxfile = new_dxfile(keep_open=keep_open, buffer_size=buffer_size, media_type=media_type, **kwargs)

    creation_kwargs, remaining_kwargs = dxpy.DXDataObject._get_creation_params(kwargs)

    with open(filename, 'rb') as fd:
        while True:
            buf = fd.read(dxfile._bufsize)
            if len(buf) == 0:
                break
            dxfile.write(buf, **remaining_kwargs)

    if not keep_open:
        dxfile.close(block=wait_on_close, **remaining_kwargs)
    dxfile.rename(os.path.basename(filename), **remaining_kwargs)
    return dxfile

def upload_string(to_upload, media_type=None, keep_open=False,
                  buffer_size=1024*1024*100, wait_on_close=False, **kwargs):
    """
    :param to_upload: String to upload into a file
    :type to_upload: string
    :param media_type: Internet Media Type
    :type media_type: string
    :returns: Remote file handler
    :rtype: :class:`dxpy.bindings.dxfile.DXFile`

    Additional optional parameters not listed: all those under
    :func:`dxpy.bindings.DXDataObject.new`.

    Uploads the given string *to_upload* into a new file object (with
    media type *media_type* if given) and returns the associated
    remote file handler.
    
    """

    dxfile = new_dxfile(media_type=media_type, keep_open=keep_open,
                        buffer_size=buffer_size, **kwargs)

    creation_kwargs, remaining_kwargs = dxpy.DXDataObject._get_creation_params(kwargs)

    dxfile.write(to_upload, **remaining_kwargs)
    dxfile.close(block=wait_on_close, **remaining_kwargs)
    return dxfile
