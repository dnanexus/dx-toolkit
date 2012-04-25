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

def open_dxfile(dxid, **kwargs):
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

    return DXFile(dxid, **kwargs)

def new_dxfile(**kwargs):
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
    
    dx_file = DXFile()
    dx_file.new(**kwargs)
    return dx_file

# TODO:
# read seeking
# chunk sizing options
# waitonclose

def download_dxfile(dxid, filename, chunksize=1024*1024):
    '''
    :param dxid: Object ID of a file
    :type dxid: string
    :param filename: Local filename
    :type filename: string

    Downloads the remote file with object ID *dxid* and saves it to
    *filename*.

    Example::

        download_dxfile("file-xxxx", "localfilename.fastq")

    '''

    with DXFile(dxid) as dxfile:
        file_content = dxfile.read(chunksize)
        with open(filename, 'w') as fd:
            fd.write(file_content)

def upload_local_file(filename, media_type=None, wait_on_close=False, **kwargs):
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

    dxfile = new_dxfile(media_type=media_type, **kwargs)

    with open(filename, 'r') as fd:
        while True:
            buf = fd.read(dxfile._bufsize)
            if len(buf) == 0:
                break
            dxfile.write(buf)

    dxfile.close(block=wait_on_close)
    dxfile.set_properties({"name": os.path.basename(filename)})
    return dxfile

def upload_string(to_upload, media_type=None, wait_on_close=False, **kwargs):
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

    dxfile = new_dxfile(media_type=media_type, **kwargs)
    dxfile.write(to_upload)
    dxfile.close(block=wait_on_close)
    return dxfile
