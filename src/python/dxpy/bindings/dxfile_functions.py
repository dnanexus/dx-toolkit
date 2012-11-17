'''
Helper Functions
****************

The following helper functions are useful shortcuts for interacting with File objects.

'''

import os, math, mmap
from dxpy.bindings import *
from math import floor

def open_dxfile(dxid, project=None, read_buffer_size=DEFAULT_BUFFER_SIZE):
    '''
    :param dxid: file ID
    :type dxid: string
    :rtype: :class:`~dxpy.bindings.dxfile.DXFile`

    Given the object ID of an uploaded file, returns a remote file
    handler that is a Python file-like object.

    Example::

      with open_dxfile("file-xxxx") as fd:
          for line in fd:
              ...

    Note that this is shorthand for::

      DXFile(dxid)

    '''
    return DXFile(dxid, project=project, read_buffer_size=read_buffer_size)

def new_dxfile(keep_open=None, mode=None, write_buffer_size=DEFAULT_BUFFER_SIZE, **kwargs):
    '''
    :param keep_open: Deprecated. Use the *mode* parameter instead.
    :type keep_open: boolean
    :param mode: One of "w" or "a" for write and append modes, respectively
    :type mode: string
    :rtype: :class:`~dxpy.bindings.dxfile.DXFile`

    Additional optional parameters not listed: all those under
    :func:`dxpy.bindings.DXDataObject.new`.

    Creates a new remote file object that is ready to be written to;
    returns a :class:`~dxpy.bindings.dxfile.DXFile` object that is a
    writable file-like object.

    Example::

        with new_dxfile(media_type="application/json") as fd:
            fd.write("foo\\n")

    Note that this is shorthand for::

        dxFile = DXFile()
        dxFile.new(**kwargs)

    '''
    dx_file = DXFile(keep_open=keep_open, mode=mode, write_buffer_size=write_buffer_size)
    dx_file.new(**kwargs)
    return dx_file

def download_dxfile(dxid, filename, chunksize=DEFAULT_BUFFER_SIZE, append=False, show_progress=False,
                    **kwargs):
    '''
    :param dxid: Remote file ID
    :type dxid: string
    :param filename: Local filename
    :type filename: string
    :param append: If True, appends to the local file (default is to truncate local file if it exists)
    :type append: boolean

    Downloads the remote file with object ID *dxid* and saves it to
    *filename*.

    Example::

        download_dxfile("file-xxxx", "localfilename.fastq")

    '''

    def print_progress(bytes_downloaded, file_size):
        num_ticks = 60

        effective_file_size = file_size or 1
        if bytes_downloaded > effective_file_size:
            effective_file_size = bytes_downloaded

        ticks = int(round((bytes_downloaded / float(effective_file_size)) * num_ticks))
        percent = int(round((bytes_downloaded / float(effective_file_size)) * 100))

        fmt = "[{0}{1}] Downloaded {2}{3} bytes ({4}%)"
        sys.stderr.write(fmt.format((('=' * (ticks - 1) + '>') if ticks > 0 else ''), ' ' * (num_ticks - ticks), bytes_downloaded, " of %d" % (file_size,) if file_size else "", percent))
        sys.stderr.flush()
        sys.stderr.write("\r")
        sys.stderr.flush()

    file_size = None
    bytes = 0

    mode = 'ab' if append else 'wb'
    with DXFile(dxid, read_buffer_size=chunksize) as dxfile, open(filename, mode) as fd:
        if show_progress:
            print_progress(0, None)
        while True:
            file_content = dxfile.read(chunksize, **kwargs)
            if file_size is None:
                file_size = dxfile._file_length

            if show_progress:
                bytes += len(file_content)
                print_progress(bytes, file_size)

            if len(file_content) == 0:
                if show_progress:
                    sys.stderr.write("\n")
                break

            fd.write(file_content)


def upload_local_file(filename=None, file=None, media_type=None, keep_open=False,
                      wait_on_close=False, use_existing_dxfile=None, show_progress=False, **kwargs):
    '''
    :param filename: Local filename
    :type filename: string
    :param file: File-like object
    :type file: File-like object
    :param media_type: Internet Media Type
    :type media_type: string
    :param keep_open: If False, closes the file after uploading
    :type keep_open: boolean
    :param wait_on_close: If True, waits for the file to close
    :type wait_on_close: boolean
    :param use_existing_dxfile: Instead of creating a new file object, upload to the specified file
    :type use_existing_dxfile: :class:`~dxpy.bindings.dxfile.DXFile`
    :returns: Remote file handler
    :rtype: :class:`~dxpy.bindings.dxfile.DXFile`

    Additional optional parameters not listed: all those under
    :func:`dxpy.bindings.DXDataObject.new`.

    Exactly one of *filename* or *file* is required.

    Uploads *filename* or reads from *file* into a new file object (with
    media type *media_type* if given) and returns the associated remote
    file handler. The "name" property of the newly created remote file
    is set to the basename of *filename* or to *file.name* (if it
    exists).

    Examples::

      # Upload from a path
      dxpy.upload_local_file("/home/ubuntu/reads.fastq.gz")
      # Upload from a file-like object
      with open("reads.fastq") as fh:
          dxpy.upload_local_file(file=fh)

    '''
    fd = file if filename is None else open(filename, 'rb')

    try:
        file_size = os.fstat(fd.fileno()).st_size
    except:
        file_size = 0
    # Raise buffer size (for files exceeding DEFAULT_BUFFER_SIZE * 10k
    # bytes) in order to prevent us from exceeding 10k parts limit.
    min_buffer_size = file_size / 10000 + 1
    if file_size >= 0 and hasattr(fd, "fileno"):
        # For mmap'd uploads the buffer size additionally must be a
        # multiple of the ALLOCATIONGRANULARITY.
        min_buffer_size = int(math.ceil(min_buffer_size / mmap.ALLOCATIONGRANULARITY)) * mmap.ALLOCATIONGRANULARITY
    buffer_size = max(DEFAULT_BUFFER_SIZE, min_buffer_size)

    if use_existing_dxfile:
        dxfile = use_existing_dxfile
    else:
        # Use 'a' mode because we will be responsible for closing the file
        # ourselves later (if requested).
        dxfile = new_dxfile(mode='a', media_type=media_type, write_buffer_size=buffer_size, **kwargs)

    creation_kwargs, remaining_kwargs = dxpy.DXDataObject._get_creation_params(kwargs)

    num_ticks = 60
    offset = 0

    def read(num_bytes):
        """
        Returns a string or mmap'd data containing the next num_bytes of
        the file, or up to the end if there are fewer than num_bytes
        left.
        """
        # In order to do the mmap, fd has to point to a real file that
        # has a fileno and a known size. If this is not the case, fall
        # back to doing an actual read from the file.
        if file_size == 0 or not hasattr(fd, "fileno"):
            return fd.read(dxfile._write_bufsize)

        bytes_available = max(file_size - offset, 0)
        if bytes_available == 0:
            return ""

        return mmap.mmap(fd.fileno(), min(dxfile._write_bufsize, bytes_available), offset=offset, access=mmap.ACCESS_READ)

    while True:
        buf = read(dxfile._write_bufsize)
        offset += len(buf)

        if len(buf) == 0:
            if show_progress:
                sys.stderr.write("\n")
            break

        dxfile.write(buf, **remaining_kwargs)

        if show_progress:
            if file_size > 0:
                ticks = int(round((offset / float(file_size)) * num_ticks))
                percent = int(round((offset / float(file_size)) * 100))

                fmt = "[{0}{1}] Uploaded ({2} of {3} bytes) {4}%"
                sys.stderr.write(fmt.format((('=' * (ticks - 1) + '>') if ticks > 0 else ''), ' ' * (num_ticks - ticks), offset, file_size, percent))
                sys.stderr.flush()
                sys.stderr.write("\r")
                sys.stderr.flush()

    if filename is not None:
        fd.close()

    if not keep_open:
        dxfile.close(block=wait_on_close, **remaining_kwargs)

    if 'name' in kwargs or use_existing_dxfile:
        pass # File has already been named
    elif filename is not None:
        dxfile.rename(os.path.basename(filename), **remaining_kwargs)
    else:
        # Try to get filename from file-like object
        try:
            dxfile.rename(os.path.basename(file.name), **remaining_kwargs)
        except AttributeError:
            pass

    return dxfile

def upload_string(to_upload, media_type=None, keep_open=None, mode=None,
                  wait_on_close=False, **kwargs):
    """
    :param to_upload: String to upload into a file
    :type to_upload: string
    :param media_type: Internet Media Type
    :type media_type: string
    :param keep_open: If False, closes the file after uploading
    :type keep_open: boolean
    :param wait_on_close: If True, waits for the file to close
    :type wait_on_close: boolean
    :returns: Remote file handler
    :rtype: :class:`~dxpy.bindings.dxfile.DXFile`

    Additional optional parameters not listed: all those under
    :func:`dxpy.bindings.DXDataObject.new`.

    Uploads the data in the string *to_upload* into a new file object
    (with media type *media_type* if given) and returns the associated
    remote file handler.

    """

    dxfile = new_dxfile(media_type=media_type, keep_open=keep_open, mode=mode, **kwargs)

    creation_kwargs, remaining_kwargs = dxpy.DXDataObject._get_creation_params(kwargs)

    dxfile.write(to_upload, **remaining_kwargs)
    dxfile.close(block=wait_on_close, **remaining_kwargs)
    return dxfile
