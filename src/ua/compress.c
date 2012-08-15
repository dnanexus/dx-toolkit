/*
 * This is a modified version of the compress2 function provided by zlib.
 * compress2 is a wrapper around the stream-based deflate functions that
 * compresses the contents of a source buffer into a preallocated
 * destination buffer in one go. This version change the initialization
 * parameters so that the resulting compressed data is a valid gzip stream.
 */

#include "compress.h"

/*
 * TODO: Is the size returned by compressBound enough for any additional
 * data that needs to be written to make a valid gzip stream?
 */

/* ===========================================================================
   Compresses the source buffer into the destination buffer. The level
   parameter has the same meaning as in deflateInit.  sourceLen is the byte
   length of the source buffer. Upon entry, destLen is the total size of the
   destination buffer, which must be at least 0.1% larger than sourceLen plus
   12 bytes. Upon exit, destLen is the actual size of the compressed buffer.

   compress2 returns Z_OK if success, Z_MEM_ERROR if there was not enough
   memory, Z_BUF_ERROR if there was not enough room in the output buffer,
   Z_STREAM_ERROR if the level parameter is invalid.
*/
int gzCompress(Bytef * dest, uLongf * destLen, const Bytef * source, uLong sourceLen, int level) {
  z_stream stream;
  int err;

  stream.next_in = (Bytef *) source;
  stream.avail_in = (uInt) sourceLen;
  stream.next_out = dest;
  stream.avail_out = (uInt) (*destLen);

  if ((uLong) stream.avail_out != *destLen) return Z_BUF_ERROR;

  stream.zalloc = (alloc_func) 0;
  stream.zfree = (free_func) 0;
  stream.opaque = (voidpf) 0;

  /* err = deflateInit(&stream, level); */
  /* This is the major change. */
  err = deflateInit2(&stream,
                     level,  // compression level (0, ..., 9; default is 6 or Z_DEFAULT_COMPRESSION)
                     Z_DEFLATED,  // the default (and only) compression method
                     31,  // windowBits is 15, plus 16 to write gzip header and trailer
                     8,  // how much memory for internal compression state (1, ..., 9; 8 is default)
                     Z_DEFAULT_STRATEGY);  // default compression algorithm
  if (err != Z_OK) return err;

  err = deflate(&stream, Z_FINISH);
  if (err != Z_STREAM_END) {
    deflateEnd(&stream);
    return err == Z_OK ? Z_BUF_ERROR : err;
  }
  *destLen = stream.total_out;

  err = deflateEnd(&stream);
  return err;
}

/*
 * Returns the usual compressBound, plus some extra padding for the gzip
 * header and trailer.
 *
 * My guess at the size of the gz_header struct in zlib.h is 68 bytes, so I
 * just doubled that for good measure.
 */
uLong gzCompressBound(uLong sourceLen) {
  return compressBound(sourceLen) + 136;
}
