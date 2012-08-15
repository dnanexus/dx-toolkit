#ifndef UA_COMPRESS_H
#define UA_COMPRESS_H

#include <zlib.h>

uLong gzCompressBound(uLong sourceLen);
int gzCompress(Bytef * dest, uLongf * destLen, const Bytef * source, uLong sourceLen, int level);

#endif
