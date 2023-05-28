#ifndef XDBC_DECOMPRESSOR_H
#define XDBC_DECOMPRESSOR_H

#include <boost/asio.hpp>

class Decompressor {
public:
    static int decompress_zstd(void *dst, const boost::asio::const_buffer &in, size_t in_size, int out_size);

    static int decompress_snappy(void *dst, const boost::asio::const_buffer &in, size_t in_size, int out_size);

    static int decompress_lzo(void *dst, const boost::asio::const_buffer &in, size_t in_size, int out_size);

    static int decompress_lz4(void *dst, const boost::asio::const_buffer &in, size_t in_size, int out_size);

    static int decompress_zlib(void *dst, const boost::asio::const_buffer &in, size_t in_size, int out_size);
};

#endif //XDBC_DECOMPRESSOR_H
