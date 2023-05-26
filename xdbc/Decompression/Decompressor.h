#ifndef XDBC_DECOMPRESSOR_H
#define XDBC_DECOMPRESSOR_H

#include <boost/asio.hpp>

#define BUFFER_SIZE 1000
#define BUFFERPOOL_SIZE 1000
#define TUPLE_SIZE 48
#define SLEEP_TIME 10ms

class Decompressor {
public:
    static int decompress_zstd(void *dst, const boost::asio::const_buffer &compressed_buffer, size_t compressed_size);

    static int decompress_snappy(void *dst, const boost::asio::const_buffer &compressed_buffer, size_t compressed_size);

    static int decompress_lzo(void *dst, const boost::asio::const_buffer &compressed_buffer, size_t compressed_size);

    static int decompress_lz4(void *dst, const boost::asio::const_buffer &compressed_buffer, size_t compressed_size);

    static int decompress_zlib(void *dst, const boost::asio::const_buffer &compressed_buffer, size_t compressed_size);
};

#endif //XDBC_DECOMPRESSOR_H
