#include "Decompressor.h"

#include <boost/asio.hpp>
#include "spdlog/spdlog.h"
#include <zstd.h>
#include <snappy.h>
#include <lzo/lzo1x.h>
#include <lz4.h>
#include <zlib.h>

int
Decompressor::decompress_zstd(void *dst, const boost::asio::const_buffer &compressed_buffer, size_t compressed_size) {
    int ret = 0;
    //TODO: fix first 2 fields for zstd
    //TODO: move decompression context outside of this function and pass it
    ZSTD_DCtx *dctx = ZSTD_createDCtx(); // create a decompression context


    // Get the raw buffer pointer and size
    //const char* compressed_data = boost::asio::buffer_cast<const char*>(compressed_buffer);
    //size_t compressed_size = boost::asio::buffer_size(compressed_buffer);

    size_t decompressed_max_size = ZSTD_getFrameContentSize(compressed_buffer.data(), compressed_size);
    size_t decompressed_size = ZSTD_decompressDCtx(dctx, dst, TUPLE_SIZE * BUFFER_SIZE,
                                                   compressed_buffer.data(), compressed_size);
    //cout << "decompressed: " << decompressed_size << endl;
    /*uint* int_ptr = static_cast<uint*>(dst);
    int val = *int_ptr;
    cout << "l_orderkey" << val << endl;*/
    // Resize the buffer to the decompressed size
    //buffer = boost::asio::buffer(data, result);

    if (ZSTD_isError(decompressed_size))
        spdlog::get("XDBC.CLIENT")->warn("ZSTD decompression error: {0}", ZSTD_getErrorName(decompressed_size));

    return ret;
}

int
Decompressor::decompress_snappy(void *dst, const boost::asio::const_buffer &compressed_buffer, size_t compressed_size) {
    int ret = 0;

    const char *data = boost::asio::buffer_cast<const char *>(compressed_buffer);
    size_t size = boost::asio::buffer_size(compressed_buffer);

    // Determine the size of the uncompressed data
    size_t uncompressed_size;
    if (!snappy::GetUncompressedLength(data, size, &uncompressed_size)) {
        spdlog::get("XDBC.CLIENT")->warn("Snappy: failed to get uncompressed size");
        //throw std::runtime_error("failed to get uncompressed size");
        ret = 1;
    }

    // Decompress the data into the provided destination
    if (!snappy::RawUncompress(data, size, static_cast<char *>(dst))) {
        spdlog::get("XDBC.CLIENT")->warn("Snappy: failed to decompress data");
        //throw std::runtime_error("Client: Snappy: failed to decompress data");
        ret = 1;
    }

    return ret;
}

int
Decompressor::decompress_lzo(void *dst, const boost::asio::const_buffer &compressed_buffer, size_t compressed_size) {
    int ret = 0;
    //std::size_t compressed_size = boost::asio::buffer_size(compressed_buffer);

    // Estimate the worst-case size of the decompressed data
    std::size_t max_uncompressed_size = compressed_size;

    // Decompress the data
    int result = lzo1x_decompress(
            reinterpret_cast<const unsigned char *>(boost::asio::buffer_cast<const char *>(compressed_buffer)),
            compressed_size,
            reinterpret_cast<unsigned char *>(dst),
            &max_uncompressed_size,
            nullptr
    );

    if (result != LZO_E_OK) {
        // Handle error
    }

    if (max_uncompressed_size != BUFFER_SIZE * TUPLE_SIZE) {
        // Handle error: the actual size of the decompressed data does not match the expected size
    }
    return ret;
}

int
Decompressor::decompress_lz4(void *dst, const boost::asio::const_buffer &compressed_buffer, size_t compressed_size) {
    int ret = 0;

    const char *compressed_data = boost::asio::buffer_cast<const char *>(compressed_buffer);

    // Get the size of the uncompressed data
    int uncompressed_size = LZ4_decompress_safe(compressed_data, static_cast<char *>(dst),
                                                compressed_size, BUFFER_SIZE * TUPLE_SIZE);

    if (uncompressed_size < 0) {
        //cout << "Client: Compressed data size: " << compressed_size << endl;
        spdlog::get("XDBC.CLIENT")->warn("LZ4: Failed to decompress data");
        //throw std::runtime_error("Client: LZ4: Failed to decompress data");
        ret = 1;
    } else if (uncompressed_size !=
               LZ4_decompress_safe(compressed_data, static_cast<char *>(dst), static_cast<int>(compressed_size),
                                   uncompressed_size)) {
        throw std::runtime_error("Failed to decompress LZ4 data: uncompressed size doesn't match expected size");
    }
    return ret;
}

int
Decompressor::decompress_zlib(void *dst, const boost::asio::const_buffer &compressed_buffer,
                              size_t compressed_size) {
    int ret = 0;
    // Get the underlying data pointer and size from the const_buffer
    const char *compressed_data = static_cast<const char *>(compressed_buffer.data());

    // Initialize zlib stream
    z_stream stream{};
    stream.next_in = reinterpret_cast<Bytef *>(const_cast<char *>(compressed_data));
    stream.avail_in = static_cast<uInt>(compressed_size);
    stream.avail_out = BUFFER_SIZE * TUPLE_SIZE;
    stream.next_out = reinterpret_cast<Bytef *>(dst);


    // Initialize zlib for decompression
    if (inflateInit(&stream) != Z_OK) {
        spdlog::get("XDBC.CLIENT")->warn("ZLIB: Failed to initialize zlib for decompression ");
        ret = 1;
    }

    // Decompress the data
    int retZ = inflate(&stream, Z_FINISH);
    if (retZ != Z_STREAM_END) {
        spdlog::get("XDBC.CLIENT")->warn("ZLIB: Decompression failed with error code: {0}", zError(retZ));
        ret = 1;
    }

    // Clean up zlib resources
    inflateEnd(&stream);
    return ret;
}

