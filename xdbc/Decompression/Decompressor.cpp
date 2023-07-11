#include "Decompressor.h"

#include <boost/asio.hpp>
#include "spdlog/spdlog.h"
#include <zstd.h>
#include <snappy.h>
#include <lzo/lzo1x.h>
#include <lz4.h>
#include <zlib.h>

int Decompressor::decompress_zstd(void *dst, const void *src, size_t in_size, int out_size) {
    int ret = 0;
    //TODO: fix first 2 fields for zstd
    //TODO: move decompression context outside of this function and pass it
    ZSTD_DCtx *dctx = ZSTD_createDCtx(); // create a decompression context

    // Get the raw buffer pointer and size
    //const char* compressed_data = boost::asio::buffer_cast<const char*>(in);
    //size_t in_size = boost::asio::buffer_size(in);

    size_t decompressed_max_size = ZSTD_getFrameContentSize(src, in_size);
    size_t decompressed_size = ZSTD_decompressDCtx(dctx, dst, out_size, src, in_size);
    //cout << "decompressed: " << decompressed_size << endl;
    /*uint* int_ptr = static_cast<uint*>(dst);
    int val = *int_ptr;
    cout << "l_orderkey" << val << endl;*/
    // Resize the buffer to the decompressed size
    //buffer = boost::asio::buffer(data, result);

    if (ZSTD_isError(decompressed_size)) {
        spdlog::get("XDBC.CLIENT")->warn("ZSTD decompression error: {0}", ZSTD_getErrorName(decompressed_size));
        ret = 1;
    }

    return ret;
}

int Decompressor::decompress_snappy(void *dst, const void *src, size_t in_size, int out_size) {
    int ret = 0;

    const char *data = reinterpret_cast<const char *>(src);


    // Determine the size of the uncompressed data
    size_t uncompressed_size;
    if (!snappy::GetUncompressedLength(data, in_size, &uncompressed_size)) {
        spdlog::get("XDBC.CLIENT")->error("Snappy: failed to get uncompressed size");
        //throw std::runtime_error("failed to get uncompressed size");
        ret = 1;
    }

    // Decompress the data into the provided destination
    if (!snappy::RawUncompress(data, in_size, static_cast<char *>(dst))) {
        spdlog::get("XDBC.CLIENT")->error("Snappy: failed to decompress data");
        //throw std::runtime_error("Client: Snappy: failed to decompress data");
        ret = 1;
    }

    return ret;
}

int Decompressor::decompress_lzo(void *dst, const void *src, size_t in_size, int out_size) {
    int ret = 0;

    auto *data = reinterpret_cast<const unsigned char *>(src);

    //std::size_t in_size = boost::asio::buffer_size(in);

    // Estimate the worst-case size of the decompressed data
    std::size_t max_uncompressed_size = in_size;

    // Decompress the data
    int result = lzo1x_decompress(data,
                                  in_size,
                                  reinterpret_cast<unsigned char *>(dst),
                                  &max_uncompressed_size,
                                  nullptr
    );

    if (result != LZO_E_OK) {
        spdlog::get("XDBC.CLIENT")->error("lzo: failed to decompress data, error {0}", result);
        // Handle error
        ret = 1;
    }

    if (max_uncompressed_size != out_size) {
        spdlog::get("XDBC.CLIENT")->error("lzo: failed, max_size: {0}, out_size {1}",
                                          max_uncompressed_size, out_size);
        // Handle error
        ret = 1;
    }
    return ret;
}

int Decompressor::decompress_lz4(void *dst, const void *src, size_t in_size, int out_size) {
    int ret = 0;
    auto *data = reinterpret_cast<const char *>(src);

    // Get the size of the uncompressed data
    int uncompressed_size = LZ4_decompress_safe(data, static_cast<char *>(dst),
                                                in_size, out_size);

    if (uncompressed_size < 0) {
        spdlog::get("XDBC.CLIENT")->error("LZ4: Failed to decompress data, uncompressed_size<0");
        ret = 1;
    } else if (uncompressed_size !=
               LZ4_decompress_safe(data, static_cast<char *>(dst), static_cast<int>(in_size),
                                   uncompressed_size)) {
        spdlog::get("XDBC.CLIENT")->error(
                "Failed to decompress LZ4 data: uncompressed size doesn't match expected size");
    }
    return ret;
}

int Decompressor::decompress_zlib(void *dst, const void *src, size_t in_size, int out_size) {
    int ret = 0;
    // Get the underlying data pointer and size from the const_buffer
    const char *compressed_data = static_cast<const char *>(src);

    // Initialize zlib stream
    z_stream stream{};
    stream.next_in = reinterpret_cast<Bytef *>(const_cast<char *>(compressed_data));
    stream.avail_in = static_cast<uInt>(in_size);
    stream.avail_out = out_size;
    stream.next_out = reinterpret_cast<Bytef *>(dst);


    // Initialize zlib for decompression
    if (inflateInit(&stream) != Z_OK) {
        spdlog::get("XDBC.CLIENT")->error("ZLIB: Failed to initialize zlib for decompression ");
        ret = 1;
    }

    // Decompress the data
    int retZ = inflate(&stream, Z_FINISH);
    if (retZ != Z_STREAM_END) {
        spdlog::get("XDBC.CLIENT")->error("ZLIB: Decompression failed with error code: {0}", zError(retZ));
        ret = 1;
    }

    // Clean up zlib resources
    inflateEnd(&stream);
    return ret;
}

