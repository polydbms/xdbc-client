//
// Created by harry on 2/6/23.
//

#include "xclient.h"
#include <iostream>
#include <boost/asio.hpp>
#include <boost/asio/deadline_timer.hpp>
#include <thread>
#include <chrono>
#include <algorithm>
#include <iterator>
#include <zstd.h>
#include <snappy.h>
#include <lzo/lzo1x.h>
#include <lz4.h>

using namespace std;

namespace xdbc {

    int decompress(int method, void *dst, const boost::asio::const_buffer &compressed_buffer, size_t compressed_size) {

        int ret = 0;

        //1 zstd
        //2 snappy
        //3 lzo
        //4 lz4

        //TODO: fix first 2 fields for zstd
        if (method == 1) {
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

            if (ZSTD_isError(decompressed_size)) {
                std::cerr << "ZSTD decompression error: " << ZSTD_getErrorName(decompressed_size) << std::endl;
            }
        }
        if (method == 2) {
            const char *data = boost::asio::buffer_cast<const char *>(compressed_buffer);
            size_t size = boost::asio::buffer_size(compressed_buffer);

            // Determine the size of the uncompressed data
            size_t uncompressed_size;
            if (!snappy::GetUncompressedLength(data, size, &uncompressed_size)) {
                //throw std::runtime_error("failed to get uncompressed size");
                cout << "Client: Snappy: failed to get uncompressed size" << endl;
                ret = 1;
            }

            // Decompress the data into the provided destination
            if (!snappy::RawUncompress(data, size, static_cast<char *>(dst))) {
                //throw std::runtime_error("Client: Snappy: failed to decompress data");
                cout << "Client: Snappy: failed to decompress data" << endl;
                ret = 1;
            }


        }
        if (method == 3) {
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
        }
        if (method == 4) {
            const char *compressed_data = boost::asio::buffer_cast<const char *>(compressed_buffer);

            // Get the size of the uncompressed data
            int uncompressed_size = LZ4_decompress_safe(compressed_data, static_cast<char *>(dst),
                                                        compressed_size, BUFFER_SIZE * TUPLE_SIZE);

            if (uncompressed_size < 0) {
                //cout << "Client: Compressed data size: " << compressed_size << endl;
                cout << "Client: LZ4: Failed to decompress data" << endl;
                //throw std::runtime_error("Client: LZ4: Failed to decompress data");
                ret = 1;
            } else if (uncompressed_size !=
                       LZ4_decompress_safe(compressed_data, static_cast<char *>(dst), static_cast<int>(compressed_size),
                                           uncompressed_size)) {
                throw std::runtime_error(
                        "Failed to decompress LZ4 data: uncompressed size doesn't match expected size");
            }
        }
        return ret;
    }


    void XClient::finalize() {
        cout << "Finalizing XClient: " << _name << endl;
        _finishedReading = true;
    }

    XClient::XClient(std::string name) : _name(name), _bufferPool(), _flagArray(), _finishedTransfer(false),
                                         _startedTransfer(false), _finishedReading(false), _readState(0),
                                         _totalBuffersRead(0) {

        cout << _name << endl;

        cout << "BUFFERPOOL SIZE: " << BUFFERPOOL_SIZE << endl;
        _bufferPool.resize(BUFFERPOOL_SIZE);

        for (auto &i: _flagArray) {
            i = 1;

        }

    }


    std::string XClient::get_name() const {
        return _name;
    }

    void XClient::printSl(shortLineitem *t) {
        cout << t->l_orderkey << " | "
             << t->l_partkey << " | "
             << t->l_suppkey << " | "
             << t->l_linenumber << " | "
             << t->l_quantity << " | "
             << t->l_extendedprice << " | "
             << t->l_discount << " | "
             << t->l_tax
             << endl;
    }

    thread XClient::startReceiving(std::string tableName) {

        thread t1(&XClient::receive, this, tableName);
        while (!_startedTransfer)
            std::this_thread::sleep_for(SLEEP_TIME);

        cout << "#3 Initialized" << endl;
        return t1;
    }

    void XClient::receive(std::string tableName) {
        using namespace boost::asio;
        using ip::tcp;


        //this is for IP address
        /*boost::asio::io_service io_service;
        //socket creation
        ip::tcp::socket socket(io_service);
        socket.connect(tcp::endpoint(boost::asio::ip::address::from_string("127.0.0.1"), 1234));
         */

        //this is for hostname
        boost::asio::io_service io_service;
        ip::tcp::socket socket(io_service);
        boost::asio::ip::tcp::resolver resolver(io_service);
        boost::asio::ip::tcp::resolver::query query("xdbcserver", "1234");
        boost::asio::ip::tcp::resolver::iterator iter = resolver.resolve(query);
        boost::asio::ip::tcp::endpoint endpoint = iter->endpoint();

        //connection
        //TODO: fix hardcoded hostname
        cout << "trying to connect" << endl;
        socket.connect(endpoint);
        cout << "connected" << endl;

        const std::string msg = tableName + "\n";
        boost::system::error_code error;
        boost::asio::write(socket, boost::asio::buffer(msg), error);


        int bpi = 0;
        int buffers = 0;

        cout << "#2 Entered receive thread" << endl;

        //while (buffers < TOTAL_TUPLES / BUFFER_SIZE) {
        while (true) {

            //cout << "Reading" << endl;
            _readState.store(0);
            int loops = 0;
            while (_flagArray[bpi] == 0) {
                bpi++;
                if (bpi == BUFFERPOOL_SIZE) {
                    bpi = 0;
                    loops++;
                    if (loops == 1000000) {
                        loops = 0;
                        cout << "client: stuck in receive" << endl;
                        std::this_thread::sleep_for(SLEEP_TIME);
                    }
                }
            }

            // getting response from server

            std::array<size_t, 2> header{};
            _readState.store(1);

            size_t headerBytes = boost::asio::read(socket, boost::asio::buffer(header),
                                                   boost::asio::transfer_exactly(16), error);


            if (error) {
                cout << "Client: boost error while reading header: " << error.message() << endl;
                cout << "Client: Header: comp: " << header[0] << ", size: " << header[1] << ", headerBytes: "
                     << headerBytes << endl;
                if (_totalBuffersRead > 0) {
                    _finishedTransfer = true;
                    break;
                }

            }

            _readState.store(2);
            //cout << "Next buffer size:" << header[0] << endl;
            size_t readBytes;
            //cout << "header | comp: " << header[0] << ", buffer size:" << header[1] << endl;

            if (header[0] > 4 || header[1] > BUFFER_SIZE * TUPLE_SIZE)
                cout << "Client: corrupt header: comp: " << header[0] << ", size: " << header[1] << ", headerBytes: "
                     << headerBytes << endl;

            if (header[0] > 0) {
                std::vector<char> compressed_buffer(header[1]);
                readBytes = boost::asio::read(socket, boost::asio::buffer(compressed_buffer),
                                              boost::asio::transfer_exactly(header[1]), error);
                boost::asio::const_buffer buffer = boost::asio::buffer(compressed_buffer.data(), readBytes);
                //TODO: handle errors correctly
                int decompError = decompress(header[0], _bufferPool[bpi].data(), buffer, readBytes);
                if (decompError == 1)
                    cout << "Client: decompress error: header: comp: " << header[0] << ", size: " << header[1]
                         << ", headerBytes: "
                         << headerBytes << endl;

            } else
                readBytes = boost::asio::read(socket, boost::asio::buffer(_bufferPool[bpi]),
                                              boost::asio::transfer_exactly(header[1]), error);

            if (error) {
                cout << "Client: boost error while reading body: " << error.message() << endl;
                cout << "Client: readBytes: " << readBytes << endl;
                if (_totalBuffersRead > 0) {
                    _finishedTransfer = true;
                    break;
                }

            }
            //cout << "Client: no error, incrementing totalBuffersRead" << endl;

            _totalBuffersRead.fetch_add(1);

            _readState.store(3);
            //cout << "Received " << readBytes << " bytes" << endl;
            //decompress(_bufferPool[bpi].data(), BUFFER_SIZE * TUPLE_SIZE, &compressed_buffer, header[0]);
            /*boost::asio::read(socket, boost::asio::buffer(_bufferPool[bpi]),
                              boost::asio::transfer_all(), error);*/

            _startedTransfer = true;

            /*for (auto x: _bufferPool[bpi]) {

                printSl(&x);
            }*/

            /*if (error == boost::asio::error::eof) {

                break;
            }*/

            _flagArray[bpi] = 0;
            buffers++;
            _readState.store(4);
            /* cout << "bpi: " << bpi << endl;
             cout << "flagidx " << (bpi / (BUFFER_SIZE * TUPLE_SIZE)) << endl;*/

        }
        _readState.store(5);
        _finishedTransfer.store(true);
        cout << "Client: finished transfer: " << _finishedTransfer << " with #buffers: " << buffers << endl;


    }


    buffWithId XClient::getBuffer() {

        int buffId = 0;
        int loops = 0;
        //TODO: remove workaround
        if (_finishedTransfer)
            buffId = -1;
        else {
            while (_flagArray[buffId] == 1) {
                buffId++;
                if (buffId == BUFFERPOOL_SIZE) {
                    buffId = 0;
                    loops++;
                    if (loops == 1000000) {
                        loops = 0;
                        cout << "Client: stuck in getBuffer" << endl;
                        cout << "Client: finishedTransfer: " << _finishedTransfer << ", emptyFlagBuffs: "
                             << emptyFlagBuffs()
                             << ", readState:" << _readState << ", totalBuffersRead: " << _totalBuffersRead
                             << endl;
                        std::this_thread::sleep_for(SLEEP_TIME);
                    }
                }
            }
        }
        buffWithId curBuf{};
        curBuf.id = buffId;
        if (buffId > -1) {
            curBuf.buff = _bufferPool[buffId];
            //std::copy(std::begin(_bufferPool[i]), std::end(_bufferPool[i]), std::begin(curBuf.buff));
        }


        return curBuf;
    }

    void XClient::markBufferAsRead(int bufferId) {
        _flagArray[bufferId] = 1;
    }

    int XClient::getBufferPoolSize() {
        return BUFFERPOOL_SIZE;
    }

    bool XClient::emptyFlagBuffs() {
        for (auto &i: _flagArray) {
            if (i == 0)
                return false;
        }
        return true;
    }

    bool XClient::hasUnread() {
        if (_finishedTransfer && emptyFlagBuffs())
            return false;
        return true;
    }


}
