#include "xclient.h"
#include <iostream>
#include <boost/asio.hpp>
#include <boost/asio/deadline_timer.hpp>
#include <boost/crc.hpp>
#include <thread>
#include <chrono>
#include <algorithm>
#include <iterator>
#include <utility>

#include "spdlog/spdlog.h"
#include "spdlog/sinks/stdout_color_sinks.h"
#include "Decompression/Decompressor.h"

using namespace std;

namespace xdbc {

    uint16_t compute_checksum(const uint8_t *data, std::size_t size) {
        uint16_t checksum = 0;
        for (std::size_t i = 0; i < size; ++i) {
            checksum ^= data[i];
        }
        return checksum;
    }

    size_t compute_crc(const boost::asio::const_buffer &buffer) {
        boost::crc_32_type crc;
        crc.process_bytes(boost::asio::buffer_cast<const void *>(buffer), boost::asio::buffer_size(buffer));
        return crc.checksum();
    }

    int decompress(int method, void *dst, const boost::asio::const_buffer &in, size_t in_size, int out_size) {
        //1 zstd
        //2 snappy
        //3 lzo
        //4 lz4
        //5 zlib

        if (method == 1)
            return Decompressor::decompress_zstd(dst, in, in_size, out_size);

        if (method == 2)
            return Decompressor::decompress_snappy(dst, in, in_size, out_size);

        if (method == 3)
            return Decompressor::decompress_lzo(dst, in, in_size, out_size);

        if (method == 4)
            return Decompressor::decompress_lz4(dst, in, in_size, out_size);

        if (method == 5)
            return Decompressor::decompress_zlib(dst, in, in_size, out_size);


        return 1;
    }


    void XClient::finalize() {
        spdlog::get("XDBC.CLIENT")->info("Finalizing XClient: {0}", _name);
        _finishedReading = true;
    }

    XClient::XClient(const RuntimeEnv &env) :
            _xdbcenv(env),
            _bufferPool(),
            _flagArray(env.bufferpool_size),
            _finishedTransfer(false),
            _startedTransfer(false),
            _finishedReading(false),
            _readState(0),
            _totalBuffersRead(0) {

        auto console = spdlog::stdout_color_mt("XDBC.CLIENT");

        spdlog::get("XDBC.CLIENT")->info("Creating Client: {0}, BPS: {1}, BS: {2}, TS: {3}, iformat: {4} ",
                                         _name, env.bufferpool_size, env.buffer_size, env.tuple_size, env.iformat);

        _bufferPool.resize(env.bufferpool_size, std::vector<std::byte>(env.buffer_size * env.tuple_size));

        for (auto &i: _flagArray)
            i.store(1);

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

        spdlog::get("XDBC.CLIENT")->info("#3 Initialized");
        return t1;
    }

    void XClient::receive(const std::string &tableName) {
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
        spdlog::get("XDBC.CLIENT")->info("trying to connect");

        socket.connect(endpoint);
        spdlog::get("XDBC.CLIENT")->info("connected");

        const std::string msg = tableName + "\n";
        boost::system::error_code error;
        boost::asio::write(socket, boost::asio::buffer(msg), error);


        int bpi = 0;
        int buffers = 0;

        spdlog::get("XDBC.CLIENT")->info("#2 Entered receive thread");

        //while (buffers < TOTAL_TUPLES / BUFFER_SIZE) {
        while (true) {

            //cout << "Reading" << endl;
            _readState.store(0);
            int loops = 0;
            while (_flagArray[bpi] == 0) {
                bpi++;
                if (bpi == _xdbcenv.bufferpool_size) {
                    bpi = 0;
                    loops++;
                    if (loops == 1000000) {
                        loops = 0;
                        spdlog::get("XDBC.CLIENT")->warn("stuck in receive");
                        std::this_thread::sleep_for(SLEEP_TIME);
                    }
                }
            }

            // getting response from server

            std::array<size_t, 4> header{};
            _readState.store(1);

            size_t headerBytes = boost::asio::read(socket, boost::asio::buffer(header),
                                                   boost::asio::transfer_exactly(32), error);

            uint16_t checksum = header[2];

            //TODO: handle error types (e.g., EOF)
            if (error) {
                spdlog::get("XDBC.CLIENT")->error("boost error while reading header: {0}", error.message());
                spdlog::get("XDBC.CLIENT")->error("header com: {0}, size: {1}, headerBytes: {2}", header[0],
                                                  header[1],
                                                  headerBytes);

                if (_totalBuffersRead > 0) {
                    _finishedTransfer = true;
                    break;
                }

            }

            _readState.store(2);
            //cout << "Next buffer size:" << header[0] << endl;
            size_t readBytes;
            //cout << "header | comp: " << header[0] << ", buffer size:" << header[1] << endl;

            if (header[0] > 5 || header[1] > _xdbcenv.buffer_size * _xdbcenv.tuple_size)
                spdlog::get("XDBC.CLIENT")->error("Client: corrupt header: comp: {0}, size: {1}, headerbytes: {2}",
                                                  header[0], header[1], headerBytes);

            if (header[0] > 0) {
                std::vector<char> compressed_buffer(header[1]);
                readBytes = boost::asio::read(socket, boost::asio::buffer(compressed_buffer),
                                              boost::asio::transfer_exactly(header[1]), error);
                boost::asio::const_buffer buffer = boost::asio::buffer(compressed_buffer.data(), readBytes);
                //TODO: handle errors correctly
                int decompError = decompress(header[0], _bufferPool[bpi].data(), buffer, readBytes,
                                             _xdbcenv.buffer_size * _xdbcenv.tuple_size);
                if (decompError == 1) {

                    /*size_t computed_checksum = compute_crc(boost::asio::buffer(_bufferPool[bpi]));
                    if (computed_checksum != checksum) {
                        spdlog::get("XDBC.CLIENT")->warn("CHECKSUM MISMATCH expected: {0}, got: {1}",
                                                         checksum, computed_checksum);
                    }*/

                    if (header[3] == 1) {
                        shortLineitem sl = {-2, -2, -2, -2, -2, -2, -2, -2};
                        std::memcpy(_bufferPool[bpi].data(), &sl, sizeof(sl));
                    }
                    if (header[3] == 2) {
                        int m2 = -2;
                        int bs = _xdbcenv.buffer_size;
                        std::memcpy(_bufferPool[bpi].data(), &m2, 4);
                        std::memcpy(_bufferPool[bpi].data() + bs * 4, &m2, 4);
                        std::memcpy(_bufferPool[bpi].data() + bs * 4 * 2, &m2, 4);
                        std::memcpy(_bufferPool[bpi].data() + bs * 4 * 3, &m2, 4);
                        std::memcpy(_bufferPool[bpi].data() + bs * 4 * 4, &m2, 8);
                        std::memcpy(_bufferPool[bpi].data() + bs * 16 + bs * 8, &m2, 8);
                        std::memcpy(_bufferPool[bpi].data() + bs * 16 + bs * 8 * 2, &m2, 8);
                        std::memcpy(_bufferPool[bpi].data() + bs * 16 + bs * 8 * 3, &m2, 8);

                    }

                    spdlog::get("XDBC.CLIENT")->warn(
                            "decompress error: header: comp: {0}, size: {1}, headerBytes: {2}",
                            header[0], header[1], headerBytes);

                }

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

        spdlog::get("XDBC.CLIENT")->info("finished transfer: {0}, #buffers: {1}", _finishedTransfer, buffers);

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
                if (buffId == _xdbcenv.bufferpool_size) {
                    buffId = 0;
                    loops++;
                    if (loops == 1000000) {
                        loops = 0;
                        spdlog::get("XDBC.CLIENT")->warn("stuck in getBuffer");
                        spdlog::get("XDBC.CLIENT")->warn(
                                "finishedTransfer: {0}, emptyFlagBuffs: {1}, readState: {2}, totalBuffersRead: {3}",
                                _finishedTransfer, emptyFlagBuffs(), _readState, _totalBuffersRead);

                        std::this_thread::sleep_for(SLEEP_TIME);
                    }
                }
            }
        }
        buffWithId curBuf{};
        curBuf.buff.resize(_xdbcenv.buffer_size * _xdbcenv.tuple_size);
        curBuf.id = buffId;
        if (buffId > -1) {
            curBuf.buff = _bufferPool[buffId];
            //TODO: set intermediate format dynamically
            curBuf.iformat = 2;
            //std::copy(std::begin(_bufferPool[i]), std::end(_bufferPool[i]), std::begin(curBuf.buff));
        }


        return curBuf;
    }

    void XClient::markBufferAsRead(int bufferId) {
        _flagArray[bufferId] = 1;
    }

    int XClient::getBufferPoolSize() {
        return _xdbcenv.bufferpool_size;
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

    std::string XClient::slStr(shortLineitem *t) {

        return std::to_string(t->l_orderkey) + std::string(", ") +
               std::to_string(t->l_partkey) + std::string(", ") +
               std::to_string(t->l_suppkey) + std::string(", ") +
               std::to_string(t->l_linenumber) + std::string(", ") +
               std::to_string(t->l_quantity) + std::string(", ") +
               std::to_string(t->l_extendedprice) + std::string(", ") +
               std::to_string(t->l_discount) + std::string(", ") +
               std::to_string(t->l_tax);
    }


}
