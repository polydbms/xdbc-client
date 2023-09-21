#include "xclient.h"
#include <iostream>
#include <boost/asio.hpp>
#include <boost/asio/deadline_timer.hpp>
#include <thread>
#include <chrono>
#include <algorithm>
#include <iterator>
#include <utility>

#include "spdlog/spdlog.h"
#include "spdlog/sinks/stdout_color_sinks.h"
#include "Decompression/Decompressor.h"


using namespace boost::asio;
using ip::tcp;

namespace xdbc {


    void XClient::finalize() {
        spdlog::get("XDBC.CLIENT")->info(
                "Finalizing XClient: {0}, shutting down {1} threads, read state {2}, consumedAll {3}, baseSocket {4}",
                _xdbcenv.env_name, _xdbcenv.rcv_parallelism, _readState, tConsumedAll(), _baseSocket.is_open());
        for (int i = 0; i < _xdbcenv.rcv_parallelism; i++) {
            _rcvThreads[i].join();
        }
        /*for (int i = 0; i < _xdbcenv.read_parallelism; i++) {
            _readThreads[i].join();
        }*/

        /*auto ex = _baseSocket.get_executor();

        auto *c = ex.target<boost::asio::io_service>();
        c->stop();*/

        _baseSocket.close();

        spdlog::get("XDBC.CLIENT")->info("Finalizing: basesocket closed");
    }

    XClient::XClient(const RuntimeEnv &env) :
            _xdbcenv(env),
            _bufferPool(),
            _consumedAll(env.rcv_parallelism),
            _readState(0),
            _totalBuffersRead(0),
            //_readThreads(env.read_parallelism),
            _rcvThreads(env.rcv_parallelism),
            _readSockets(),
            _baseSocket(_ioContext) {

        auto console = spdlog::stdout_color_mt("XDBC.CLIENT");

        spdlog::get("XDBC.CLIENT")->info("Creating Client: {0}, BPS: {1}, BS: {2}, TS: {3}, iformat: {4} ",
                                         _xdbcenv.env_name, env.bufferpool_size, env.buffer_size, env.tuple_size,
                                         env.iformat);

        _bufferPool.resize(env.bufferpool_size, std::vector<std::byte>(env.buffer_size * env.tuple_size));


        for (int i = 0; i < env.rcv_parallelism; i++) {
            _consumedAll[i].store(false);
        }

    }

    XClient::~XClient() {
        // Destructor implementation...
    }

    std::string XClient::get_name() const {
        return _xdbcenv.env_name;
    }

    std::string read_(tcp::socket &socket) {
        boost::asio::streambuf buf;
        boost::system::error_code error;
        size_t bytes = boost::asio::read_until(socket, buf, "\n", error);

        if (error) {
            spdlog::get("XDBC.CLIENT")->warn("Boost error while reading: {0} ", error.message());
        }
        std::string data = boost::asio::buffer_cast<const char *>(buf.data());
        return data;
    }

    int XClient::startReceiving(const std::string &tableName) {

        //establish base connection with server
        XClient::initialize(tableName);

        //create rcv threads
        for (int i = 0; i < _xdbcenv.rcv_parallelism; i++) {
            FBQ_ptr q(new queue<int>);
            _xdbcenv.rcvBufferPtr.push_back(q);
            //initially all buffers are free to write into
            for (int j = i * (_xdbcenv.bufferpool_size / _xdbcenv.rcv_parallelism);
                 j < (i + 1) * (_xdbcenv.bufferpool_size / _xdbcenv.rcv_parallelism);
                 j++)
                q->push(j);

            _rcvThreads[i] = std::thread(&XClient::receive, this, i);
        }

        for (int i = 0; i < _xdbcenv.read_parallelism; i++) {
            FBQ_ptr q(new queue<int>);
            _xdbcenv.readBufferPtr.push_back(q);
        }

        spdlog::get("XDBC.CLIENT")->info("#3 Initialized");
        return 1;
    }


    void XClient::initialize(const std::string &tableName) {

        //this is for IP address
        /*boost::asio::io_service io_service;
        //socket creation
        ip::tcp::socket socket(io_service);
        socket.connect(tcp::endpoint(boost::asio::ip::address::from_string("127.0.0.1"), 1234));
         */

        //this is for hostname
        
        boost::asio::ip::tcp::resolver resolver(_ioContext);
        boost::asio::ip::tcp::resolver::query query(_xdbcenv.server_host, _xdbcenv.server_port);
        boost::asio::ip::tcp::resolver::iterator iter = resolver.resolve(query);
        boost::asio::ip::tcp::endpoint endpoint = iter->endpoint();

        spdlog::get("XDBC.CLIENT")->info("Basesocket: trying to connect");

        _baseSocket.connect(endpoint);
        spdlog::get("XDBC.CLIENT")->info("Basesocket: connected");

        const std::string msg = tableName + "\n";
        boost::system::error_code error;
        boost::asio::write(_baseSocket, boost::asio::buffer(msg), error);

        //std::this_thread::sleep_for(_xdbcenv.sleep_time*10);
        std::string ready = read_(_baseSocket);

        //ready.erase(std::remove(ready.begin(), ready.end(), '\n'), ready.cend());
        spdlog::get("XDBC.CLIENT")->info("Basesocket: Server signaled: {0}", ready);

        //return socket;

    }


    void XClient::receive(int thr) {
        spdlog::get("XDBC.CLIENT")->info("Entered read thread {0} ", thr);
        boost::asio::io_service io_service;
        ip::tcp::socket socket(io_service);
        boost::asio::ip::tcp::resolver resolver(io_service);
        boost::asio::ip::tcp::resolver::query query(_xdbcenv.server_host,
                                                    std::to_string(stoi(_xdbcenv.server_port) + thr + 1));
        boost::asio::ip::tcp::resolver::iterator iter = resolver.resolve(query);
        boost::asio::ip::tcp::endpoint endpoint = iter->endpoint();


        try {
            socket.connect(endpoint);

        } catch (const boost::system::system_error &error) {
            spdlog::get("XDBC.CLIENT")->warn("Server error: {0}", error.what());
            //std::this_thread::sleep_for(_xdbcenv.sleep_time);
        }


        spdlog::get("XDBC.CLIENT")->info("Read thread {0} connected to {1}:{2}",
                                         thr, endpoint.address().to_string(), endpoint.port());

        const std::string msg = std::to_string(thr) + "\n";
        boost::system::error_code error;

        try {
            size_t b = boost::asio::write(socket, boost::asio::buffer(msg), error);
        } catch (const boost::system::system_error &e) {
            spdlog::get("XDBC.CLIENT")->warn("Could not write thread no, error: {0}", e.what());
        }

        //partition read threads
        //int minBId = thr * (_xdbcenv.bufferpool_size / _xdbcenv.rcv_parallelism);
        //int maxBId = (thr + 1) * (_xdbcenv.bufferpool_size / _xdbcenv.rcv_parallelism);

        //spdlog::get("XDBC.CLIENT")->info("Read thread {0} assigned ({1},{2})", thr, minBId, maxBId);

        int bpi = 0;
        int buffers = 0;

        spdlog::get("XDBC.CLIENT")->info("Read thread {0} started", thr);

        size_t headerBytes;
        size_t readBytes;
        int decompError;
        int readThreadId = 0;
        while (error != boost::asio::error::eof) {

            _readState.store(0);
            bpi = _xdbcenv.rcvBufferPtr[thr]->pop();
            _readState.store(1);

            // getting response from server
            Header header{};
            headerBytes = boost::asio::read(socket, boost::asio::buffer(&header, sizeof(Header)),
                                            boost::asio::transfer_exactly(sizeof(Header)), error);

            //uint16_t checksum = header.crc;

            //TODO: handle error types (e.g., EOF)
            if (error) {
                spdlog::get("XDBC.CLIENT")->error("Read thread {0}: boost error while reading header: {1}", thr,
                                                  error.message());
                spdlog::get("XDBC.CLIENT")->error("header comp: {0}, size: {1}, headerBytes: {2}",
                                                  header.compressionType,
                                                  header.totalSize,
                                                  headerBytes);

                if (error == boost::asio::error::eof) {

                }
                break;
            }

            _readState.store(2);

            // check for errors in header
            if (header.compressionType > 6)
                spdlog::get("XDBC.CLIENT")->error("Client: corrupt header: comp: {0}, size: {1}, headerbytes: {2}",
                                                  header.compressionType, header.totalSize, headerBytes);
            if (header.totalSize > _xdbcenv.buffer_size * _xdbcenv.tuple_size)
                spdlog::get("XDBC.CLIENT")->error(
                        "Client: corrupt body: comp: {0}, size: {1}/{2}, headerbytes: {3}",
                        header.compressionType, header.totalSize, _xdbcenv.buffer_size * _xdbcenv.tuple_size,
                        headerBytes);

            // all good, read incoming body
            std::vector<std::byte> compressed_buffer(header.totalSize);
            if (header.compressionType > 0) {
                readBytes = boost::asio::read(socket, boost::asio::buffer(compressed_buffer),
                                              boost::asio::transfer_exactly(header.totalSize), error);

            } else {
                readBytes = boost::asio::read(socket, boost::asio::buffer(_bufferPool[bpi]),
                                              boost::asio::transfer_exactly(header.totalSize), error);
            }

            // check for errors in body
            //TODO: handle errors correctly

            if (error) {
                spdlog::get("XDBC.CLIENT")->error("Client: boost error while reading body: readBytes {0}, error: {1}",
                                                  readBytes, error.message());
                if (error == boost::asio::error::eof) {

                }
                break;
            }

            if (header.compressionType > 0) {
                if (header.compressionType == 6) {

                    int posInWriteBuffer = 0;
                    size_t posInReadBuffer = 0;
                    int i = 0;

                    for (auto attribute: _xdbcenv.schema) {

                        //spdlog::get("XDBC.CLIENT")->warn("Handling att: {0}", std::get<0>(attribute));

                        if (std::get<1>(attribute) == "INT") {


                            decompError = Decompressor::decompress_int_col(
                                    reinterpret_cast<const uint32_t *>(compressed_buffer.data()) +
                                    posInReadBuffer / std::get<2>(attribute),
                                    header.attributeSize[i] / std::get<2>(attribute),
                                    _bufferPool[bpi].data() + posInWriteBuffer,
                                    _xdbcenv.buffer_size);

                        } else if (std::get<1>(attribute) == "DOUBLE") {
                            /*spdlog::get("XDBC.CLIENT")->warn("Handling att: {0}, compressed_size: {1}",
                                                             std::get<0>(attribute), attSize);*/

                            /*decompError = Decompressor::decompress_zstd(_bufferPool[bpi].data() + posInWriteBuffer,
                                                                          compressed_buffer.data() + posInReadBuffer,
                                                                          header.attributeSize[i],
                                                                          _xdbcenv.buffer_size *8);*/
                            //TODO: refactor fpzip decompression to method
                            decompError = Decompressor::decompress_double_col(
                                    compressed_buffer.data() + posInReadBuffer,
                                    header.attributeSize[i],
                                    _bufferPool[bpi].data() + posInWriteBuffer,
                                    _xdbcenv.buffer_size);

                        }
                        posInWriteBuffer += _xdbcenv.buffer_size * std::get<2>(attribute);
                        posInReadBuffer += header.attributeSize[i];
                        i++;
                    }

                } else
                    decompError = Decompressor::decompress(header.compressionType, _bufferPool[bpi].data(),
                                                           compressed_buffer.data(), readBytes,
                                                           _xdbcenv.buffer_size * _xdbcenv.tuple_size);

                if (decompError == 1) {

                    /*size_t computed_checksum = compute_crc(boost::asio::buffer(_bufferPool[bpi]));
                    if (computed_checksum != checksum) {
                        spdlog::get("XDBC.CLIENT")->warn("CHECKSUM MISMATCH expected: {0}, got: {1}",
                                                         checksum, computed_checksum);
                    }*/

                    if (header.intermediateFormat == 1) {
                        Utils::shortLineitem sl = {-2, -2, -2, -2, -2, -2, -2, -2};
                        std::memcpy(_bufferPool[bpi].data(), &sl, sizeof(sl));
                    }
                    if (header.intermediateFormat == 2) {
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
                            header.compressionType, header.totalSize, headerBytes);

                }

            }
            //printSl(reinterpret_cast<shortLineitem *>(compressed_buffer.data()));

            _totalBuffersRead.fetch_add(1);
            _xdbcenv.readBufferPtr[readThreadId]->push(bpi);
            readThreadId++;
            if (readThreadId == _xdbcenv.read_parallelism)
                readThreadId = 0;

            buffers++;
            _readState.store(3);
        }

        /*for (auto x: _bufferPool[bpi])
            printSl(&x);*/

        _readState.store(4);

        for (int i = 0; i < _xdbcenv.read_parallelism; i++)
            _xdbcenv.readBufferPtr[i]->push(-1);

        socket.close();

        spdlog::get("XDBC.CLIENT")->info("Read thread {0} #buffers: {1}", thr, buffers);
    }


    buffWithId XClient::getBuffer() {

        int buffId;

        //TODO: remove workaround
        buffId = _xdbcenv.readBufferPtr[0]->pop();

        buffWithId curBuf{};
        curBuf.buff.resize(_xdbcenv.buffer_size * _xdbcenv.tuple_size);
        curBuf.id = buffId;
        if (buffId > -1) {
            curBuf.buff = _bufferPool[buffId];
            //TODO: set intermediate format dynamically
            curBuf.iformat = _xdbcenv.iformat;
            //std::copy(std::begin(_bufferPool[i]), std::end(_bufferPool[i]), std::begin(curBuf.buff));
        } else if (buffId == -1) {
            _consumedAll[0].store(true);
        }

        return curBuf;
    }


    int XClient::getBufferPoolSize() const {
        return _xdbcenv.bufferpool_size;
    }

    bool XClient::hasNext() {
        if (tConsumedAll())
            return false;

        return true;
    }

    bool XClient::tConsumedAll() {

        for (int i = 0; i < _xdbcenv.rcv_parallelism; i++)
            if (!_consumedAll[i])
                return false;

        return true;
    }

    void XClient::markBufferAsRead(int buffId) {

        _xdbcenv.rcvBufferPtr[0]->push(buffId);
    }

}
