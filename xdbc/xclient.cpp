#include "xclient.h"
#include <iostream>
#include <boost/asio.hpp>
#include <boost/asio/deadline_timer.hpp>
#include <thread>
#include <chrono>
#include <algorithm>
#include <iterator>
#include <utility>
#include <numeric>

#include "spdlog/spdlog.h"
#include "spdlog/stopwatch.h"
#include "spdlog/sinks/stdout_color_sinks.h"
#include "spdlog/sinks/basic_file_sink.h"
#include "Decompression/Decompressor.h"


using namespace boost::asio;
using ip::tcp;

namespace xdbc {

    XClient::XClient(RuntimeEnv &env) :
            _xdbcenv(&env),
            _bufferPool(),
            _readState(0),
            _totalBuffersRead(0),
            _decompThreads(env.decomp_parallelism),
            _rcvThreads(env.rcv_parallelism),
            _readSockets(),
            _emptyDecompThreadCtr(env.read_parallelism),
            _markedFreeCounter(0),
            _outThreadId(0),
            _baseSocket(_ioContext) {

        auto console = spdlog::stdout_color_mt("XDBC.CLIENT");
        auto file_log = spdlog::basic_logger_mt("XDBC.CLIENT.FILE", "xdbcclient.txt", true);
        file_log->set_pattern("%t, %v, %E.%Fs");

        spdlog::get("XDBC.CLIENT.FILE")->info("Client");

        // Sollte eigentlich ein multithreaded logger sein, der gleichzeitig in die console und in eine Datei loggt. Gibt aber einen segmentation fault!
        /*        std::vector<spdlog::sink_ptr> sinks;
        sinks.push_back(std::make_shared<spdlog::sinks::stdout_color_sink_mt>());
        sinks.push_back(std::make_shared<spdlog::sinks::basic_file_sink_mt>("xdbcclient.txt", true));
        auto combined_logger = std::make_shared<spdlog::logger>("XDBC.CLIENT", begin(sinks), end(sinks));
        spdlog::register_logger(combined_logger);

        try
        {
            // Set up spdlog logger
            auto testlogger = spdlog::get("XDBC.CLIENT");
            if (!testlogger)
            {
                throw std::runtime_error("Logger not found");
            }

            // Log a message
            testlogger->error("Hello, World!");
        }
        catch (const std::exception& e)
        {
            // Handle exception and print error message
            std::cout << "Error: " << e.what() << std::endl;
            //spdlog::get("XDBC.CLIENT")->error("Yep, an errror!");
        }*/


        spdlog::get("XDBC.CLIENT")->info("Creating Client: {0}, BPS: {1}, BS: {2}, TS: {3}, iformat: {4} ",
                                         _xdbcenv->env_name, env.bufferpool_size, env.buffer_size, env.tuple_size,
                                         env.iformat);

        // populate bufferpool with empty vectors (header + payload)
        _bufferPool.resize(env.bufferpool_size,
                           std::vector<std::byte>(sizeof(Header) + env.buffer_size * env.tuple_size));


        for (int i = 0; i < env.read_parallelism; i++) {
            _emptyDecompThreadCtr[i] = 0;
        }
    }

    XClient::~XClient() {
        // Destructor implementation...
        spdlog::get("XDBC.CLIENT.FILE")->info("Client");
        ClientEnvPredictor pre;
        ClientRuntimeParams newParams = pre.tweakNextParams(_xdbcenv);
        spdlog::get("XDBC.CLIENT")->info("New parameters could be: rcv_parallelism {0}, decomp_parallelism {1}, read_parallelism {2}", newParams.rcv_parallelism, newParams.decomp_parallelism, newParams.read_parallelism);
    }

    void XClient::finalize() {
        spdlog::get("XDBC.CLIENT")->info(
                "Finalizing XClient: {0}, shutting down {1} receive threads & {2} decomp threads",
                _xdbcenv->env_name, _xdbcenv->rcv_parallelism, _xdbcenv->decomp_parallelism);

        for (int i = 0; i < _xdbcenv->rcv_parallelism; i++) {
            _rcvThreads[i].join();
        }

        for (int i = 0; i < _xdbcenv->decomp_parallelism; i++) {
            _decompThreads[i].join();
        }
        _baseSocket.close();

        spdlog::get("XDBC.CLIENT")->info("Finalizing: basesocket closed");
    }


    std::string XClient::get_name() const {
        return _xdbcenv->env_name;
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
        for (int i = 0; i < _xdbcenv->rcv_parallelism; i++) {
            FBQ_ptr q(new queue<int>);
            _xdbcenv->freeBufferIds.push_back(q);
            //initially all buffers are free to write into
            for (int j = i * (_xdbcenv->bufferpool_size / _xdbcenv->rcv_parallelism);
                 j < (i + 1) * (_xdbcenv->bufferpool_size / _xdbcenv->rcv_parallelism);
                 j++)
                q->push(j);

            _rcvThreads[i] = std::thread(&XClient::receive, this, i);
        }

        for (int i = 0; i < _xdbcenv->decomp_parallelism; i++) {
            FBQ_ptr q(new queue<int>);
            _xdbcenv->compressedBufferIds.push_back(q);
            _decompThreads[i] = std::thread(&XClient::decompress, this, i);
        }

        for (int i = 0; i < _xdbcenv->read_parallelism; i++) {
            FBQ_ptr q(new queue<int>);
            _xdbcenv->decompressedBufferIds.push_back(q);
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

        spdlog::get("XDBC.CLIENT.FILE")->info("Connection");

        //this is for hostname

        boost::asio::ip::tcp::resolver resolver(_ioContext);
        boost::asio::ip::tcp::resolver::query query(_xdbcenv->server_host, _xdbcenv->server_port);
        boost::asio::ip::tcp::resolver::iterator iter = resolver.resolve(query);
        boost::asio::ip::tcp::endpoint endpoint = iter->endpoint();

        spdlog::get("XDBC.CLIENT")->info("Basesocket: trying to connect");

        _baseSocket.connect(endpoint);
        spdlog::get("XDBC.CLIENT")->info("Basesocket: connected");

        const std::string msg = tableName + "\n";
        boost::system::error_code error;
        boost::asio::write(_baseSocket, boost::asio::buffer(msg), error);

        //std::this_thread::sleep_for(_xdbcenv->sleep_time*10);
        std::string ready = read_(_baseSocket);

        //ready.erase(std::remove(ready.begin(), ready.end(), '\n'), ready.cend());
        spdlog::get("XDBC.CLIENT")->info("Basesocket: Server signaled: {0}", ready);

        spdlog::get("XDBC.CLIENT.FILE")->info("Connection");
        //return socket;

    }


    void XClient::receive(int thr) {
        spdlog::get("XDBC.CLIENT")->info("Entered receive thread {0} ", thr);
        spdlog::get("XDBC.CLIENT.FILE")->info("Receive data");
        boost::asio::io_service io_service;
        ip::tcp::socket socket(io_service);
        boost::asio::ip::tcp::resolver resolver(io_service);
        boost::asio::ip::tcp::resolver::query query(_xdbcenv->server_host,
                                                    std::to_string(stoi(_xdbcenv->server_port) + thr + 1));
        boost::asio::ip::tcp::resolver::iterator iter = resolver.resolve(query);
        boost::asio::ip::tcp::endpoint endpoint = iter->endpoint();


        try {
            socket.connect(endpoint);

        } catch (const boost::system::system_error &error) {
            spdlog::get("XDBC.CLIENT")->warn("Server error: {0}", error.what());
            //std::this_thread::sleep_for(_xdbcenv->sleep_time);
        }


        spdlog::get("XDBC.CLIENT")->info("Receive thread {0} connected to {1}:{2}",
                                         thr, endpoint.address().to_string(), endpoint.port());

        const std::string msg = std::to_string(thr) + "\n";
        boost::system::error_code error;

        try {
            size_t b = boost::asio::write(socket, boost::asio::buffer(msg), error);
        } catch (const boost::system::system_error &e) {
            spdlog::get("XDBC.CLIENT")->warn("Could not write thread no, error: {0}", e.what());
        }

        //partition read threads
        //int minBId = thr * (_xdbcenv->bufferpool_size / _xdbcenv->rcv_parallelism);
        //int maxBId = (thr + 1) * (_xdbcenv->bufferpool_size / _xdbcenv->rcv_parallelism);

        //spdlog::get("XDBC.CLIENT")->info("Read thread {0} assigned ({1},{2})", thr, minBId, maxBId);

        int bpi;
        int buffers = 0;

        spdlog::get("XDBC.CLIENT")->info("Receive thread {0} started", thr);

        size_t headerBytes;
        size_t readBytes;

        int decompThreadId = 0;
        while (error != boost::asio::error::eof) {

            // Wait for next free buffer. Measure wait time and set appropriate readstates while wating and when starting after the wait.
            _readState.store(0);
            auto start_wait = std::chrono::high_resolution_clock::now();

            bpi = _xdbcenv->freeBufferIds[thr]->pop();

            auto duration_wait_microseconds = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::high_resolution_clock::now() - start_wait).count();
            _xdbcenv->rcv_wait_time.fetch_add(duration_wait_microseconds, std::memory_order_relaxed);
            _readState.store(1);

            // getting response from server. Start with reading header and measuring header receive time.

            auto start_hdrrcv = std::chrono::high_resolution_clock::now();

            Header header{};
            headerBytes = boost::asio::read(socket, boost::asio::buffer(&header, sizeof(Header)),
                                            boost::asio::transfer_exactly(sizeof(Header)), error);

            auto duration_hdr_microseconds = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::high_resolution_clock::now() - start_hdrrcv).count();
            _xdbcenv->rcv_time.fetch_add(duration_hdr_microseconds, std::memory_order_relaxed);

            //uint16_t checksum = header.crc;

            //TODO: handle error types (e.g., EOF)
            if (error) {
                spdlog::get("XDBC.CLIENT")->error("Receive thread {0}: boost error while reading header: {1}", thr,
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
            if (header.totalSize > _xdbcenv->buffer_size * _xdbcenv->tuple_size)
                spdlog::get("XDBC.CLIENT")->error(
                        "Client: corrupt body: comp: {0}, size: {1}/{2}, headerbytes: {3}",
                        header.compressionType, header.totalSize, _xdbcenv->buffer_size * _xdbcenv->tuple_size,
                        headerBytes);

            // all good, read incoming body and measure time
            std::vector<std::byte> compressed_buffer(sizeof(Header) + header.totalSize);

            auto start_fullrcv = std::chrono::high_resolution_clock::now();

            //TODO: read header directly into the compressed buffer
            std::memcpy(_bufferPool[bpi].data(), &header, sizeof(Header));

            readBytes = boost::asio::read(socket, boost::asio::buffer(_bufferPool[bpi].data() + sizeof(Header),
                                                                      header.totalSize),
                                          boost::asio::transfer_exactly(header.totalSize), error);

            auto duration_full_microseconds = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::high_resolution_clock::now() - start_fullrcv).count();
            _xdbcenv->rcv_time.fetch_add(duration_full_microseconds, std::memory_order_relaxed);

            // check for errors in body
            //TODO: handle errors correctly

            if (error) {
                spdlog::get("XDBC.CLIENT")->error("Client: boost error while reading body: readBytes {0}, error: {1}",
                                                  readBytes, error.message());
                if (error == boost::asio::error::eof) {

                }
                break;
            }



            //printSl(reinterpret_cast<shortLineitem *>(compressed_buffer.data()));

            _totalBuffersRead.fetch_add(1);
            _xdbcenv->compressedBufferIds[decompThreadId]->push(bpi);
            decompThreadId++;
            if (decompThreadId == _xdbcenv->decomp_parallelism)
                decompThreadId = 0;

            buffers++;
            _readState.store(3);
        }

        /*for (auto x: _bufferPool[bpi])
            printSl(&x);*/

        _readState.store(4);

        for (int i = 0; i < _xdbcenv->decomp_parallelism; i++)
            _xdbcenv->compressedBufferIds[i]->push(-1);

        socket.close();

        spdlog::get("XDBC.CLIENT")->info("Receive thread {0} #buffers: {1}", thr, buffers);
        spdlog::get("XDBC.CLIENT.FILE")->info("Receive data");
    }

    void XClient::  decompress(int thr) {
        spdlog::get("XDBC.CLIENT.FILE")->info("Decompress data");

        int readThrId = 0;
        int emptyCtr = 0;
        int decompError;
        std::vector<char> decompressed_buffer(_xdbcenv->buffer_size * _xdbcenv->tuple_size);
        while (emptyCtr < _xdbcenv->rcv_parallelism) {

            // Wait for next buffer to decompress and measure the waiting time
            auto start_wait = std::chrono::high_resolution_clock::now();

            int compBuffId = _xdbcenv->compressedBufferIds[thr]->pop();

            auto duration_wait_microseconds = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::high_resolution_clock::now() - start_wait).count();
            _xdbcenv->decomp_wait_time.fetch_add(duration_wait_microseconds, std::memory_order_relaxed);


            // decompress the specific buffer depending on the type (header or not, type of header,...) and measure time

            auto start = std::chrono::high_resolution_clock::now();

            if (compBuffId == -1)
                emptyCtr++;
            else {

                Header *header = reinterpret_cast<Header *>(_bufferPool[compBuffId].data());
                std::byte *compressed_buffer = _bufferPool[compBuffId].data() + sizeof(Header);

                if (header->compressionType > 0) {

                    //TODO: refactor decompress_cols with schema in Decompressor
                    if (header->compressionType == 6) {

                        int posInWriteBuffer = 0;
                        size_t posInReadBuffer = 0;
                        int i = 0;

                        for (auto attribute: _xdbcenv->schema) {

                            //spdlog::get("XDBC.CLIENT")->warn("Handling att: {0}", std::get<0>(attribute));

                            if (std::get<1>(attribute) == "INT") {

                                decompError = Decompressor::decompress_int_col(
                                        reinterpret_cast<const uint32_t *>(compressed_buffer) +
                                        posInReadBuffer / std::get<2>(attribute),
                                        header->attributeSize[i] / std::get<2>(attribute),
                                        decompressed_buffer.data() + posInWriteBuffer,
                                        _xdbcenv->buffer_size);

                            } else if (std::get<1>(attribute) == "DOUBLE") {
                                /*spdlog::get("XDBC.CLIENT")->warn("Handling att: {0}, compressed_size: {1}",
                                                                 std::get<0>(attribute), attSize);*/

                                /*decompError = Decompressor::decompress_zstd(
                                        decompressed_buffer.data() + posInWriteBuffer,
                                        compressed_buffer + posInReadBuffer,
                                        header->attributeSize[i],
                                        _xdbcenv->buffer_size * 8);*/

                                decompError = Decompressor::decompress_double_col(
                                        compressed_buffer + posInReadBuffer,
                                        header->attributeSize[i],
                                        decompressed_buffer.data() + posInWriteBuffer,
                                        _xdbcenv->buffer_size);

                            }
                            posInWriteBuffer += _xdbcenv->buffer_size * std::get<2>(attribute);
                            posInReadBuffer += header->attributeSize[i];
                            i++;
                        }

                    } else
                        decompError = Decompressor::decompress(header->compressionType, decompressed_buffer.data(),
                                                               compressed_buffer, header->totalSize,
                                                               _xdbcenv->buffer_size * _xdbcenv->tuple_size);

                    if (decompError == 1) {

                        /*size_t computed_checksum = compute_crc(boost::asio::buffer(_bufferPool[bpi]));
                        if (computed_checksum != checksum) {
                            spdlog::get("XDBC.CLIENT")->warn("CHECKSUM MISMATCH expected: {0}, got: {1}",
                                                             checksum, computed_checksum);
                        }*/

                        //TODO: remove hardcoded by adding schema
                        if (header->intermediateFormat == 1) {
                            Utils::shortLineitem sl = {-2, -2, -2, -2, -2, -2, -2, -2};
                            std::memcpy(decompressed_buffer.data(), &sl, sizeof(sl));
                        } else if (header->intermediateFormat == 2) {
                            int m2 = -2;
                            int bs = _xdbcenv->buffer_size;
                            std::memcpy(decompressed_buffer.data(), &m2, 4);
                            std::memcpy(decompressed_buffer.data() + bs * 4, &m2, 4);
                            std::memcpy(decompressed_buffer.data() + bs * 4 * 2, &m2, 4);
                            std::memcpy(decompressed_buffer.data() + bs * 4 * 3, &m2, 4);
                            std::memcpy(decompressed_buffer.data() + bs * 4 * 4, &m2, 8);
                            std::memcpy(decompressed_buffer.data() + bs * 16 + bs * 8, &m2, 8);
                            std::memcpy(decompressed_buffer.data() + bs * 16 + bs * 8 * 2, &m2, 8);
                            std::memcpy(decompressed_buffer.data() + bs * 16 + bs * 8 * 3, &m2, 8);

                        }

                        spdlog::get("XDBC.CLIENT")->warn(
                                "decompress error: header: comp: {0}, size: {1}",
                                header->compressionType, header->totalSize);

                    }

                    memcpy(_bufferPool[compBuffId].data(), decompressed_buffer.data(),
                           _xdbcenv->tuple_size * _xdbcenv->buffer_size);

                } else if (header->compressionType == 0) {

                    memmove(_bufferPool[compBuffId].data(), compressed_buffer, header->totalSize);
                }

                auto duration_microseconds = std::chrono::duration_cast<std::chrono::microseconds>(
                        std::chrono::high_resolution_clock::now() - start).count();
                _xdbcenv->decomp_time.fetch_add(duration_microseconds, std::memory_order_relaxed);

                _xdbcenv->decompressedBufferIds[readThrId]->push(compBuffId);

                readThrId++;
                if (readThrId == _xdbcenv->read_parallelism)
                    readThrId = 0;
            }
        }

        for (int i = 0; i < _xdbcenv->read_parallelism; i++)
            _xdbcenv->decompressedBufferIds[i]->push(-1);

        spdlog::get("XDBC.CLIENT")->warn("Decomp thread {0} finished", thr);
        spdlog::get("XDBC.CLIENT.FILE")->info("Decompress data");
    }

    //TODO: handle parallelism internally
    bool XClient::hasNext(int readThreadId) {

        if (_emptyDecompThreadCtr[readThreadId] == _xdbcenv->decomp_parallelism)
            return false;

        return true;
    }

    //TODO: handle parallelism internally
    buffWithId XClient::getBuffer(int readThreadId) {

        int buffId = _xdbcenv->decompressedBufferIds[readThreadId]->pop();

        if (buffId == -1)
            _emptyDecompThreadCtr[readThreadId]++;

        buffWithId curBuf{};
        curBuf.buff.resize(_xdbcenv->buffer_size * _xdbcenv->tuple_size);
        curBuf.id = buffId;

        if (buffId > -1) {
            curBuf.buff = _bufferPool[buffId];
            //TODO: set intermediate format dynamically
            curBuf.iformat = _xdbcenv->iformat;
            //std::copy(std::begin(_bufferPool[i]), std::end(_bufferPool[i]), std::begin(curBuf.buff));
        }


        return curBuf;
    }

    int XClient::getBufferPoolSize() const {
        return _xdbcenv->bufferpool_size;
    }

    void XClient::markBufferAsRead(int buffId) {
        //TODO: ensure equal distribution
        //spdlog::get("XDBC.CLIENT")->warn("freeing {0}", _markedFreeCounter % _xdbcenv->rcv_parallelism);
        _xdbcenv->freeBufferIds[_markedFreeCounter % _xdbcenv->rcv_parallelism]->push(buffId);
        _markedFreeCounter.fetch_add(1);
    }

}
