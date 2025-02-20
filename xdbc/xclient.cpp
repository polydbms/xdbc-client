#include "xclient.h"
#include <boost/asio.hpp>
#include <boost/asio/deadline_timer.hpp>
#include <thread>
#include <chrono>
#include <algorithm>
#include <iterator>
#include <fstream>
#include <iomanip>
#include "spdlog/spdlog.h"
#include "spdlog/stopwatch.h"
#include "spdlog/sinks/stdout_color_sinks.h"
#include "Decompression/Decompressor.h"
#include "xdbc/metrics_calculator.h"

using namespace boost::asio;
using ip::tcp;

namespace xdbc
{

    XClient::XClient(RuntimeEnv &env) : _xdbcenv(&env),
                                        _bufferPool(),
                                        _totalBuffersRead(0),
                                        _decompThreads(env.decomp_parallelism),
                                        _rcvThreads(env.rcv_parallelism),
                                        _serThreads(env.ser_parallelism),
                                        _writeThreads(env.write_parallelism),
                                        _readSockets(),
                                        //_emptyDecompThreadCtr(env.write_parallelism),
                                        _markedFreeCounter(0),
                                        _emptyDecompThreadCtr(0),
                                        _baseSocket(_ioContext)
    {

        auto console_logger = spdlog::get("XDBC.CLIENT");

        if (!console_logger)
        {
            // Logger does not exist, create it
            console_logger = spdlog::stdout_color_mt("XDBC.CLIENT");
        }

        PTQ_ptr pq(new customQueue<ProfilingTimestamps>);
        env.pts = pq;

        spdlog::get("XDBC.CLIENT")->info("Creating Client: {0}, BPS: {1}, BS: {2} KiB, TS: {3} bytes, iformat: {4} ", _xdbcenv->env_name, env.buffers_in_bufferpool, env.buffer_size, env.tuple_size, env.iformat);

        // populate bufferpool with empty vectors (header + payload)
        _bufferPool.resize(env.buffers_in_bufferpool,
                           std::vector<std::byte>(sizeof(Header) + env.tuples_per_buffer * env.tuple_size));

        _xdbcenv->bp = &_bufferPool;
        // calculate buffers per queue
        int total_workers = _xdbcenv->rcv_parallelism + _xdbcenv->decomp_parallelism +
                            _xdbcenv->ser_parallelism + _xdbcenv->write_parallelism;

        // TODO: check and increase bufferpool size if necessary or exit
        int available_buffers_for_queues = _xdbcenv->buffers_in_bufferpool - total_workers;

        if (_xdbcenv->buffers_in_bufferpool < total_workers ||
            available_buffers_for_queues < total_workers)
        {

            spdlog::get("XDBC.CLIENT")->error("Buffer allocation error: Total buffers: {0}. "
                                              "\nRequired buffers:  Total: {1},"
                                              "\nAvailable for queues: {2}. "
                                              "\nIncrease the buffer pool size to at least {1}.",
                                              _xdbcenv->buffers_in_bufferpool, total_workers, available_buffers_for_queues);
        }

        int queueCapacityPerComp = available_buffers_for_queues / 4;
        int serQueueCapacity = queueCapacityPerComp + available_buffers_for_queues % 4;

        // Unified receive queue
        _xdbcenv->freeBufferIds = std::make_shared<customQueue<int>>();
        // Unified decompression queue
        _xdbcenv->compressedBufferIds = std::make_shared<customQueue<int>>();
        _xdbcenv->compressedBufferIds->setCapacity(queueCapacityPerComp);
        _xdbcenv->finishedRcvThreads.store(0);

        // Unified decompression queue
        _xdbcenv->decompressedBufferIds = std::make_shared<customQueue<int>>();
        _xdbcenv->decompressedBufferIds->setCapacity(queueCapacityPerComp);
        _xdbcenv->finishedDecompThreads.store(0);

        _xdbcenv->serializedBufferIds->setCapacity(serQueueCapacity);
        _xdbcenv->finishedSerializerThreads.store(0);
        _xdbcenv->finishedWriteThreads.store(0);

        // Initially populate the freeBufferIds (receive) queue with all buffer IDs
        for (int i = 0; i < env.buffers_in_bufferpool; ++i)
        {
            _xdbcenv->freeBufferIds->push(i);
        }

        spdlog::get("XDBC.CLIENT")->info("Initialized queues, "
                                         "freeBuffersQ: {0}, "
                                         "compQ: {1}, "
                                         "decompQ: {1}, "
                                         "serQ: {2} ",
                                         env.buffers_in_bufferpool, queueCapacityPerComp, serQueueCapacity);
    }

    void XClient::finalize()
    {

        spdlog::get("XDBC.CLIENT")->info("Finalizing XClient: {0}, shutting down {1} receive threads & {2} decomp threads", _xdbcenv->env_name, _xdbcenv->rcv_parallelism, _xdbcenv->decomp_parallelism);

        _xdbcenv->monitor.store(false);
        _monitorThread.join();

        for (int i = 0; i < _xdbcenv->decomp_parallelism; i++)
        {
            _decompThreads[i].join();
        }

        for (int i = 0; i < _xdbcenv->rcv_parallelism; i++)
        {
            _rcvThreads[i].join();
        }

        _baseSocket.close();
        spdlog::get("XDBC.CLIENT")->info("Finalizing: basesocket closed");

        auto end = std::chrono::steady_clock::now();
        auto total_time = std::chrono::duration_cast<std::chrono::milliseconds>(end - _xdbcenv->startTime).count();
        spdlog::get("XDBC.CLIENT")->info("Total elapsed time: {0} ms", total_time);

        auto pts = std::vector<xdbc::ProfilingTimestamps>(_xdbcenv->pts->size());
        while (_xdbcenv->pts->size() != 0)
            pts.push_back(_xdbcenv->pts->pop());

        auto component_metrics = calculate_metrics(pts, _xdbcenv->buffer_size);
        std::ostringstream totalTimes;
        std::ostringstream procTimes;
        std::ostringstream waitingTimes;
        std::ostringstream totalThroughput;
        std::ostringstream perBufferThroughput;

        for (const auto &[component, metrics] : component_metrics)
        {

            if (!component.empty())
            {
                totalTimes << component << ":\t" << metrics.overall_time_ms << "ms, ";
                procTimes << component << ":\t" << metrics.processing_time_ms << "ms, ";
                waitingTimes << component << ":\t" << metrics.waiting_time_ms << "ms, ";
                totalThroughput << component << ":\t" << metrics.total_throughput << "mb/s, ";
                perBufferThroughput << component << ":\t" << metrics.per_buffer_throughput << "mb/s, ";
            }
        }

        spdlog::get("XDBC.CLIENT")->info("xdbc client | \n all:\t {} \n proc:\t{} \n wait:\t{} \n thr:\t {} \n thr/b:\t {}", totalTimes.str(), procTimes.str(), waitingTimes.str(), totalThroughput.str(), perBufferThroughput.str());

        auto loads = printAndReturnAverageLoad(*_xdbcenv);

        const std::string filename = "/tmp/xdbc_client_timings.csv";

        std::ostringstream headerStream;
        headerStream << "transfer_id,total_time,"
                     << "rcv_wait_time,rcv_proc_time,rcv_throughput,rcv_throughput_pb,free_load,"
                     << "decomp_wait_time,decomp_proc_time,decomp_throughput,decomp_throughput_pb,decomp_load,"
                     << "ser_wait_time,ser_proc_time,ser_throughput,ser_throughput_pb,ser_load,"
                     << "write_wait_time,write_proc_time,write_throughput,write_throughput_pb,write_load\n";

        std::ifstream file_check(filename);
        bool is_empty = file_check.peek() == std::ifstream::traits_type::eof();
        file_check.close();

        std::ofstream csv_file(filename,
                               std::ios::out | std::ios::app);

        if (is_empty)
            csv_file << headerStream.str();

        csv_file << std::fixed << std::setprecision(2)
                 << std::to_string(_xdbcenv->transfer_id) << "," << total_time << ","
                 << component_metrics["rcv"].waiting_time_ms << ","
                 << component_metrics["rcv"].processing_time_ms << ","
                 << component_metrics["rcv"].total_throughput << ","
                 << component_metrics["rcv"].per_buffer_throughput << ","
                 << std::get<0>(loads) << ","
                 << component_metrics["decomp"].waiting_time_ms << ","
                 << component_metrics["decomp"].processing_time_ms << ","
                 << component_metrics["decomp"].total_throughput << ","
                 << component_metrics["decomp"].per_buffer_throughput << ","
                 << std::get<1>(loads) << ","
                 << component_metrics["ser"].waiting_time_ms << ","
                 << component_metrics["ser"].processing_time_ms << ","
                 << component_metrics["ser"].total_throughput << ","
                 << component_metrics["ser"].per_buffer_throughput << ","
                 << std::get<2>(loads) << ","
                 << component_metrics["write"].waiting_time_ms << ","
                 << component_metrics["write"].processing_time_ms << ","
                 << component_metrics["write"].total_throughput << ","
                 << component_metrics["write"].per_buffer_throughput << ","
                 << std::get<3>(loads) << "\n";
        csv_file.close();
    }

    std::string XClient::get_name() const
    {
        return _xdbcenv->env_name;
    }

    std::string read_(tcp::socket &socket)
    {
        boost::asio::streambuf buf;
        boost::system::error_code error;
        size_t bytes = boost::asio::read_until(socket, buf, "\n", error);

        if (error)
        {
            spdlog::get("XDBC.CLIENT")->warn("Boost error while reading: {0} ", error.message());
        }
        std::string data = boost::asio::buffer_cast<const char *>(buf.data());
        return data;
    }

    int XClient::startReceiving(const std::string &tableName)
    {

        // establish base connection with server
        XClient::initialize(tableName);

        _xdbcenv->monitor.store(true);

        _monitorThread = std::thread(&XClient::monitorQueues, this, _xdbcenv->profilingInterval);

        // create rcv threads
        for (int i = 0; i < _xdbcenv->rcv_parallelism; i++)
        {
            _rcvThreads[i] = std::thread(&XClient::receive, this, i);
        }

        // create decomp threads
        for (int i = 0; i < _xdbcenv->decomp_parallelism; i++)
        {
            _decompThreads[i] = std::thread(&XClient::decompress, this, i);
        }

        spdlog::get("XDBC.CLIENT")->info("Initialized receiver & decomp threads");

        return 1;
    }

    void XClient::monitorQueues(int interval_ms)
    {

        long long curTimeInterval = interval_ms / 1000;

        while (_xdbcenv->monitor)
        {
            auto now = std::chrono::high_resolution_clock::now();
            auto timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();

            // Calculate the total size of all queues in each category
            size_t freeBufferTotalSize = _xdbcenv->freeBufferIds->size();
            size_t compressedBufferTotalSize = _xdbcenv->compressedBufferIds->size();
            size_t decompressedBufferTotalSize = _xdbcenv->decompressedBufferIds->size();
            size_t serializedBufferTotalSize = _xdbcenv->serializedBufferIds->size();

            // size_t freeBufferTotalSize = 0;
            // for (auto &queue_ptr: _xdbcenv->freeBufferIds) {
            //     freeBufferTotalSize += queue_ptr->size();
            // }

            // size_t compressedBufferTotalSize = 0;
            // for (auto &queue_ptr: _xdbcenv->compressedBufferIds) {
            //     compressedBufferTotalSize += queue_ptr->size();
            // }

            // size_t decompressedBufferTotalSize = 0;
            // for (auto &queue_ptr: _xdbcenv->decompressedBufferIds) {
            //     decompressedBufferTotalSize += queue_ptr->size();
            // }

            // Store the measurement as a tuple
            _xdbcenv->queueSizes.emplace_back(curTimeInterval, freeBufferTotalSize, compressedBufferTotalSize,
                                              decompressedBufferTotalSize, serializedBufferTotalSize);

            std::this_thread::sleep_for(std::chrono::milliseconds(interval_ms));
            curTimeInterval += interval_ms / 1000;
        }
    }

    void XClient::initialize(const std::string &tableName)
    {

        // this is for IP address
        /*boost::asio::io_service io_service;
        //socket creation
        ip::tcp::socket socket(io_service);
        socket.connect(tcp::endpoint(boost::asio::ip::address::from_string("127.0.0.1"), 1234));
         */

        // this is for hostname

        boost::asio::ip::tcp::resolver resolver(_ioContext);
        boost::asio::ip::tcp::resolver::query query(_xdbcenv->server_host, _xdbcenv->server_port);
        boost::asio::ip::tcp::resolver::iterator iter = resolver.resolve(query);
        boost::asio::ip::tcp::endpoint endpoint = iter->endpoint();

        spdlog::get("XDBC.CLIENT")->info("Basesocket: trying to connect");

        boost::system::error_code ec;
        _baseSocket.connect(endpoint, ec);

        int tries = 0;
        while (ec && tries < 3)
        {
            spdlog::get("XDBC.CLIENT")->warn("Basesocket not connecting, trying to reconnect...");
            tries++;
            _baseSocket.close();
            std::this_thread::sleep_for(_xdbcenv->sleep_time * 10);
            _baseSocket.connect(endpoint, ec);
        }

        if (ec)
        {
            spdlog::get("XDBC.CLIENT")->error("Failed to connect after retries: {0}", ec.message());
            throw boost::system::system_error(ec); // Explicitly throw if connection fails
        }

        spdlog::get("XDBC.CLIENT")->info("Basesocket: connected to {0}:{1}", endpoint.address().to_string(), endpoint.port());

        boost::system::error_code error;
        const std::string &msg = tableName;
        std::uint32_t tableNameSize = msg.size();
        std::vector<boost::asio::const_buffer> tableNameBuffers;

        tableNameBuffers.emplace_back(boost::asio::buffer(&tableNameSize, sizeof(tableNameSize)));
        tableNameBuffers.emplace_back(boost::asio::buffer(msg));

        boost::asio::write(_baseSocket, tableNameBuffers, error);

        std::uint32_t data_size = _xdbcenv->schemaJSON.size();
        std::vector<boost::asio::const_buffer> buffers;
        buffers.emplace_back(boost::asio::buffer(&data_size, sizeof(data_size)));
        buffers.emplace_back(boost::asio::buffer(_xdbcenv->schemaJSON));

        boost::asio::write(_baseSocket, buffers, error);

        // std::this_thread::sleep_for(_xdbcenv->sleep_time*10);
        std::string ready = read_(_baseSocket);

        // TODO: make a check that server is actually ready and try again until ready
        // ready.erase(std::remove(ready.begin(), ready.end(), '\n'), ready.cend());
        spdlog::get("XDBC.CLIENT")->info("Basesocket: Server signaled: {0}", ready);

        // return socket;
    }

    void XClient::receive(int thr)
    {
        _xdbcenv->pts->push(ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "rcv", "start"});
        spdlog::get("XDBC.CLIENT")->info("Entered receive thread {0} ", thr);
        boost::asio::io_service io_service;
        ip::tcp::socket socket(io_service);
        boost::asio::ip::tcp::resolver resolver(io_service);
        boost::asio::ip::tcp::resolver::query query(_baseSocket.remote_endpoint().address().to_string(),
                                                    std::to_string(stoi(_xdbcenv->server_port) + thr + 1));
        boost::asio::ip::tcp::resolver::iterator iter = resolver.resolve(query);
        boost::asio::ip::tcp::endpoint endpoint = iter->endpoint();

        bool connected = false;

        try
        {
            socket.connect(endpoint);
            connected = true;
            spdlog::get("XDBC.CLIENT")->info("Receive thread {0} connected to {1}:{2}", thr, endpoint.address().to_string(), endpoint.port());
        }
        catch (const boost::system::system_error &error)
        {
            spdlog::get("XDBC.CLIENT")->warn("Server error: {0}", error.what());
            // std::this_thread::sleep_for(_xdbcenv->sleep_time);
        }

        if (connected)
        {

            const std::string msg = std::to_string(thr) + "\n";
            boost::system::error_code error;

            try
            {
                size_t b = boost::asio::write(socket, boost::asio::buffer(msg), error);
            }
            catch (const boost::system::system_error &e)
            {
                spdlog::get("XDBC.CLIENT")->warn("Could not write thread no, error: {0}", e.what());
            }

            int bpi;
            int buffers = 0;

            spdlog::get("XDBC.CLIENT")->info("Receive thread {0} started", thr);

            size_t headerBytes;
            size_t readBytes;

            while (error != boost::asio::error::eof)
            {

                bpi = _xdbcenv->freeBufferIds->pop();
                _xdbcenv->pts->push(ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "rcv", "pop"});
                // spdlog::get("XDBC.CLIENT")->info("Receive thread {0} got buff {1}", thr, bpi);

                // getting response from server, first the header
                headerBytes = boost::asio::read(socket, boost::asio::buffer(_bufferPool[bpi].data(), sizeof(Header)),
                                                boost::asio::transfer_exactly(sizeof(Header)), error);
                Header header = *reinterpret_cast<Header *>(_bufferPool[bpi].data());

                // TODO: handle error types (e.g., EOF)
                if (error || header.compressionType > 6 ||
                    header.totalSize > _xdbcenv->tuples_per_buffer * _xdbcenv->tuple_size)
                {

                    if (error)
                    {
                        spdlog::get("XDBC.CLIENT")->error("Receive thread {0}: boost error while reading header: {1}", thr, error.message());
                        if (error == boost::asio::error::eof)
                        {
                            spdlog::get("XDBC.CLIENT")->error("EOF");
                        }
                        break;
                    }

                    spdlog::get("XDBC.CLIENT")->error("Client: corrupt body: comp: {0}, size: {1}/{2}, headerbytes: {3}", header.compressionType, header.totalSize, _xdbcenv->tuples_per_buffer * _xdbcenv->tuple_size, headerBytes);
                }

                // all good, read incoming body and measure time
                readBytes = boost::asio::read(socket, boost::asio::buffer(_bufferPool[bpi].data() + sizeof(Header), header.totalSize),
                                              boost::asio::transfer_exactly(header.totalSize), error);

                // TODO: handle errors correctly
                if (error)
                {
                    spdlog::get("XDBC.CLIENT")->error("Client: boost error while reading body: readBytes {0}, error: {1}", readBytes, error.message());
                    if (error == boost::asio::error::eof)
                    {
                    }
                    break;
                }

                _totalBuffersRead.fetch_add(1);
                _xdbcenv->pts->push(ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "rcv", "push"});
                _xdbcenv->compressedBufferIds->push(bpi);

                buffers++;
            }

            _xdbcenv->finishedRcvThreads.fetch_add(1);
            if (_xdbcenv->finishedRcvThreads == _xdbcenv->rcv_parallelism)
            {
                for (int i = 0; i < _xdbcenv->decomp_parallelism; i++)
                    _xdbcenv->compressedBufferIds->push(-1);
            }
            socket.close();

            spdlog::get("XDBC.CLIENT")->info("Receive thread {0} finished, #buffers: {1}", thr, buffers);
        }
        else
            spdlog::get("XDBC.CLIENT")->error("Receive thread {0} could not connect", thr);

        _xdbcenv->pts->push(ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "rcv", "end"});
    }

    void XClient::decompress(int thr)
    {
        _xdbcenv->pts->push(ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "decomp", "start"});

        int decompError;
        int buffersDecompressed = 0;

        while (true)
        {

            int compBuffId = _xdbcenv->compressedBufferIds->pop();

            if (compBuffId == -1)
                break;
            _xdbcenv->pts->push(ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "decomp", "pop"});

            Header *header = reinterpret_cast<Header *>(_bufferPool[compBuffId].data());
            std::byte *compressed_buffer = _bufferPool[compBuffId].data() + sizeof(Header);
            // spdlog::get("XDBC.CLIENT")->info("decompress thread total tuples {}", header->totalTuples);

            // just forward buffer if not compressed
            if (header->compressionType == 0)
            {
                _xdbcenv->pts->push(
                    ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "decomp", "push"});
                _xdbcenv->decompressedBufferIds->push(compBuffId);
            }
            else if (header->compressionType > 0)
            {

                // we need a free buffer to decompress
                int decompBuffId = _xdbcenv->freeBufferIds->pop();

                // spdlog::get("XDBC.CLIENT")->info("Decompressor thr {} got free buff {}", thr, decompBuffId);

                auto &decompressed_buffer = _bufferPool[decompBuffId];

                // TODO: refactor decompress_cols with schema in Decompressor
                if (header->compressionType == 6)
                {

                    // TODO: decompress every column individually
                }
                else
                    decompError = Decompressor::decompress(header->compressionType,
                                                           decompressed_buffer.data() + sizeof(Header),
                                                           compressed_buffer, header->totalSize,
                                                           _xdbcenv->tuples_per_buffer * _xdbcenv->tuple_size);

                if (decompError == 1)
                {

                    // TODO: check error handling
                    spdlog::get("XDBC.CLIENT")->warn("decompress error: header: comp: {0}, size: {1}", header->compressionType, header->totalSize);

                    // since there was an error return both buffers
                    _xdbcenv->freeBufferIds->push(compBuffId);
                    _xdbcenv->freeBufferIds->push(decompBuffId);
                }
                else
                {

                    Header newHeader{};
                    newHeader.totalTuples = header->totalTuples;
                    newHeader.totalSize = header->uncompressedSize;
                    newHeader.intermediateFormat = header->intermediateFormat;

                    memcpy(decompressed_buffer.data(), &newHeader, sizeof(Header));
                    // spdlog::get("XDBC.CLIENT")->warn("read totalTuples: {}", header->totalTuples);

                    _xdbcenv->pts->push(
                        ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "decomp", "push"});

                    _xdbcenv->decompressedBufferIds->push(decompBuffId);
                    _xdbcenv->freeBufferIds->push(compBuffId);
                }
            }

            buffersDecompressed++;
        }

        _xdbcenv->finishedDecompThreads.fetch_add(1);
        if (_xdbcenv->finishedDecompThreads == _xdbcenv->decomp_parallelism)
        {
            for (int i = 0; i < _xdbcenv->ser_parallelism; i++)
                _xdbcenv->decompressedBufferIds->push(-1);
        }
        spdlog::get("XDBC.CLIENT")->warn("Decomp thread {0} finished, {1} buffers", thr, buffersDecompressed);
        _xdbcenv->pts->push(ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "decomp", "end"});
    }

    // TODO: handle parallelism internally
    bool XClient::hasNext(int readThreadId)
    {
        if (_emptyDecompThreadCtr == _xdbcenv->write_parallelism)
            return false;

        return true;
    }

    // TODO: handle parallelism internally
    buffWithId XClient::getBuffer(int readThreadId)
    {

        int buffId = _xdbcenv->decompressedBufferIds->pop();
        _xdbcenv->pts->push(
            ProfilingTimestamps{std::chrono::high_resolution_clock::now(), readThreadId, "write", "pop"});

        buffWithId curBuf{};
        if (buffId == -1)
            _emptyDecompThreadCtr.fetch_add(1);

        size_t totalTuples = 0;
        size_t totalSize = 0;

        if (buffId > -1)
        {
            auto header = reinterpret_cast<Header *>(_bufferPool[buffId].data());
            totalTuples = header->totalTuples;
            totalSize = header->totalSize;
            curBuf.buff = _bufferPool[buffId].data() + sizeof(Header);
        }
        curBuf.id = buffId;
        curBuf.totalTuples = totalTuples;
        curBuf.totalSize = totalSize;

        // TODO: set intermediate format dynamically
        curBuf.iformat = _xdbcenv->iformat;

        // spdlog::get("XDBC.CLIENT")->warn("Sending buffer {0} to read thread {1}", buffId, readThreadId);

        return curBuf;
    }

    int XClient::getBufferPoolSize() const
    {
        return _xdbcenv->buffers_in_bufferpool;
    }

    void XClient::markBufferAsRead(int buffId)
    {
        // TODO: ensure equal distribution
        // spdlog::get("XDBC.CLIENT")->warn("freeing {0} for {1}", buffId, _markedFreeCounter % _xdbcenv->rcv_parallelism);
        _xdbcenv->freeBufferIds->push(buffId);
        _markedFreeCounter.fetch_add(1);
    }

}
