#ifndef XDBC_XCLIENT_H
#define XDBC_XCLIENT_H

#include <string>
#include <vector>
#include <array>
#include <atomic>
#include <thread>
#include <stack>
#include <boost/asio.hpp>
#include <set>
#include "customQueue.h"
#include "utils.h"

using namespace boost::asio;
using ip::tcp;

namespace xdbc {

    constexpr size_t MAX_ATTRIBUTES = 230;
    struct Header {

        size_t compressionType;
        size_t totalSize;
        size_t totalTuples;
        size_t intermediateFormat;
        size_t crc;
        size_t attributeSize[MAX_ATTRIBUTES];
        size_t attributeComp[MAX_ATTRIBUTES];

    };

    struct SchemaAttribute {
        std::string name;
        std::string tpe;
        int size;
    };

    struct ProfilingTimestamps {
        std::chrono::high_resolution_clock::time_point timestamp;
        int thread;
        std::string component;
        std::string event;
    };

    typedef std::shared_ptr<customQueue<int>> FBQ_ptr;
    typedef std::shared_ptr<customQueue<ProfilingTimestamps>> PTQ_ptr;

    struct RuntimeEnv {
        long transfer_id;
        std::string env_name;
        int buffers_in_bufferpool;
        int buffer_size;
        int tuples_per_buffer;
        int tuple_size;
        int iformat;
        std::chrono::milliseconds sleep_time;
        int rcv_parallelism;
        int decomp_parallelism;
        int write_parallelism;
        std::chrono::steady_clock::time_point startTime;
        std::string table;
        std::string server_host;
        std::string server_port;
        std::vector<SchemaAttribute> schema;
        std::string schemaJSON;
        FBQ_ptr freeBufferIds;
        FBQ_ptr compressedBufferIds;
        FBQ_ptr decompressedBufferIds;
        int mode;
        std::vector<std::tuple<long long, size_t, size_t, size_t>> queueSizes;
        std::atomic<bool> monitor;

        PTQ_ptr pts;
    };

    struct buffWithId {
        int id;
        int iformat;
        size_t totalTuples;
        std::byte *buff;
    };

    class XClient {
    private:

        RuntimeEnv *_xdbcenv;
        std::atomic<int> _readState;
        std::vector<std::vector<std::byte>> _bufferPool;
        std::vector<std::atomic<bool>> _consumedAll;
        std::atomic<int> _totalBuffersRead;
        std::vector<std::thread> _rcvThreads;
        std::vector<std::thread> _decompThreads;
        std::vector<ip::tcp::socket> _readSockets;
        boost::asio::io_context _ioContext;
        boost::asio::ip::tcp::socket _baseSocket;
        int _emptyDecompThreadCtr;
        std::atomic<int> _markedFreeCounter;
        std::thread _monitorThread;

    public:
    XClient(RuntimeEnv &xdbcenv);
 /* 
    XClient::XClient(RuntimeEnv &env) :
        Purpose: Initializes an `XClient` object with configuration and resources from the provided `RuntimeEnv`.
        Input: `env` (RuntimeEnv): A reference to the `RuntimeEnv` object that contains the runtime configuration and environment settings.
        Output: Initializes various member variables, sets up logging, and prepares resources such as buffer pools and thread counters.
        Data Processing: The constructor initializes:
            - The `_xdbcenv` pointer to reference the provided `RuntimeEnv` object.
            - The `_bufferPool` to hold decompressed data buffers.
            - Preallocates vector sizes to `_decompThreads` and `_rcvThreads`, i.e. empty threads without any associated function.
            - The `_readSockets` for handling network sockets (although not fully defined in the constructor).
            - The `_emptyDecompThreadCtr` and `_markedFreeCounter` to manage thread states and buffer marking.
            - A `console_logger` is created using `spdlog` if not already available.
            - A `ProfilingTimestamps` pq`) is initialized and assigned to `env.pts`.
            - The buffer pool {2D pool(i.e. vector) of buffers} is populated with empty vectors sized to hold both headers and payloads, based on the configuration from `env`.
        Additional Notes: The constructor logs initialization details using `spdlog`, including buffer pool size, buffer size, tuple size, and intermediate format settings.
    */
    // Calls:
    // spdlog::get - retrieves logger for XDBC.CLIENT
    // spdlog::stdout_color_mt - creates a new logger if none exists
    // env.pts = pq - creates and assigns a new customQueue to env.pts
    // _bufferPool.resize - resizes the buffer pool with predefined size and vector elements. Resize is a member function of containers like vector for prealocation
    // _emptyDecompThreadCtr[] initialization with zeros


    std::string get_name() const;
/*
 * Purpose: Retrieves the name of the XClient environment.
 * Input: None.
 * Output: Returns the environment name as a string.
 * Process: Fetches and returns the `env_name` from the `_xdbcenv` object.
 */
// Calls:
// _xdbcenv->env_name - returns the environment's name


    void receive(int threadno);
/*
 * Purpose: Handles receiving data from the server on a specific thread.
 * Input: The thread index (`thr`) to uniquely identify the thread.
 * Output: None.
 * Process:
 * - Establishes a socket connection to the server (specific to the thread index).
 * - Sends the thread index to the server to indicate which thread is handling the data.
 * - Reads header and body data from the server, handling potential errors.
 * - Processes the data (e.g., checks headers for correctness) and pushes it to the decompression queue.
 * - Tracks and logs profiling timestamps throughout the process.
 * - Closes the socket when finished and logs the number of buffers processed.
 */
// Function: receive
// Calls:
// - ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "rcv", "start"} - Pushes profiling timestamp indicating thread start
// - spdlog::get("XDBC.CLIENT")->info(...) - Logs thread initiation and connection status
// - boost::asio::ip::tcp::resolver - Resolves server address and port for connection
// - socket.connect - Attempts to connect to the server using resolved endpoint
// - boost::asio::write - Sends thread ID to the server
// - spdlog::get("XDBC.CLIENT")->warn(...) - Logs connection errors
// - _xdbcenv->freeBufferIds[thr]->pop - Retrieves buffer ID for data read
// - boost::asio::read - Reads header and body data from the server socket
// - spdlog::get("XDBC.CLIENT")->error(...) - Logs errors while reading data
// - _totalBuffersRead.fetch_add(1) - Tracks total buffers read by incrementing a counter
// - _xdbcenv->compressedBufferIds[decompThreadId]->push(bpi) - Pushes decompressed buffer ID for further processing
// - socket.close - Closes the socket connection to the server


    void decompress(int threadno);
/* 
    void XClient::decompress(int thr) {
        Purpose: Decompresses buffers received by a specific thread and processes them.
        Input: The thread ID (`thr`) specifying which decompression thread is calling the function.
        Output: Decompressed data is written back into the buffer pool for further processing.
        Data Processing: The function pops compressed buffer IDs, checks the compression type, decompresses the data (either via specific column decompression or general decompression), and writes the decompressed data into a buffer. The process involves handling different attribute types and error checking. Profiling timestamps are logged during various stages of decompression.
    }
    */
// Function: decompress
// Calls:
// - ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "decomp", "start"} - Pushes profiling timestamp indicating thread start
// - _xdbcenv->compressedBufferIds[thr]->pop - Retrieves compressed buffer ID for decompression
// - Decompressor::decompress_int_col - Decompresses integer columns from compressed data
// - Decompressor::decompress_double_col - Decompresses double columns from compressed data
// - memcpy - Copies decompressed data into buffer for transfer
// - spdlog::get("XDBC.CLIENT")->warn(...) - Logs decompression errors
// - _xdbcenv->decompressedBufferIds[readThrId]->push(compBuffId) - Pushes decompressed buffer ID for further processing
// - ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "decomp", "end"} - Pushes profiling timestamp indicating thread end


   void initialize(const std::string &tableName);
/*
 * Purpose: Initializes the base connection to the server and sends the table schema.
 * Input: The table name (`tableName`) to identify the target table.
 * Output: None.
 * Process: 
 * - Resolves the server's address and establishes a connection to the server using Boost.Asio.
 * - Sends the table name and schema data to the server.
 * - Reads the server's "ready" signal to confirm that it is prepared for communication.
 * - Handles retries if the connection fails.
 */
// Calls:
    // boost::asio::ip::tcp::resolver - resolves the server hostname and port
    // boost::asio::ip::tcp::endpoint - creates an endpoint for the server
    // _baseSocket.connect - attempts to connect to the server with retries
    // boost::asio::write - sends table name and schema data to the server
    // read_ - reads the server's response
        

    int startReceiving(const std::string &tableName);
/*
 * Purpose: Initializes the client to start receiving data from the server.
 * Input: A table name string (`tableName`) to identify the target table.
 * Output: Returns 1 if successful, indicating that receiving is initialized.
 * Process: 
 * - Calls the `initialize()` function to establish a base connection with the server.
 * - Starts a monitor thread to track queue sizes at intervals.
 * - Creates and starts multiple threads to handle receiving, decompression, and writing operations.
 * - Sets up necessary buffer pools for each thread to operate on.
 */
// Calls:
// XClient::initialize - initializes the base socket and establishes connection
// std::thread(&XClient::monitorQueues) - starts the monitor thread
// _xdbcenv->freeBufferIds.push_back - pushes new queues to freeBufferIds
// _rcvThreads[i] = std::thread(&XClient::receive) - starts receive threads
// _decompThreads[i] = std::thread(&XClient::decompress) - starts decompression threads

        
        bool hasNext(int readThread);
/* 
    bool XClient::hasNext(int readThreadId) {
        Purpose: Checks if there are more buffers available for processing by the specified read thread.
        Input: The read thread ID (`readThreadId`).
        Output: Returns `true` if there are still decompressed buffers for the read thread to process, `false` otherwise.
        Data Processing: Compares the number of empty decompression threads with the total parallelism to decide if the read thread can continue fetching buffers.
    }
    */
// Function: hasNext
// Calls:
// - _xdbcenv->decomp_parallelism - Checks if all decompression threads are finished


        
        buffWithId getBuffer(int readThread);
    /* 
    buffWithId XClient::getBuffer(int readThreadId) {
        Purpose: Fetches the next decompressed buffer for a specific read thread.
        Input: The read thread ID (`readThreadId`).
        Output: Returns a `buffWithId` structure containing the buffer data, buffer ID, total number of tuples, and intermediate format.
        Data Processing: Pops a decompressed buffer from the buffer pool, retrieves its data (including total tuples), and prepares it for the read thread. It also sets the intermediate format for the buffer.
    }
    */
// Function: getBuffer
// Calls:
// - _xdbcenv->decompressedBufferIds[readThreadId]->pop - Retrieves next decompressed buffer ID
// - ProfilingTimestamps{std::chrono::high_resolution_clock::now(), readThreadId, "write", "pop"} - Pushes profiling timestamp for buffer retrieval
// - std::memcpy - Copies total tuples from buffer
// - spdlog::get("XDBC.CLIENT")->warn(...) - Logs buffer allocation


        int getBufferPoolSize() const;
    /* 
    int XClient::getBufferPoolSize() const {
        Purpose: Returns the total size of the buffer pool.
        Input: None.
        Output: The total number of buffers in the buffer pool.
        Data Processing: Retrieves the value of `_xdbcenv->buffers_in_bufferpool` to return the size of the buffer pool.
    }
    */
// Function: getBufferPoolSize
// Calls:
// - _xdbcenv->buffers_in_bufferpool - Retrieves the number of buffers in the buffer pool



        void finalize();
        /*
 * Purpose: Cleans up and finalizes the client by shutting down threads and closing connections.
 * Input: None.
 * Output: None.
 * Process: 
 * - Logs the finalization of the client with thread counts.
 * - Joins all active threads (receive, decompress) to ensure all tasks complete.
 * - Closes the base socket connection.
 * - Logs the total elapsed time for the client run.
 * - Collects profiling timestamps, calculates component metrics (e.g., receive, decompress, write times), 
 *   and formats them into strings.
 * - Writes performance metrics to a CSV file.
 */
    // Calls:
    // spdlog::get - logs to the XDBC.CLIENT logger
    // _monitorThread.join - waits for the monitor thread to finish
    // _decompThreads[i].join - joins all decompression threads
    // _rcvThreads[i].join - joins all receiving threads
    // _baseSocket.close - closes the base socket
    // std::chrono::steady_clock::now - gets the current timestamp
    // std::chrono::duration_cast - calculates elapsed time in milliseconds
    // calculate_metrics - calculates metrics based on profiling timestamps
    // printAndReturnAverageLoad - retrieves average load for components
    // std::ofstream - writes profiling data to a CSV file


        void markBufferAsRead(int buffId);
/* 
    void XClient::markBufferAsRead(int buffId) {
        Purpose: Marks a buffer as read and frees it for reuse by other threads.
        Input: The buffer ID (`buffId`) to be marked as read.
        Output: None (the buffer is freed for reuse).
        Data Processing: Pushes the buffer ID onto a free buffer queue (`freeBufferIds`) for reuse and increments a counter to ensure buffer distribution among threads.
    }
    */
// Function: markBufferAsRead
// Calls:
// - _xdbcenv->freeBufferIds[_markedFreeCounter % _xdbcenv->rcv_parallelism]->push(buffId) - Marks buffer as available for re-use
// - _markedFreeCounter.fetch_add(1) - Increments the counter for marked buffers


        void monitorQueues(int interval_ms);
/*
 * Purpose: Monitors and logs the size of various queues (free, compressed, decompressed) periodically.
 * Input: Interval in milliseconds (`interval_ms`) to check queue sizes.
 * Output: None.
 * Process:
 * - Continuously monitors the size of the free, compressed, and decompressed buffers.
 * - Logs the size of the queues at regular intervals.
 * - Stores queue size data as a tuple for later analysis.
 */
    // Calls:
    // std::chrono::high_resolution_clock::now - gets current timestamp
    // std::chrono::duration_cast - calculates current time in milliseconds
    // _xdbcenv->freeBufferIds.size() - calculates total size of free buffer queue
    // _xdbcenv->compressedBufferIds.size() - calculates total size of compressed buffer queue
    // _xdbcenv->decompressedBufferIds.size() - calculates total size of decompressed buffer queue
    // std::this_thread::sleep_for - puts the thread to sleep for the specified interval


    };

}

#endif //XDBC_XCLIENT_H
