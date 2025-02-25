#ifndef XDBC_RUNTIMEENV_H
#define XDBC_RUNTIMEENV_H

#include "customQueue.h"
#include <vector>
#include <string>
#include <atomic>
#include <chrono>
#include <memory>
#include <numeric>
#include <sstream>

namespace xdbc {

    constexpr size_t MAX_ATTRIBUTES = 230;

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

    struct transfer_details {
        float elapsed_time = 0.0f;     // Default value for elapsed_time
        std::vector<int> bufProcessed; // Default value: vector with one element, 0
        std::tuple<size_t, size_t, size_t> latest_queueSizes;
    };

    class RuntimeEnv {
    public:
        // Public members for configuration and state
        std::vector<std::vector<std::byte>> *bp = nullptr;
        long transfer_id = 0;
        std::string env_name;
        int buffers_in_bufferpool = 0;
        int buffer_size = 0;
        int tuples_per_buffer = 0;
        int tuple_size = 0;
        int iformat = 0;
        std::chrono::milliseconds sleep_time = std::chrono::milliseconds(100);
        int rcv_parallelism = 1;
        int decomp_parallelism = 1;
        int ser_parallelism = 1;
        int write_parallelism = 1;
        bool skip_serializer = false;
        std::string target;

        std::chrono::steady_clock::time_point startTime;
        std::string table;
        std::string server_host;
        std::string server_port;
        std::vector<SchemaAttribute> schema;
        std::string schemaJSON;
        FBQ_ptr freeBufferIds = std::make_shared<customQueue<int>>();
        FBQ_ptr compressedBufferIds = std::make_shared<customQueue<int>>();
        FBQ_ptr decompressedBufferIds = std::make_shared<customQueue<int>>();
        FBQ_ptr serializedBufferIds = std::make_shared<customQueue<int>>();
        int mode = 0;
        std::vector<std::tuple<long long, size_t, size_t, size_t, size_t>> queueSizes;
        std::atomic<bool> monitor = false;
        int profilingInterval = 1000;
        std::atomic<int> finishedRcvThreads = 0;
        std::atomic<int> finishedDecompThreads = 0;
        std::atomic<int> finishedSerializerThreads = 0;
        std::atomic<int> finishedWriteThreads = 0;
        PTQ_ptr pts = std::make_shared<customQueue<ProfilingTimestamps>>();

        int spawn_source;
        transfer_details tf_paras;
        std::atomic<int> enable_updation;

        std::string toString() const {
            std::ostringstream oss;

            oss << "RuntimeEnv Configuration:\n";
            oss << "--------------------------\n";
            oss << "Environment Name: " << env_name << "\n";
            oss << "Transfer ID: " << transfer_id << "\n";
            oss << "Table: " << table << "\n";
            oss << "Server Host: " << server_host << "\n";
            oss << "Server Port: " << server_port << "\n";

            oss << "Buffers in Buffer Pool: " << buffers_in_bufferpool << "\n";
            oss << "Buffer Size: " << buffer_size << "\n";
            oss << "Tuples per Buffer: " << tuples_per_buffer << "\n";
            oss << "Tuple Size: " << tuple_size << "\n";
            oss << "Input Format: " << iformat << "\n";
            oss << "Sleep Time (ms): " << sleep_time.count() << "\n";

            oss << "Parallelism:\n";
            oss << "  Receive: " << rcv_parallelism << "\n";
            oss << "  Decompress: " << decomp_parallelism << "\n";
            oss << "  Serialize: " << ser_parallelism << "\n";
            oss << "  Write: " << write_parallelism << "\n";

            oss << "Buffer Queues:\n";
            oss << "  Free Buffer Queue Size: " << freeBufferIds->size() << "\n";
            oss << "  Compressed Buffer Queue Size: " << compressedBufferIds->size() << "\n";
            oss << "  Decompressed Buffer Queue Size: " << decompressedBufferIds->size() << "\n";
            oss << "  Serialized Buffer Queue Size: " << serializedBufferIds->size() << "\n";

            oss << "Finished Threads:\n";
            oss << "  Receive: " << finishedRcvThreads.load() << "\n";
            oss << "  Decompress: " << finishedDecompThreads.load() << "\n";
            oss << "  Serialize: " << finishedSerializerThreads.load() << "\n";
            oss << "  Write: " << finishedWriteThreads.load() << "\n";

            oss << "Schema JSON: " << schemaJSON << "\n";
            oss << "Mode: " << mode << "\n";

            oss << "--------------------------\n";

            return oss.str();
        }

        // Default constructor
        RuntimeEnv() = default;

        // Utility to calculate tuple size based on schema
        void calculateTupleSize() {
            tuple_size = std::accumulate(schema.begin(), schema.end(), 0,
                                         [](int acc, const SchemaAttribute &attr) {
                                             return acc + attr.size;
                                         });
            tuples_per_buffer = (buffer_size * 1024 / tuple_size);
        }
    };

    typedef std::shared_ptr<customQueue<int>> FBQ_ptr;
    typedef std::shared_ptr<customQueue<ProfilingTimestamps>> PTQ_ptr;

    struct Header {

        size_t compressionType;
        size_t totalSize;
        size_t totalTuples;
        size_t intermediateFormat;
        size_t uncompressedSize;
        size_t crc;
        size_t attributeSize[MAX_ATTRIBUTES];
        size_t attributeComp[MAX_ATTRIBUTES];
    };
} // namespace xdbc

#endif // XDBC_RUNTIMEENV_H
