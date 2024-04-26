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
#include "queue.h"
#include "utils.h"
#include "env_predictor.h"

using namespace boost::asio;
using ip::tcp;

namespace xdbc {

    constexpr size_t MAX_ATTRIBUTES = 230;
    struct Header {

        size_t compressionType;
        size_t totalSize;
        size_t intermediateFormat;
        size_t crc;
        size_t attributeSize[MAX_ATTRIBUTES];
        size_t attributeComp[MAX_ATTRIBUTES];

    };
    typedef std::shared_ptr<queue<int>> FBQ_ptr;

    struct SchemaAttribute {
        std::string name;
        std::string tpe;
        int size;
    };

    struct RuntimeEnv {
        long transfer_id;
        std::string env_name;
        int bufferpool_size;
        int buffer_size;
        int tuple_size;
        int iformat;
        std::chrono::milliseconds sleep_time;
        int rcv_parallelism;
        int decomp_parallelism;
        int write_parallelism;
        std::atomic<std::chrono::time_point<std::chrono::high_resolution_clock>> start_rcv_time;
        std::atomic<std::chrono::time_point<std::chrono::high_resolution_clock>> start_decomp_time;
        std::atomic<long long> rcv_wait_time;
        std::atomic<long long> decomp_wait_time;
        std::atomic<long long> write_wait_time;
        std::atomic<long long> rcv_time;
        std::atomic<long long> decomp_time;
        std::atomic<long long> write_time;
        std::string table;
        std::string server_host;
        std::string server_port;
        std::vector<SchemaAttribute> schema;
        std::vector<FBQ_ptr> freeBufferIds;
        std::vector<FBQ_ptr> compressedBufferIds;
        std::vector<FBQ_ptr> decompressedBufferIds;
        int mode;
    };

    struct buffWithId {
        int id;
        int iformat;
        std::vector<std::byte> buff;
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
        std::vector<int> _emptyDecompThreadCtr;
        std::atomic<int> _outThreadId;
        std::atomic<int> _markedFreeCounter{};

    public:

        XClient(RuntimeEnv &xdbcenv);

        ~XClient();

        std::string get_name() const;

        void receive(int threadno);

        void decompress(int threadno);

        void initialize(const std::string &tableName);

        int startReceiving(const std::string &tableName);

        bool hasNext(int readThread);

        buffWithId getBuffer(int readThread);

        int getBufferPoolSize() const;

        void finalize();

        void markBufferAsRead(int buffId);
    };

}

#endif //XDBC_XCLIENT_H
