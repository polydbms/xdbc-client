#ifndef XDBC_XCLIENT_H
#define XDBC_XCLIENT_H

#include <string>
#include <vector>
#include <array>
#include <atomic>
#include <thread>
#include <stack>
#include <boost/asio.hpp>

using namespace boost::asio;
using ip::tcp;

namespace xdbc {

    struct RuntimeEnv {
        std::string env_name;
        int bufferpool_size;
        int buffer_size;
        int tuple_size;
        int iformat;
        std::chrono::milliseconds sleep_time;
        int parallelism;
        std::string table;
    };

    struct shortLineitem {
        int l_orderkey;
        int l_partkey;
        int l_suppkey;
        int l_linenumber;
        double l_quantity;
        double l_extendedprice;
        double l_discount;
        double l_tax;
    };

    struct buffWithId {
        int id;
        int iformat;
        std::vector<std::byte> buff;
    };

    class XClient {
    private:

        RuntimeEnv _xdbcenv;
        std::vector<std::atomic<int>> _flagArray;
        std::atomic<int> _readState;
        std::vector<std::vector<std::byte>> _bufferPool;
        std::vector<std::atomic<bool>> _finishedTransfer;
        std::vector<std::atomic<bool>> _startedTransfer;
        std::atomic<int> _totalBuffersRead;
        std::vector<std::thread> _readThreads;
        std::vector<ip::tcp::socket> _readSockets;
        boost::asio::io_context _ioContext;
        boost::asio::ip::tcp::socket _baseSocket;


    public:

        explicit XClient(const RuntimeEnv &xdbcenv);

        std::string get_name() const;

        void receive(int threadno);

        ip::tcp::socket initialize(const std::string &tableName);

        int startReceiving(const std::string &tableName);

        bool hasUnread();

        buffWithId getBuffer();

        void markBufferAsRead(int bufferId);

        int getBufferPoolSize();

        void printSl(shortLineitem *t);

        std::string slStr(shortLineitem *t);

        void finalize();

        bool emptyFlagBuffs();

        bool tFinishedTransfer();

        bool tStartedTransfer();
    };

}

#endif //XDBC_XCLIENT_H
