#ifndef XDBC_XCLIENT_H
#define XDBC_XCLIENT_H

#include <string>
#include <vector>
#include <array>
#include <atomic>
#include <thread>
#include <stack>

#define SLEEP_TIME 10ms

namespace xdbc {

    struct RuntimeEnv {
        std::string env_name;
        int bufferpool_size;
        int buffer_size;
        int tuple_size;
        int iformat;
        int sleep_time;
        int parallelism;
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

        std::string _name;
        RuntimeEnv _xdbcenv;
        std::vector<std::atomic<int>> _flagArray;
        std::atomic<int> _readState;
        std::vector<std::vector<std::byte>> _bufferPool;
        std::atomic<bool> _finishedTransfer;
        std::atomic<bool> _startedTransfer;
        std::atomic<bool> _finishedReading;
        std::atomic<int> _totalBuffersRead;


    public:

        explicit XClient(const RuntimeEnv &xdbcenv);

        std::string get_name() const;

        void receive(const std::string &tableName);

        std::thread startReceiving(std::string tableName);

        bool hasUnread();

        buffWithId getBuffer();

        void markBufferAsRead(int bufferId);

        int getBufferPoolSize();

        void printSl(shortLineitem *t);

        std::string slStr(shortLineitem *t);

        void finalize();

        bool emptyFlagBuffs();
    };

}

#endif //XDBC_XCLIENT_H
