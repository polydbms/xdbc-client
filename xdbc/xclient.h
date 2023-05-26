#ifndef XDBC_XCLIENT_H
#define XDBC_XCLIENT_H

#include <string>
#include <vector>
#include <array>
#include <atomic>
#include <thread>
#include <stack>

#define TOTAL_TUPLES 10000000
#define BUFFER_SIZE 1000
#define BUFFERPOOL_SIZE 1000
#define TUPLE_SIZE 48
#define SLEEP_TIME 10ms

namespace xdbc {

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
        std::atomic<int> _flagArray[BUFFERPOOL_SIZE];
        std::atomic<int> _readState;
        std::vector<std::vector<std::byte>> _bufferPool;
        std::atomic<bool> _finishedTransfer;
        std::atomic<bool> _startedTransfer;
        std::atomic<bool> _finishedReading;
        std::atomic<int> _totalBuffersRead;


    public:

        XClient(std::string name);

        std::string get_name() const;

        void receive(std::string tableName);

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
