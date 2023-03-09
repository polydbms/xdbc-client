#ifndef XDBC_XCLIENT_H
#define XDBC_XCLIENT_H

#include <string>
#include <vector>
#include <array>
#include <atomic>
#include <thread>

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
        std::array<shortLineitem, BUFFER_SIZE> buff;
    };

    class XClient {
    private:

        std::string _name;
        int _flagArray[BUFFERPOOL_SIZE];
        std::vector<std::array<shortLineitem, BUFFER_SIZE>> _bufferPool;
        std::atomic<bool> _finishedTransfer;
        std::atomic<bool> _startedTransfer;
        std::atomic<bool> _finishedReading;


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

        void finalize();
    };

}

#endif //XDBC_XCLIENT_H
