//
// Created by harry on 2/6/23.
//

#include "xclient.h"
#include <iostream>
#include <boost/asio.hpp>
#include <thread>
#include <chrono>
#include <algorithm>
#include <iterator>

using namespace std;

namespace xdbc {

    void XClient::finalize() {
        cout << "Finalizing XClient: " << _name << endl;
        _finishedReading = true;
    }

    XClient::XClient(std::string name) : _name(name), _bufferPool(), _flagArray(), _finishedTransfer(false),
                                         _startedTransfer(false), _finishedReading(false) {

        cout << _name << endl;

        cout << "BUFFERPOOL SIZE: " << BUFFERPOOL_SIZE << endl;
        _bufferPool.resize(BUFFERPOOL_SIZE);
        for (int &i: _flagArray) {
            i = 1;

        }

    }


    std::string XClient::get_name() const {
        return _name;
    }

    void XClient::printSl(shortLineitem *t) {
        cout << t->l_orderkey << " | "
             << t->l_partkey << " | "
             << t->l_suppkey << " | "
             << t->l_linenumber << " | "
             << t->l_quantity << " | "
             << t->l_extendedprice << " | "
             << t->l_discount << " | "
             << t->l_tax
             << endl;
    }

    thread XClient::startReceiving(std::string tableName) {
        thread t1(&XClient::receive, this, tableName);
        while (!_startedTransfer)
            std::this_thread::sleep_for(SLEEP_TIME);

        cout << "#3 Initialized" << endl;
        return t1;
    }

    void XClient::receive(std::string tableName) {
        using namespace boost::asio;
        using ip::tcp;

        boost::asio::io_service io_service;
        //socket creation
        ip::tcp::socket socket(io_service);
        //connection
        socket.connect(tcp::endpoint(boost::asio::ip::address::from_string("127.0.0.1"), 1234));

        const std::string msg = tableName + "\n";
        boost::system::error_code error;
        boost::asio::write(socket, boost::asio::buffer(msg), error);

        int bpi = 0;
        int buffers = 0;

        cout << "#2 Entered receive thread" << endl;

        //while (buffers < TOTAL_TUPLES / BUFFER_SIZE) {
        while (true) {
            //cout << "Reading" << endl;

            int loops = 0;
            while (_flagArray[bpi] == 0) {
                bpi++;
                if (bpi == BUFFERPOOL_SIZE) {
                    bpi = 0;
                    loops++;
                    if (loops == 1000000) {
                        loops = 0;
                        cout << "stuck in receive" << endl;
                        std::this_thread::sleep_for(SLEEP_TIME);
                    }
                }
            }

            // getting response from server

            boost::asio::read(socket, boost::asio::buffer(_bufferPool[bpi]),
                              boost::asio::transfer_exactly(BUFFER_SIZE * TUPLE_SIZE), error);
            /*boost::asio::read(socket, boost::asio::buffer(_bufferPool[bpi]),
                              boost::asio::transfer_all(), error);*/

            _startedTransfer = true;

            /*for (auto x: _bufferPool[bpi]) {

                printSl(&x);
            }*/



            if (error == boost::asio::error::eof)
                break;
            _flagArray[bpi] = 0;
            buffers++;

            /* cout << "bpi: " << bpi << endl;
             cout << "flagidx " << (bpi / (BUFFER_SIZE * TUPLE_SIZE)) << endl;*/

        }
        cout << "finished transfer with #buffers: " << buffers << endl;
        _finishedTransfer = true;

    }


    buffWithId XClient::getBuffer() {

        int buffId = 0;
        int loops = 0;
        while (_flagArray[buffId] == 1 && !_finishedReading) {
            buffId++;
            if (buffId == BUFFERPOOL_SIZE) {
                buffId = 0;
                loops++;
                if (loops == 100000) {
                    loops = 0;
                    cout << "stuck in getBuffer" << endl;
                    std::this_thread::sleep_for(SLEEP_TIME);
                }
            }
        }

        buffWithId curBuf{};
        curBuf.id = buffId;
        curBuf.buff = _bufferPool[buffId];
        //std::copy(std::begin(_bufferPool[i]), std::end(_bufferPool[i]), std::begin(curBuf.buff));

        return curBuf;
    }

    void XClient::markBufferAsRead(int bufferId) {
        _flagArray[bufferId] = 1;
    }

    int XClient::getBufferPoolSize() {
        return BUFFERPOOL_SIZE;
    }

    bool XClient::hasUnread() {

        if (!_finishedTransfer)
            return true;
        for (int i: _flagArray) {
            if (i == 0 && !_finishedTransfer)
                return true;
            else if (_finishedTransfer)
                return false;
        }
        return false;
    }


}
