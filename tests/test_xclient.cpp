#include <iostream>
#include <thread>
#include "../xdbc/xclient.h"
#include <iomanip>
#include "spdlog/spdlog.h"
#include "spdlog/sinks/stdout_color_sinks.h"

//#define BUFFERPOOL_SIZE 1000
#define TOTAL_TUPLES 10000000

using namespace std;

int main() {

    xdbc::XClient c("cpp Client");

    auto console = spdlog::stdout_color_mt("XCLIENT");

    spdlog::get("XCLIENT")->info("#1 Constructed XClient called: {0}", c.get_name());

    thread t1 = c.startReceiving("test_10000000");


    int min = INT32_MAX;
    int max = INT32_MIN;
    long sum = 0;
    long cnt = 0;
    long totalcnt = 0;

    spdlog::get("XCLIENT")->info("#4 called receive");

    auto start = std::chrono::steady_clock::now();

    int buffsRead = 0;
    while (c.hasUnread()) {
        xdbc::buffWithId curBuffWithId = c.getBuffer();
        //cout << "Iteration at tuple:" << cnt << " and buffer " << buffsRead << endl;
        if (curBuffWithId.id >= 0) {
            if (curBuffWithId.iformat == 1) {
                auto *ptr = reinterpret_cast<xdbc::shortLineitem *>(curBuffWithId.buff.data());
                std::vector<xdbc::shortLineitem> sls(ptr, ptr + BUFFER_SIZE);
                for (auto sl: sls) {
                    totalcnt++;
                    //cout << "Buffer with Id: " << curBuffWithId.id << " l_orderkey: " << sl.l_orderkey << endl;
                    if (sl.l_orderkey < 0) {
                        spdlog::get("XCLIENT")->warn("Empty tuple at buffer: {0}, tuple: {1}", curBuffWithId.id, cnt);
                        c.printSl(&sl);
                        break;
                    } else {
                        cnt++;
                        sum += sl.l_orderkey;
                        if (sl.l_orderkey < min)
                            min = sl.l_orderkey;
                        if (sl.l_orderkey > max)
                            max = sl.l_orderkey;

                    }
                }
            }
            if (curBuffWithId.iformat == 2) {
                // Create a byte pointer to the starting address of the vector
                std::byte *dataPtr = curBuffWithId.buff.data();

                // Construct the first four vectors of type int at the dataPtr address


                int *v1 = reinterpret_cast<int *>(curBuffWithId.buff.data());
                int *v2 = reinterpret_cast<int *>(curBuffWithId.buff.data() + BUFFER_SIZE * 4);
                int *v3 = reinterpret_cast<int *>(curBuffWithId.buff.data() + BUFFER_SIZE * 4 * 2);
                int *v4 = reinterpret_cast<int *>(curBuffWithId.buff.data() + BUFFER_SIZE * 4 * 3);
                double *v5 = reinterpret_cast<double *>(curBuffWithId.buff.data() + BUFFER_SIZE * 4 * 4);
                double *v6 = reinterpret_cast<double *>(curBuffWithId.buff.data() + BUFFER_SIZE * 4 * 4 +
                                                        BUFFER_SIZE * 8 * 1);
                double *v7 = reinterpret_cast<double *>(curBuffWithId.buff.data() + BUFFER_SIZE * 4 * 4 +
                                                        BUFFER_SIZE * 8 * 2);
                double *v8 = reinterpret_cast<double *>(curBuffWithId.buff.data() + BUFFER_SIZE * 4 * 4 +
                                                        BUFFER_SIZE * 8 * 3);

                if (buffsRead == 1) {

                    spdlog::get("XCLIENT")->warn("shortLineitem: {0} | {1} | {2} | {3} | {4} | {5} | {6} | {7} ",
                                                 v1[1], v2[1], v3[1], v4[1], v5[1], v6[1], v7[1], v8[1]);
                }

                for (int i = 0; i < BUFFER_SIZE; i++) {
                    totalcnt++;
                    //cout << "Buffer with Id: " << curBuffWithId.id << " l_orderkey: " << sl.l_orderkey << endl;
                    if (v1[i] < 0) {
                        spdlog::get("XCLIENT")->warn("Empty tuple at buffer: {0}, tuple: {1}", curBuffWithId.id, cnt);
                        spdlog::get("XCLIENT")->warn("l_orderkey: {0}", v1[i]);
                        //c.printSl(&sl);
                        break;
                    } else {
                        cnt++;
                        sum += v1[i];
                        if (v1[i] < min)
                            min = v1[i];
                        if (v1[i] > max)
                            max = v1[i];

                    }
                }

            }
            buffsRead++;
            c.markBufferAsRead(curBuffWithId.id);
        } else {
            cout << " found invalid buffer with id: " << curBuffWithId.id << endl;
            break;
        }

    }
    c.finalize();

    spdlog::get("XCLIENT")->info("Total read buffers: {0}", buffsRead);

    auto end = std::chrono::steady_clock::now();

    spdlog::get("XCLIENT")->info("totalcnt: {0}", totalcnt);
    spdlog::get("XCLIENT")->info("cnt: {0}", cnt);
    spdlog::get("XCLIENT")->info("min: {0}", min);
    spdlog::get("XCLIENT")->info("max: {0}", max);
    spdlog::get("XCLIENT")->info("avg: {0}", (sum / (double) cnt));

    spdlog::get("XCLIENT")->info("Total elapsed time: {0} ms, #tuples: {1}",
                                 std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count(), cnt);

    t1.join();
    return 0;
}