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
            int iformat = 1;
            if (iformat == 1) {
                for (auto sl: curBuffWithId.buff) {
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
            if (iformat == 2) {
                // Access the elements from the original inner vector
                int* element1 = reinterpret_cast<int*>(innerVectorData + calculateOffset(0, sizeof(int)));
                double* element2 = reinterpret_cast<double*>(innerVectorData + calculateOffset(1, sizeof(double)));


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