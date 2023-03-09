#include <iostream>
#include <thread>
#include "../xdbc/xclient.h"
#include <iomanip>

//#define BUFFERPOOL_SIZE 1000
#define TOTAL_TUPLES 10000000

using namespace std;

int main() {

    xdbc::XClient c("cpp Client");

    cout << "#1 Constructed XClient called: " << c.get_name() << endl;

    thread t1 = c.startReceiving("test");


    int min = INT32_MAX;
    int max = INT32_MIN;
    long sum = 0;
    long cnt = 0;
    long totalcnt = 0;

    cout << "#4 called receive" << endl;

    auto start = std::chrono::steady_clock::now();

    int buffsRead = 0;
    while (c.hasUnread()) {
        xdbc::buffWithId curBuffWithId = c.getBuffer();
        //cout << "Iteration at tuple:" << cnt << " and buffer " << buffsRead << endl;
        if (curBuffWithId.id >= 0) {
            for (auto sl: curBuffWithId.buff) {
                totalcnt++;
                //cout << "Buffer with Id: " << curBuffWithId.id << " l_orderkey: " << sl.l_orderkey << endl;
                if (sl.l_orderkey < 0) {
                    cout << "Empty tuple at buffer: " << curBuffWithId.id << " and tuple " << cnt << endl;
                    c.printSl(&sl);
                    break;
                }
                cnt++;
                sum += sl.l_orderkey;
                if (sl.l_orderkey < min && sl.l_orderkey > 0)
                    min = sl.l_orderkey;
                if (sl.l_orderkey > max)
                    max = sl.l_orderkey;
            }
        } else
            cout << " found buffer with id: " << curBuffWithId.id << endl;

        buffsRead++;
        c.markBufferAsRead(curBuffWithId.id);
    }
    c.finalize();

    cout << "Total read Buffers: " << buffsRead << endl;


    auto end = std::chrono::steady_clock::now();

    cout << "totalcnt: " << totalcnt << endl;
    cout << "cnt: " << cnt << endl;
    cout << "min: " << min << endl;
    cout << "max: " << max << endl;
    cout << "avg:" << fixed << (sum / (double) cnt) << endl;

    cout << "Elapsed time in milliseconds: "
         << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count()
         << " ms" << " for #tuples: " << cnt << endl;

    t1.join();
    return 0;
}