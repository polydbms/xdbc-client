
#include "Tester.h"
#include <numeric>
#include <algorithm>
#include <iostream>
#include <thread>
#include <utility>
#include <fstream>
#include <iomanip>
#include "spdlog/spdlog.h"
#include "spdlog/sinks/stdout_color_sinks.h"

Tester::Tester(std::string name, xdbc::RuntimeEnv &env,
               std::vector<std::tuple<std::string, std::string, int>> schema)
        : name(std::move(name)), env(&env), schema(std::move(schema)), xclient(env) {

    start = std::chrono::steady_clock::now();
    spdlog::get("XCLIENT")->info("#1 Constructed XClient called: {0}", xclient.get_name());

}

void Tester::close() {

    xclient.finalize();
    
    auto end = std::chrono::steady_clock::now();
    auto total_time = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

    spdlog::get("XCLIENT")->info("Total elapsed time: {0} ms", total_time);

    long long rcv_wait_time = env->rcv_wait_time.load(std::memory_order_relaxed) / 1000 / env->rcv_parallelism;
    long long rcv_time = (env->rcv_time.load(std::memory_order_relaxed) / 1000 - rcv_wait_time);

    long long decomp_wait_time = env->decomp_wait_time.load(std::memory_order_relaxed) / 1000 / env->decomp_parallelism;
    long long decomp_time = (env->decomp_time.load(std::memory_order_relaxed) / 1000 - decomp_wait_time);

    long long write_wait_time = env->write_wait_time.load(std::memory_order_relaxed) / 1000 / env->write_parallelism;
    long long write_time = (env->write_time.load(std::memory_order_relaxed) / 1000 - write_wait_time);


    spdlog::get("XCLIENT")->info("xdbc client | receive time: {0} ms, decompress time: {1} ms, write time {2} ms",
                                 rcv_time, decomp_time, write_time);

    spdlog::get("XCLIENT")->info(
            "xdbc client | receive wait time: {0} ms, decompress wait time: {1} ms, write wait time {2} ms",
            rcv_wait_time, decomp_wait_time, write_wait_time);

    std::ofstream csv_file("/tmp/xdbc_client_timings.csv",
                           std::ios::out | std::ios::app);

    csv_file << std::fixed << std::setprecision(2)
             << std::to_string(env->transfer_id) << "," << total_time << ","
             << rcv_wait_time << ","
             << rcv_time << ","
             << decomp_wait_time << ","
             << decomp_time << ","
             << write_wait_time << ","
             << write_time << "\n";
    csv_file.close();

}

int Tester::analyticsThread(int thr, int &min, int &max, long &sum, long &cnt, long &totalcnt) {

    int buffsRead = 0;

    while (xclient.hasNext(thr)) {
        // Get next read buffer and measure the wait time
        auto start_wait = std::chrono::high_resolution_clock::now();

        xdbc::buffWithId curBuffWithId = xclient.getBuffer(thr);

        auto duration_wait_microseconds = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::high_resolution_clock::now() - start_wait).count();
        env->write_wait_time.fetch_add(duration_wait_microseconds, std::memory_order_relaxed);

        // Read the buffer and measure the working time


        //cout << "Iteration at tuple:" << cnt << " and buffer " << buffsRead << endl;
        if (curBuffWithId.id >= 0) {
            if (curBuffWithId.iformat == 1) {
                //TODO replace vector copying with direct access like  in storage thread
                auto *ptr = reinterpret_cast<Utils::shortLineitem *>(curBuffWithId.buff.data());
                std::vector<Utils::shortLineitem> sls(ptr, ptr + env->buffer_size);

                for (auto sl: sls) {
                    totalcnt++;
                    //cout << "Buffer with Id: " << curBuffWithId.id << " l_orderkey: " << sl.l_orderkey << endl;
                    if (sl.l_orderkey < 0) {
                        spdlog::get("XCLIENT")->warn("Empty tuple at buffer: {0}, tupleNo: {1}, tuple: [{2}]",
                                                     curBuffWithId.id, cnt, Utils::slStr(&sl));

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
                if (buffsRead == 1) {

                    /*spdlog::get("XCLIENT")->info(
                            "first shortLineitem: {0} | {1} | {2} | {3} | {4} | {5} | {6} | {7} ",
                            sl.l_orderkey, sl.l_partkey, sl.l_suppkey, sl.l_linenumber, sl.l_quantity,
                            sl.l_extendedprice, sl.l_discount, sl.l_tax);*/
                }

            } else if (curBuffWithId.iformat == 2) {
                // Create a byte pointer to the starting address of the vector
                std::byte *dataPtr = curBuffWithId.buff.data();

                // Construct the first four vectors of type int at the dataPtr address

                int *v1 = reinterpret_cast<int *>(curBuffWithId.buff.data());
                int *v2 = reinterpret_cast<int *>(curBuffWithId.buff.data() + env->buffer_size * 4);
                int *v3 = reinterpret_cast<int *>(curBuffWithId.buff.data() + env->buffer_size * 4 * 2);
                int *v4 = reinterpret_cast<int *>(curBuffWithId.buff.data() + env->buffer_size * 4 * 3);
                double *v5 = reinterpret_cast<double *>(curBuffWithId.buff.data() + env->buffer_size * 4 * 4);
                double *v6 = reinterpret_cast<double *>(curBuffWithId.buff.data() + env->buffer_size * 4 * 4 +
                                                        env->buffer_size * 8 * 1);
                double *v7 = reinterpret_cast<double *>(curBuffWithId.buff.data() + env->buffer_size * 4 * 4 +
                                                        env->buffer_size * 8 * 2);
                double *v8 = reinterpret_cast<double *>(curBuffWithId.buff.data() + env->buffer_size * 4 * 4 +
                                                        env->buffer_size * 8 * 3);

                if (buffsRead == 1) {

                    spdlog::get("XCLIENT")->info("first shortLineitem: {0} | {1} | {2} | {3} | {4} | {5} | {6} | {7} ",
                                                 v1[0], v2[0], v3[0], v4[0], v5[0], v6[0], v7[0], v8[0]);
                }

                for (int i = 0; i < env->buffer_size; i++) {
                    totalcnt++;
                    /*if (v1[i] > 0) {
                        spdlog::get("XCLIENT")->info(
                                "first shortLineitem: {0} | {1} | {2} | {3} | {4} | {5} | {6} | {7} ",
                                v1[i], v2[i], v3[i], v4[i], v5[i], v6[i], v7[i], v8[i]);
                    }*/
                    //cout << "Buffer with Id: " << curBuffWithId.id << " l_orderkey: " << sl.l_orderkey << endl;
                    if (v1[i] < 0) {
                        //spdlog::get("XCLIENT")->warn("Empty tuple at buffer: {0}, tuple_no: {1}, l_orderkey: {2}",
                        //                            curBuffWithId.id, cnt, v1[i]);
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
            xclient.markBufferAsRead(curBuffWithId.id);
        } else {
            spdlog::get("XCLIENT")->warn("Read thread {0} found invalid buffer with id: {1}, buff_no: {2}",
                                         thr, curBuffWithId.id, buffsRead);
            break;
        }

        // add the measured time to total reading time


    }

    return buffsRead;

}


void Tester::runAnalytics() {

    xclient.startReceiving(env->table);
    spdlog::get("XCLIENT")->info("#4 called receive, after: {0}ms",
                                 std::chrono::duration_cast<std::chrono::milliseconds>(
                                         std::chrono::steady_clock::now() - start).count());

    int mins[env->write_parallelism];
    int maxs[env->write_parallelism];
    long sums[env->write_parallelism];
    long cnts[env->write_parallelism];
    long totalcnts[env->write_parallelism];

    std::thread writeThreads[env->write_parallelism];

    for (int i = 0; i < env->write_parallelism; i++) {

        mins[i] = INT32_MAX;
        maxs[i] = INT32_MIN;

        sums[i] = 0L;
        cnts[i] = 0L;
        totalcnts[i] = 0L;
        writeThreads[i] = std::thread(&Tester::analyticsThread, this, i, std::ref(mins[i]), std::ref(maxs[i]),
                                      std::ref(sums[i]), std::ref(cnts[i]), std::ref(totalcnts[i]));
    }

    for (int i = 0; i < env->write_parallelism; i++) {
        writeThreads[i].join();
    }


    int *minValuePtr = std::min_element(mins, mins + env->write_parallelism);
    int *maxValuePtr = std::max_element(maxs, maxs + env->write_parallelism);
    long sum = std::accumulate(sums, sums + env->write_parallelism, 0L);
    long cnt = std::accumulate(cnts, cnts + env->write_parallelism, 0L);
    long totalcnt = std::accumulate(totalcnts, totalcnts + env->write_parallelism, 0L);

    spdlog::get("XCLIENT")->info("totalcnt: {0}", totalcnt);
    spdlog::get("XCLIENT")->info("cnt: {0}", cnt);
    spdlog::get("XCLIENT")->info("min: {0}", *minValuePtr);
    spdlog::get("XCLIENT")->info("max: {0}", *maxValuePtr);
    spdlog::get("XCLIENT")->info("avg: {0}", (sum / (double) cnt));
}

int Tester::storageThread(int thr, const std::string &filename) {

    std::ofstream csvFile(filename + std::to_string(thr) + ".csv", std::ios::out);

    std::ostringstream csvBuffer;
    int totalcnt = 0;
    int cnt = 0;
    int buffsRead = 0;
    while (xclient.hasNext(thr)) {
        // Get next read buffer and measure the waiting time
        auto start_wait = std::chrono::high_resolution_clock::now();

        xdbc::buffWithId curBuffWithId = xclient.getBuffer(thr);

        auto duration_wait_microseconds = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::high_resolution_clock::now() - start_wait).count();
        env->write_wait_time.fetch_add(duration_wait_microseconds, std::memory_order_relaxed);


        //cout << "Iteration at tuple:" << cnt << " and buffer " << buffsRead << endl;
        if (curBuffWithId.id >= 0) {
            if (curBuffWithId.iformat == 1) {
                auto *ptr = reinterpret_cast<Utils::shortLineitem *>(curBuffWithId.buff.data());

                for (int i = 0; i < env->buffer_size; ++i) {
                    Utils::shortLineitem &sl = ptr[i];
                    totalcnt++;
                    if (sl.l_orderkey < 0) {
                        spdlog::get("XCLIENT")->warn("Empty tuple at buffer: {0}, tupleNo: {1}, tuple: [{2}]",
                                                     curBuffWithId.id, cnt, Utils::slStr(&sl));
                        break;
                    } else {
                        cnt++;
                        csvBuffer << sl.l_orderkey << "," << sl.l_partkey << "," << sl.l_suppkey
                                  << "," << sl.l_linenumber << "," << sl.l_quantity << "," << sl.l_extendedprice
                                  << "," << sl.l_discount << "," << sl.l_tax << "\n";
                    }
                }
                if (buffsRead == 1) {

                    /*spdlog::get("XCLIENT")->info(
                            "first shortLineitem: {0} | {1} | {2} | {3} | {4} | {5} | {6} | {7} ",
                            sl.l_orderkey, sl.l_partkey, sl.l_suppkey, sl.l_linenumber, sl.l_quantity,
                            sl.l_extendedprice, sl.l_discount, sl.l_tax);*/
                }
                csvFile << csvBuffer.str();

                csvBuffer.str("");
            }
            if (curBuffWithId.iformat == 2) {
                // Create a byte pointer to the starting address of the vector
                //std::byte *dataPtr = curBuffWithId.buff.data();

                // Construct the first four vectors of type int at the dataPtr address
                //TODO: use schema info instead of hardcoded pointers

                int *v1 = reinterpret_cast<int *>(curBuffWithId.buff.data());
                int *v2 = reinterpret_cast<int *>(curBuffWithId.buff.data() + env->buffer_size * 4);
                int *v3 = reinterpret_cast<int *>(curBuffWithId.buff.data() + env->buffer_size * 4 * 2);
                int *v4 = reinterpret_cast<int *>(curBuffWithId.buff.data() + env->buffer_size * 4 * 3);
                double *v5 = reinterpret_cast<double *>(curBuffWithId.buff.data() + env->buffer_size * 4 * 4);
                double *v6 = reinterpret_cast<double *>(curBuffWithId.buff.data() + env->buffer_size * 4 * 4 +
                                                        env->buffer_size * 8 * 1);
                double *v7 = reinterpret_cast<double *>(curBuffWithId.buff.data() + env->buffer_size * 4 * 4 +
                                                        env->buffer_size * 8 * 2);
                double *v8 = reinterpret_cast<double *>(curBuffWithId.buff.data() + env->buffer_size * 4 * 4 +
                                                        env->buffer_size * 8 * 3);

                if (buffsRead == 1) {

                    spdlog::get("XCLIENT")->info(
                            "first shortLineitem: {0} | {1} | {2} | {3} | {4} | {5} | {6} | {7} ",
                            v1[0], v2[0], v3[0], v4[0], v5[0], v6[0], v7[0], v8[0]);
                }

                for (int i = 0; i < env->buffer_size; i++) {
                    totalcnt++;
                    /*if (v1[i] > 0) {
                        spdlog::get("XCLIENT")->info(
                                "first shortLineitem: {0} | {1} | {2} | {3} | {4} | {5} | {6} | {7} ",
                                v1[i], v2[i], v3[i], v4[i], v5[i], v6[i], v7[i], v8[i]);
                    }*/
                    //cout << "Buffer with Id: " << curBuffWithId.id << " l_orderkey: " << sl.l_orderkey << endl;
                    csvBuffer << v1[i] << "," << v2[i] << "," << v3[i] << "," << v4[i] << "," << v5[i] << ","
                              << v6[i] << "," << v7[i] << "," << v8[i] << "\n";
                }

                csvFile << csvBuffer.str();

                csvBuffer.str("");

            }
            buffsRead++;
            xclient.markBufferAsRead(curBuffWithId.id);
        } else {
            spdlog::get("XCLIENT")->warn("found invalid buffer with id: {0}, buff_no: {1}",
                                         curBuffWithId.id, buffsRead);
            break;
        }

    }

    spdlog::get("XCLIENT")->info("Write thread {0} Total written buffers: {1}", thr, buffsRead);
    csvFile.close();

    return buffsRead;
}


void Tester::runStorage(const std::string &filename) {


    xclient.startReceiving(env->table);
    spdlog::get("XCLIENT")->info("#4 called receive, after: {0}ms",
                                 std::chrono::duration_cast<std::chrono::milliseconds>(
                                         std::chrono::steady_clock::now() - start).count());

    std::thread writeThreads[env->write_parallelism];

    for (int i = 0; i < env->write_parallelism; i++) {
        writeThreads[i] = std::thread(&Tester::storageThread, this, i, std::ref(filename));
    }


    for (int i = 0; i < env->write_parallelism; i++) {
        writeThreads[i].join();
    }

    //TODO: combine multiple files or maybe do in external bash script
    /*std::ofstream csvFile(filename, std::ios::out);
    csvFile.seekp(0, std::ios::end);
    std::streampos fileSize = csvFile.tellp();
    spdlog::get("XCLIENT")->info("fileSize: {0}", fileSize);
    csvFile.close();*/


}