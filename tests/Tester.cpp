
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

Tester::Tester(std::string name, xdbc::RuntimeEnv &env)
        : name(std::move(name)), env(&env), xclient(env) {

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

    size_t tupleSize = 0;
    for (const auto &attr: env->schema) {
        tupleSize += attr.size;
    }

    while (xclient.hasNext(thr)) {
        // Get next read buffer and measure the wait time
        auto start_wait = std::chrono::high_resolution_clock::now();

        xdbc::buffWithId curBuffWithId = xclient.getBuffer(thr);

        auto duration_wait_microseconds = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::high_resolution_clock::now() - start_wait).count();
        env->write_wait_time.fetch_add(duration_wait_microseconds, std::memory_order_relaxed);

        if (curBuffWithId.id >= 0) {
            if (curBuffWithId.iformat == 1) {
                auto dataPtr = curBuffWithId.buff.data();

                for (int i = 0; i < env->buffer_size; ++i) {
                    totalcnt++;

                    int *firstAttribute = reinterpret_cast<int *>(dataPtr);

                    if (*firstAttribute < 0) {
                        spdlog::get("XCLIENT")->warn("Empty tuple at buffer: {0}, tupleNo: {1}", curBuffWithId.id, i);
                        break;
                    }
                    cnt++;
                    sum += *firstAttribute;
                    if (*firstAttribute < min) min = *firstAttribute;
                    if (*firstAttribute > max) max = *firstAttribute;

                    dataPtr += tupleSize;
                }
            } else if (curBuffWithId.iformat == 2) {

                int *v1 = reinterpret_cast<int *>(curBuffWithId.buff.data());
                for (int i = 0; i < env->buffer_size; i++) {
                    totalcnt++;
                    if (v1[i] < 0) {
                        spdlog::get("XCLIENT")->warn("Empty tuple at buffer: {0}, tuple_no: {1}", curBuffWithId.id, i);
                        break;
                    }

                    sum += v1[i];
                    if (v1[i] < min) min = v1[i];
                    if (v1[i] > max) max = v1[i];
                    cnt++;
                }

            }
            buffsRead++;
            xclient.markBufferAsRead(curBuffWithId.id);
        } else {
            spdlog::get("XCLIENT")->warn("Write thread {0} found invalid buffer with id: {1}, buff_no: {2}",
                                         thr, curBuffWithId.id, buffsRead);
            break;
        }

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

    std::ostringstream csvBuffer(std::ios::out | std::ios::ate);
    //TODO: make generic
    //csvBuffer.str(std::string(env->buffer_size * schema.size() * 20, '\0'));
    csvBuffer.clear();

    int totalcnt = 0;
    int cnt = 0;
    int buffsRead = 0;


    std::vector<size_t> offsets(env->schema.size());
    size_t baseOffset = 0;
    for (size_t i = 0; i < env->schema.size(); ++i) {
        offsets[i] = baseOffset;
        if (env->schema[i].tpe == "INT") {
            baseOffset += env->buffer_size * sizeof(int);
        } else if (env->schema[i].tpe == "DOUBLE") {
            baseOffset += env->buffer_size * sizeof(double);
        }
    }


    while (xclient.hasNext(thr)) {
        // Get next read buffer and measure the waiting time
        auto start_wait = std::chrono::high_resolution_clock::now();

        xdbc::buffWithId curBuffWithId = xclient.getBuffer(thr);

        auto duration_wait_microseconds = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::high_resolution_clock::now() - start_wait).count();
        env->write_wait_time.fetch_add(duration_wait_microseconds, std::memory_order_relaxed);

        if (curBuffWithId.id >= 0) {
            if (curBuffWithId.iformat == 1) {

                auto dataPtr = curBuffWithId.buff.data();
                for (size_t i = 0; i < env->buffer_size; ++i) {
                    size_t offset = 0;

                    // Check the first attribute before proceeding
                    //TODO: fix empty tuples by not writing them on the server side
                    if (env->schema.front().tpe == "INT" && *reinterpret_cast<int *>(dataPtr) < 0) {
                        spdlog::get("XCLIENT")->warn("Empty tuple at buffer: {0}, tupleNo: {1}",
                                                     curBuffWithId.id, cnt);
                        break;
                    }

                    for (const auto &attr: env->schema) {
                        if (attr.tpe == "INT") {
                            csvBuffer << *reinterpret_cast<int *>(dataPtr + offset);
                            offset += sizeof(int);
                        } else if (attr.tpe == "DOUBLE") {
                            csvBuffer << *reinterpret_cast<double *>(dataPtr + offset);
                            offset += sizeof(double);
                        }

                        csvBuffer << (&attr != &env->schema.back() ? "," : "\n");
                    }

                    cnt++;
                    dataPtr += offset;
                }

                csvFile << csvBuffer.str();
                csvBuffer.str("");
            }
            if (curBuffWithId.iformat == 2) {

                std::vector<void *> pointers(env->schema.size());
                std::vector<int *> intPointers(env->schema.size());
                std::vector<double *> doublePointers(env->schema.size());
                std::byte *dataPtr = curBuffWithId.buff.data();

                // Initialize pointers for the current buffer
                for (size_t j = 0; j < env->schema.size(); ++j) {
                    pointers[j] = static_cast<void *>(dataPtr + offsets[j]);
                    if (env->schema[j].tpe == "INT") {
                        intPointers[j] = reinterpret_cast<int *>(pointers[j]);
                    } else if (env->schema[j].tpe == "DOUBLE") {
                        doublePointers[j] = reinterpret_cast<double *>(pointers[j]);
                    }
                }

                // Loop over rows
                for (int i = 0; i < env->buffer_size; ++i) {
                    if (*(intPointers[0] + i) < 0) {
                        spdlog::get("XCLIENT")->warn("Empty tuple at buffer: {0}, tupleNo: {1}", curBuffWithId.id, i);
                        break;  // Exit the loop if the first element is less than zero
                    }
                    for (size_t j = 0; j < env->schema.size(); ++j) {
                        if (env->schema[j].tpe == "INT") {
                            csvBuffer << *(intPointers[j] + i);
                        } else if (env->schema[j].tpe == "DOUBLE") {
                            csvBuffer << *(doublePointers[j] + i);
                        }
                        csvBuffer << (j < env->schema.size() - 1 ? "," : "\n");
                    }
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