
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

    xclient.startReceiving(env.table);
    spdlog::get("XCLIENT")->info("#4 called receive, after: {0}ms",
                                 std::chrono::duration_cast<std::chrono::milliseconds>(
                                         std::chrono::steady_clock::now() - start).count());

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

    //Compute profiling info
    std::map<std::string, long long> totalDurations;
    std::map<std::string, int> countEntries;

    for (const auto &entry: env->profilingInfo) {
        totalDurations[entry.first] += entry.second;
        countEntries[entry.first]++;
    }

    // Compute and print throughput for each component
    for (const auto &component: totalDurations) {
        const std::string &key = component.first;
        long long totalDuration = component.second; // Total duration in microseconds
        int count = countEntries[key]; // Number of entries

        // Calculate total data size in bytes
        double totalDataSizeBytes =
                static_cast<double>(count) * env->profilingBufferCnt * (env->buffer_size * 1024);

        // Convert total data size to MB
        double totalDataSizeMB = totalDataSizeBytes / 1e6;

        // Convert total duration to seconds
        double totalDurationSeconds = totalDuration / 1e6;

        // Calculate throughput in MB/s
        double throughput = totalDataSizeMB / totalDurationSeconds;

        if (key == "receive")
            throughput *= env->rcv_parallelism;
        else if (key == "decomp")
            throughput *= env->decomp_parallelism;
        else if (key == "write")
            throughput *= env->write_parallelism;

        spdlog::get("XCLIENT")->info("Component: {0}, throughput {1} MB/s", key, throughput);
    }

}


int Tester::analyticsThread(int thr, int &min, int &max, long &sum, long &cnt, long &totalcnt) {

    int buffsRead = 0;

    size_t tupleSize = 0;
    for (const auto &attr: env->schema) {
        tupleSize += attr.size;
    }
    auto start_profiling = std::chrono::high_resolution_clock::now();
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

                for (int i = 0; i < env->tuples_per_buffer; ++i) {
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
                    //spdlog::get("XCLIENT")->warn("Cnt {0}", cnt);

                    dataPtr += tupleSize;
                }
            } else if (curBuffWithId.iformat == 2) {

                int *v1 = reinterpret_cast<int *>(curBuffWithId.buff.data());
                for (int i = 0; i < env->tuples_per_buffer; i++) {
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

            if (buffsRead > 0 && buffsRead % env->profilingBufferCnt == 0) {
                auto duration_profiling = std::chrono::duration_cast<std::chrono::microseconds>(
                        std::chrono::high_resolution_clock::now() - start_profiling).count();
                env->profilingInfo.insert({"write", duration_profiling});
                start_profiling = std::chrono::high_resolution_clock::now();
                /*spdlog::get("XCLIENT")->info("Write thr {0} profiling {1} ms per {2} buffs", thr,
                                                 duration_profiling, xdbcEnv->profilingBufferCnt);*/
            }
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

    //std::ofstream csvFile(filename + std::to_string(thr) + ".csv", std::ios::out);
    //std::ostringstream csvBuffer(std::ios::out | std::ios::ate);

    std::ofstream csvFile(filename + std::to_string(thr) + ".csv", std::ios::out | std::ios::binary);
    //std::ostringstream csvBuffer;
    //csvBuffer << std::setprecision(std::numeric_limits<double>::max_digits10);
    //csvBuffer << std::setprecision(10);
    //TODO: make generic
    //csvBuffer.str(std::string(env->buffer_size * 1024, '\0'));
    //csvBuffer.clear();
    std::string csvBuffer;
    csvBuffer.reserve((env->buffer_size + 100) * 1024); // Preallocate
    char firstSchemaAttChar = env->schema[0].tpe[0];


    int totalcnt = 0;
    int cnt = 0;
    int buffsRead = 0;
    long long deserTime = 0;
    long long writeTime = 0;

    int schemaSize = env->schema.size();

    //TODO: refactor to call only when columnar format (2)
    std::vector<size_t> offsets(schemaSize);
    size_t baseOffset = 0;
    for (size_t i = 0; i < schemaSize; ++i) {
        offsets[i] = baseOffset;
        baseOffset += env->tuples_per_buffer * env->schema[i].size;
    }

    //TODO: call only when row
    std::vector<size_t> offsetsRow(schemaSize);
    std::vector<size_t> sizes(schemaSize);
    std::vector<size_t> schemaChars(schemaSize);
    size_t baseOffsetRow = 0;
    for (size_t i = 0; i < schemaSize; ++i) {
        offsetsRow[i] = baseOffsetRow;
        if (env->schema[i].tpe[0] == 'I') {
            sizes[i] = 4; // sizeof(int)
            schemaChars[i] = 'I';
        } else if (env->schema[i].tpe[0] == 'D') {
            sizes[i] = 8; // sizeof(double)
            schemaChars[i] = 'D';
        } else if (env->schema[i].tpe[0] == 'C') {
            sizes[i] = 1; // sizeof(char)
            schemaChars[i] = 'C';
        } else if (env->schema[i].tpe[0] == 'S') {
            sizes[i] = env->schema[i].size;
            schemaChars[i] = 'S';
        }
        baseOffsetRow += sizes[i];
    }

    auto start_profiling = std::chrono::high_resolution_clock::now();
    while (xclient.hasNext(thr)) {
        // Get next read buffer and measure the waiting time
        auto start_wait = std::chrono::high_resolution_clock::now();

        xdbc::buffWithId curBuffWithId = xclient.getBuffer(thr);

        auto duration_wait_microseconds = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::high_resolution_clock::now() - start_wait).count();
        env->write_wait_time.fetch_add(duration_wait_microseconds, std::memory_order_relaxed);

        if (curBuffWithId.id >= 0) {
            auto start_deser = std::chrono::high_resolution_clock::now();
            if (curBuffWithId.iformat == 1) {

                auto dataPtr = curBuffWithId.buff.data();
                for (size_t i = 0; i < env->tuples_per_buffer; ++i) {
                    size_t offset = 0;

                    // Check the first attribute before proceeding
                    //TODO: fix empty tuples by not writing them on the server side
                    if (firstSchemaAttChar == 'I' && *reinterpret_cast<int *>(dataPtr) < 0) {
                        spdlog::get("XCLIENT")->warn("Empty tuple at buffer: {0}, tupleNo: {1}",
                                                     curBuffWithId.id, cnt);
                        break;
                    }
                    char tempBuffer[50];
                    //TODO: char comparison works for our current type system
                    for (size_t j = 0; j < schemaSize; ++j) {
                        const char *dataPtrOffset = reinterpret_cast<const char *>(dataPtr + offset);
                        switch (schemaChars[j]) {
                            case 'I': {
                                int value = *reinterpret_cast<const int *>(dataPtrOffset);
                                //csvBuffer.append(std::to_string(value));
                                csvBuffer.append(tempBuffer, sprintf(tempBuffer, "%d", value));
                                break;
                            }
                            case 'D': {
                                double value = *reinterpret_cast<const double *>(dataPtrOffset);
                                csvBuffer.append(tempBuffer, sprintf(tempBuffer, "%.2f", value));
                                break;
                            }
                            case 'C': {
                                csvBuffer.push_back(*dataPtrOffset);
                                break;
                            }
                            case 'S': {
                                csvBuffer.append(dataPtrOffset, sizes[j]);
                                break;
                            }
                            default:
                                break;
                        }
                        offset += sizes[j];
                        //csvBuffer.push_back(j == schemaSize - 1 ? '\n' : ',');
                        csvBuffer.push_back(',');
                    }
                    csvBuffer.back() = '\n';


                    cnt++;
                    dataPtr += offset;
                }

                /*csvFile << csvBuffer.str();
                csvBuffer.str("");*/
            }
            if (curBuffWithId.iformat == 2) {

                std::vector<void *> pointers(schemaSize);
                std::vector<int *> intPointers(schemaSize);
                std::vector<double *> doublePointers(schemaSize);
                std::vector<char *> charPointers(schemaSize);
                std::vector<char *> stringPointers(schemaSize);

                std::byte *dataPtr = curBuffWithId.buff.data();

                // Initialize pointers for the current buffer

                for (size_t j = 0; j < schemaSize; ++j) {

                    pointers[j] = dataPtr + offsets[j];
                    switch (schemaChars[j]) {
                        case 'I': {
                            intPointers[j] = reinterpret_cast<int *>(pointers[j]);
                            break;
                        }
                        case 'D': {
                            doublePointers[j] = reinterpret_cast<double *>(pointers[j]);
                            break;
                        }
                        case 'C': {
                            charPointers[j] = reinterpret_cast<char *>(pointers[j]);
                            break;
                        }
                        case 'S': {
                            stringPointers[j] = reinterpret_cast<char *>(pointers[j]);
                            break;
                        }
                    }
                }

                // Loop over rows
                for (int i = 0; i < env->tuples_per_buffer; ++i) {
                    if (*(intPointers[0] + i) < 0) {
                        spdlog::get("XCLIENT")->warn("Empty tuple at buffer: {0}, tupleNo: {1}", curBuffWithId.id, i);
                        break;  // Exit the loop if the first element is less than zero
                    }
                    char tempBuffer[50];
                    for (size_t j = 0; j < schemaSize; ++j) {
                        //const auto &schema = env->schema[j];
                        switch (schemaChars[j]) {
                            case 'I': {
                                int value = *(intPointers[j] + i);

                                csvBuffer.append(std::to_string(value));
                                break;
                            }
                            case 'D': {
                                double value = *(doublePointers[j] + i);
                                csvBuffer.append(tempBuffer, sprintf(tempBuffer, "%.2f", value));
                                break;
                            }
                            case 'C': {
                                csvBuffer.push_back(*(charPointers[j] + i));
                                break;
                            }
                            case 'S': {
                                //csvBuffer.append(stringPointers[j] + i * schema.size, schema.size);
                                csvBuffer.append(stringPointers[j] + i * sizes[j], sizes[j]);
                                break;
                            }
                            default:
                                break;
                        }
                        csvBuffer.push_back(',');
                    }
                    csvBuffer.back() = '\n';
                }
            }
            deserTime += std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::high_resolution_clock::now() - start_deser).count();

            auto start_write = std::chrono::high_resolution_clock::now();

            //spdlog::get("XCLIENT")->info("csv buffer size {0} ", csvBuffer.size());
            csvFile.write(csvBuffer.data(), csvBuffer.size());
            csvBuffer.clear();
            writeTime += std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::high_resolution_clock::now() - start_write).count();

            buffsRead++;

            if (buffsRead > 0 && buffsRead % env->profilingBufferCnt == 0) {
                auto duration_profiling = std::chrono::duration_cast<std::chrono::microseconds>(
                        std::chrono::high_resolution_clock::now() - start_profiling).count();
                env->profilingInfo.insert({"write", duration_profiling});
                start_profiling = std::chrono::high_resolution_clock::now();
                /*spdlog::get("XCLIENT")->info("Write thr {0} profiling {1} ms per {2} buffs", thr,
                                                 duration_profiling, xdbcEnv->profilingBufferCnt);*/
            }

            xclient.markBufferAsRead(curBuffWithId.id);
        } else {
            spdlog::get("XCLIENT")->warn("found invalid buffer with id: {0}, buff_no: {1}",
                                         curBuffWithId.id, buffsRead);
            break;
        }

    }

    spdlog::get("XCLIENT")->info("Thr {0} DeserTime: {1}, WriteTime {2}", thr, deserTime / 1000, writeTime / 1000);

    spdlog::get("XCLIENT")->info("Write thread {0} Total written buffers: {1}", thr, buffsRead);
    csvFile.close();

    return buffsRead;
}


void Tester::runStorage(const std::string &filename) {

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