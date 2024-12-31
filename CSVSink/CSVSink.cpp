#include "CSVSink.h"
#include <fstream>
#include <spdlog/spdlog.h>
#include <sstream>

CsvSink::CsvSink(std::string baseFilename, int threadCount, xdbc::RuntimeEnv *runtimeEnv)
        : baseFilename(std::move(baseFilename)), threadCount(threadCount), runtimeEnv(runtimeEnv) {
    bufferPool = runtimeEnv->bp;
}

void CsvSink::serialize(int thr) {
    runtimeEnv->pts->push(xdbc::ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "ser", "start"});

    spdlog::get("XDBC.CSVSINK")->info("CSV Serializer started thread {}", thr);

    const auto &schema = runtimeEnv->schema;
    size_t schemaSize = schema.size();

    // Precompute sizes, serializers, and maximum tuple size
    std::vector<size_t> sizes(schemaSize);
    std::vector<char> delimiters(schemaSize);
    using SerializeFunc = size_t (*)(const void *, char *, size_t, char);
    std::vector<SerializeFunc> serializers(schemaSize);

    size_t maxTupleSize = 0;
    for (size_t i = 0; i < schemaSize; ++i) {
        if (schema[i].tpe[0] == 'I') {
            sizes[i] = 4; // sizeof(int)
            serializers[i] = SerializeAttribute<int>;
            maxTupleSize += 12; // Pessimistic size for integer serialization
        } else if (schema[i].tpe[0] == 'D') {
            sizes[i] = 8; // sizeof(double)
            serializers[i] = SerializeAttribute<double>;
            maxTupleSize += 24; // Pessimistic size for double serialization
        } else if (schema[i].tpe[0] == 'C') {
            sizes[i] = 1; // sizeof(char)
            serializers[i] = SerializeAttribute<char>;
            maxTupleSize += 2; // Single character + delimiter
        } else if (schema[i].tpe[0] == 'S') {
            sizes[i] = schema[i].size;
            serializers[i] = SerializeAttribute<const char *>;
            maxTupleSize += schema[i].size + 1; // Fixed string size + delimiter
        }
        delimiters[i] = (i == schemaSize - 1) ? '\n' : ','; // Newline for the last attribute, commas for others
    }

    //TODO: only for format 1
    std::vector<size_t> columnOffsets(schemaSize);
    size_t totalRowSize = 0;
    for (size_t j = 0; j < schemaSize; ++j) {
        columnOffsets[j] = totalRowSize;
        totalRowSize += sizes[j];
    }


    size_t bufferSizeInBytes = runtimeEnv->buffer_size * 1024;

    while (true) {
        int bufferId = runtimeEnv->decompressedBufferIds->pop();
        runtimeEnv->pts->push(xdbc::ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "ser", "pop"});

        if (bufferId == -1) break; // Termination signal

        const auto &inBufferPtr = (*bufferPool)[bufferId];
        size_t tuplesToRead = *reinterpret_cast<const size_t *>(inBufferPtr.data());
        const char *basePtr = reinterpret_cast<const char *>(inBufferPtr.data() + sizeof(size_t));

        int serializedBufferId = runtimeEnv->freeBufferIds->pop();

        auto &outBuffer = (*bufferPool)[serializedBufferId];
        char *writePtr = reinterpret_cast<char *>(outBuffer.data() + sizeof(size_t));
        size_t totalSerializedBytes = 0;

        std::vector<const char *> columnStartPointers(schemaSize);
        size_t cumulativeOffset = 0;
        for (size_t k = 0; k < schemaSize; ++k) {
            columnStartPointers[k] = basePtr + cumulativeOffset;
            cumulativeOffset += tuplesToRead * sizes[k]; // Move by the total size of this column
        }


        for (size_t i = 0; i < tuplesToRead; ++i) {
            if (totalSerializedBytes + maxTupleSize > bufferSizeInBytes) {
                // Buffer is full, push it to the queue
                std::memcpy(outBuffer.data(), &totalSerializedBytes, sizeof(size_t));
                runtimeEnv->pts->push(
                        xdbc::ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "ser", "push"});
                runtimeEnv->serializedBufferIds->push(serializedBufferId);

                // Fetch a new buffer
                serializedBufferId = runtimeEnv->freeBufferIds->pop();
                outBuffer = (*bufferPool)[serializedBufferId];
                writePtr = reinterpret_cast<char *>(outBuffer.data() + sizeof(size_t));
                totalSerializedBytes = 0;
            }


            for (size_t j = 0; j < schemaSize; ++j) {
                const char *dataPtr;
                if (runtimeEnv->iformat == 1) { // Row-major
                    dataPtr = basePtr + i * totalRowSize + columnOffsets[j];
                } else if (runtimeEnv->iformat == 2) { // Column-major
                    dataPtr = columnStartPointers[j] + i * sizes[j];
                }

                totalSerializedBytes += serializers[j](
                        dataPtr, writePtr + totalSerializedBytes, sizes[j], delimiters[j]);
            }
        }

        // Write any remaining data to the buffer
        if (totalSerializedBytes > 0) {
            std::memcpy(outBuffer.data(), &totalSerializedBytes, sizeof(size_t));
            runtimeEnv->pts->push(
                    xdbc::ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "ser", "push"});

            runtimeEnv->serializedBufferIds->push(serializedBufferId);
        }

        // Release decompressed buffer back to freeBufferIds
        runtimeEnv->freeBufferIds->push(bufferId);
    }

    runtimeEnv->pts->push(xdbc::ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "ser", "end"});

    runtimeEnv->finishedSerializerThreads.fetch_add(1);
    if (runtimeEnv->finishedSerializerThreads == runtimeEnv->ser_parallelism) {
        for (int i = 0; i < runtimeEnv->write_parallelism; ++i) {
            runtimeEnv->serializedBufferIds->push(-1); // Termination signal
        }
    }
}


void CsvSink::write(int thr) {
    runtimeEnv->pts->push(xdbc::ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "write", "start"});

    spdlog::get("XDBC.CSVSINK")->info("CSV Writer started thread {}", thr);
    std::ofstream outputFile;
    std::string fileName = baseFilename + "_thread_" + std::to_string(thr) + ".csv";

    outputFile.open(fileName, std::ios::out | std::ios::binary);
    if (!outputFile.is_open()) {
        throw std::runtime_error("Failed to open output file: " + fileName);
    }

    while (true) {
        int bufferId = runtimeEnv->serializedBufferIds->pop();
        runtimeEnv->pts->push(
                xdbc::ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "write", "pop"});

        //spdlog::get("XDBC.CSVSINK")->info("CSV Writer {} got serialized buff {}", thr, bufferId);

        if (bufferId == -1) break; // Termination signal

        const auto &serializedBuffer = (*bufferPool)[bufferId];
        size_t serializedBytes = *reinterpret_cast<const size_t *>(serializedBuffer.data());
        const char *dataPtr = reinterpret_cast<const char *>(serializedBuffer.data() + sizeof(size_t));
        outputFile.write(dataPtr, serializedBytes);

        runtimeEnv->pts->push(
                xdbc::ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "write", "push"});

        runtimeEnv->freeBufferIds->push(bufferId);
    }

    outputFile.close();
    runtimeEnv->finishedWriteThreads.fetch_add(1);
    runtimeEnv->pts->push(xdbc::ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "write", "end"});

}