#include "CSVSink.h"

#include <utility>
#include <spdlog/spdlog.h>
#include <thread>

CsvSink::CsvSink(std::string baseFilename, int threadCount, xdbc::RuntimeEnv *runtimeEnv)
        : baseFilename(std::move(baseFilename)), threadCount(threadCount), runtimeEnv(runtimeEnv) {
    bufferPool = runtimeEnv->bp;
}


void CsvSink::serialize(int thr) {

    spdlog::get("XDBC.CSVSINK")->info("CSV Serializer started thread {0}", thr);
    size_t bytesWritten;
    const auto &schema = runtimeEnv->schema;
    size_t schemaSize = schema.size();

    // Precompute sizes and serializers
    std::vector<size_t> maxAttributeSizes(schemaSize);
    std::vector<size_t> sizes(schemaSize);
    std::vector<char> delimiters(schemaSize);

    using SerializeFunc = size_t (*)(const void *, char *, size_t, char);
    std::vector<SerializeFunc> serializers(schemaSize);

    size_t maxTupleSize = 0;

    for (size_t i = 0; i < schemaSize; ++i) {
        if (schema[i].tpe[0] == 'I') {
            sizes[i] = 4; // sizeof(int)
            maxAttributeSizes[i] = 12; // Max size for int
            serializers[i] = SerializeAttribute<int>;
        } else if (schema[i].tpe[0] == 'D') {
            sizes[i] = 8; // sizeof(double)
            maxAttributeSizes[i] = 24; // Max size for double with precision
            serializers[i] = SerializeAttribute<double>;
        } else if (schema[i].tpe[0] == 'C') {
            sizes[i] = 1; // sizeof(char)
            maxAttributeSizes[i] = 1; // Single char
            serializers[i] = SerializeAttribute<char>;
        } else if (schema[i].tpe[0] == 'S') {
            sizes[i] = schema[i].size;
            maxAttributeSizes[i] = schema[i].size; // Fixed-size string length from schema
            serializers[i] = SerializeAttribute<const char *>;
        }
        delimiters[i] = (i == schemaSize - 1) ? '\n' : ','; // Newline for the last attribute, commas for others

        maxTupleSize += maxAttributeSizes[i]; // Accumulate total attribute size
    }

    // Add space for delimiters
    maxTupleSize += schemaSize; // One delimiter per attribute

    // Get the initial write buffer
    int writeBufferId = runtimeEnv->freeBufferIds->pop();
    spdlog::get("XDBC.CSVSINK")->info("CSV Serializer {} got write buff {}", thr, writeBufferId);

    auto &writeBuffer = (*bufferPool)[writeBufferId];
    char *writeBufferPtr = reinterpret_cast<char *>(writeBuffer.data() + sizeof(size_t));

    size_t serializedBytes = 0;
    size_t serializedTuples = 0;
    size_t writtenBuffers = 0;
    size_t bufferSizeInBytes = runtimeEnv->buffer_size * 1024;

    while (true) {
        int bufferId = runtimeEnv->decompressedBufferIds->pop();
        spdlog::get("XDBC.CSVSINK")->info("CSV Serializer {} got deser buff {}", thr, bufferId);

        if (bufferId == -1) break; // Termination signal

        const auto &inBufferPtr = (*bufferPool)[bufferId];
        size_t tuplesToRead = *reinterpret_cast<const size_t *>(inBufferPtr.data());
        const char *readBufferPtr = reinterpret_cast<const char *>(inBufferPtr.data() + sizeof(size_t));

        for (size_t i = 0; i < tuplesToRead; ++i) {
            // Check if the next tuple fits in the current buffer
            size_t bytesToWrite = serializedBytes + sizeof(size_t) + maxTupleSize;
            //if (bytesToWrite > bufferSizeInBytes) {
            if (i % 5000 == 0) {

                spdlog::get("XDBC.CSVSINK")->info("CSV Serializer {} bytesToWrite/max {}/{}", thr,
                                                  bytesToWrite, bufferSizeInBytes);


                //spdlog::info("Buffer pool size: {}", bufferPool->size());
                memcpy(writeBuffer.data(), &serializedBytes, sizeof(size_t));

/*                spdlog::get("XDBC.CSVSINK")->info("CSV Serializer {} finished write buff {}", thr, writeBufferId);
                std::string serializedChunk(reinterpret_cast<char *>(writeBuffer.data() + sizeof(size_t)), 600);
                spdlog::info("Serialized chunk of size {} for buff {}: {}", serializedBytes, writeBufferId,
                             serializedChunk);*/

                runtimeEnv->serializedBufferIds->push(writeBufferId);

                // Fetch a new buffer
                writeBufferId = runtimeEnv->freeBufferIds->pop();

                spdlog::get("XDBC.CSVSINK")->info("CSV Serializer {} got write buff {}", thr, writeBufferId);
                writeBuffer = (*bufferPool)[writeBufferId];


                writeBufferPtr = reinterpret_cast<char *>(writeBuffer.data() + sizeof(size_t));
                //std::this_thread::sleep_for(std::chrono::seconds(1));


                serializedBytes = 0;
                serializedTuples = 0;
                writtenBuffers++;
            }
            /*if (writtenBuffers == 1)
                break;*/


            // Serialize each attribute of the tuple
            for (size_t j = 0; j < schemaSize; ++j) {
                const char *currentReadPtr;

                if (runtimeEnv->iformat == 1) { // Row-Major
                    currentReadPtr = readBufferPtr;
                    readBufferPtr += sizes[j];
                } else if (runtimeEnv->iformat == 2) { // Column-Major
                    currentReadPtr = reinterpret_cast<const char *>(
                            inBufferPtr.data() + sizeof(size_t) + (j * tuplesToRead * sizes[j]) + (i * sizes[j]));
                }

                // Serialize the current attribute
                bytesWritten = serializers[j](currentReadPtr, writeBufferPtr + serializedBytes, sizes[j],
                                              delimiters[j]);
                serializedBytes += bytesWritten;

            }

            serializedTuples++;
        }

        spdlog::get("XDBC.CSVSINK")->info("CSV Serializer {} finished deser buff {}", thr, bufferId);

        // Release the processed input buffer
        runtimeEnv->freeBufferIds->push(bufferId);

    }

    // Finalize the last buffer if it has data
    if (serializedTuples > 0) {
        memcpy(writeBuffer.data(), &serializedBytes, sizeof(size_t));
        runtimeEnv->serializedBufferIds->push(writeBufferId);
    }


    runtimeEnv->finishedSerializerThreads.fetch_add(1);
    if (runtimeEnv->finishedSerializerThreads == runtimeEnv->ser_parallelism) {
        for (int i = 0; i < runtimeEnv->write_parallelism; i++)
            runtimeEnv->serializedBufferIds->push(-1);
    }

}

void CsvSink::write(int thr) {

    //std::this_thread::sleep_for(std::chrono::seconds(5));

    spdlog::get("XDBC.CSVSINK")->info("CSV Writer started thread {0}", thr);

    // Open the output file for the current thread
    std::ofstream outputFile;
    std::string fileName = baseFilename + "_thread_" + std::to_string(thr) + ".csv";

    outputFile.open(fileName, std::ios::out | std::ios::binary);
    if (!outputFile.is_open()) {
        throw std::runtime_error("Failed to open output file: " + fileName);
    }

    while (true) {
        // Poll a serialized buffer from the queue
        int bufferId = runtimeEnv->serializedBufferIds->pop();
        spdlog::get("XDBC.CSVSINK")->info("CSV Writer {} got s buff {}", thr, bufferId);

        if (bufferId == -1) break; // Termination signal

        // Access the serialized buffer
        const auto &serializedBuffer = (*bufferPool)[bufferId];

        // Read the number of bytes written from the header
        size_t serializedBytes = *reinterpret_cast<const size_t *>(serializedBuffer.data());

        // Get a pointer to the actual serialized data (after the header)
        const char *dataPtr = reinterpret_cast<const char *>(serializedBuffer.data() + sizeof(size_t));

        std::string serializedChunk(dataPtr, 600);
        spdlog::get("XDBC.CSVSINK")->info("Writer got buff: {} with #bytes: {} and content {}", bufferId,
                                          serializedBytes, serializedChunk);

        // Write the serialized data to the file
        outputFile.write(dataPtr, serializedBytes);
        outputFile.flush();

        // Push the buffer ID back to the free buffer queue
        runtimeEnv->freeBufferIds->push(bufferId);
    }

    outputFile.close();
    runtimeEnv->finishedWriteThreads.fetch_add(1);
}


