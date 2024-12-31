#include "CSVSink.h"
#include <fstream>
#include <spdlog/spdlog.h>
#include <sstream>

CsvSink::CsvSink(std::string baseFilename, int threadCount, xdbc::RuntimeEnv *runtimeEnv)
        : baseFilename(std::move(baseFilename)), threadCount(threadCount), runtimeEnv(runtimeEnv) {
    bufferPool = runtimeEnv->bp;
}

void CsvSink::serialize(int thr) {

}

void CsvSink::write(int thr) {
    spdlog::get("XDBC.CSVSINK")->info("CSV Writer/Serializer started thread {}", thr);

    // Open the output file for the current thread
    std::ofstream outputFile;
    std::string fileName = baseFilename + "_thread_" + std::to_string(thr) + ".csv";
    outputFile.open(fileName, std::ios::out | std::ios::binary);

    if (!outputFile.is_open()) {
        throw std::runtime_error("Failed to open output file: " + fileName);
    }

    const auto &schema = runtimeEnv->schema;
    size_t schemaSize = schema.size();
    std::vector<size_t> sizes(schemaSize);
    std::vector<char> delimiters(schemaSize);

    using SerializeFunc = size_t (*)(const void *, char *, size_t, char);
    std::vector<SerializeFunc> serializers(schemaSize);

    // Precompute attribute sizes, delimiters, and serializers
    for (size_t i = 0; i < schemaSize; ++i) {
        if (schema[i].tpe[0] == 'I') {
            sizes[i] = 4; // sizeof(int)
            serializers[i] = SerializeAttribute<int>;
        } else if (schema[i].tpe[0] == 'D') {
            sizes[i] = 8; // sizeof(double)
            serializers[i] = SerializeAttribute<double>;
        } else if (schema[i].tpe[0] == 'C') {
            sizes[i] = 1; // sizeof(char)
            serializers[i] = SerializeAttribute<char>;
        } else if (schema[i].tpe[0] == 'S') {
            sizes[i] = schema[i].size;
            serializers[i] = SerializeAttribute<const char *>;
        }
        delimiters[i] = (i == schemaSize - 1) ? '\n' : ','; // Newline for the last attribute, commas for others
    }

    while (true) {
        // Poll a decompressed buffer from the queue
        int bufferId = runtimeEnv->decompressedBufferIds->pop();
        //spdlog::get("XDBC.CSVSINK")->info("CSV Serializer/Writer {} got decompressed buff {}", thr, bufferId);

        if (bufferId == -1) break; // Termination signal

        const auto &decompressedBuffer = (*runtimeEnv->bp)[bufferId];
        size_t tuplesToRead = *reinterpret_cast<const size_t *>(decompressedBuffer.data());
        const char *dataPtr = reinterpret_cast<const char *>(decompressedBuffer.data() + sizeof(size_t));

        std::vector<char> tupleBuffer(250); // Temporary buffer for one tuple
        for (size_t i = 0; i < tuplesToRead; ++i) {
            size_t tupleOffset = 0;
            for (size_t j = 0; j < schemaSize; ++j) {
                // Serialize each attribute into the tuple buffer
                tupleOffset += serializers[j](dataPtr, tupleBuffer.data() + tupleOffset, sizes[j], delimiters[j]);
                dataPtr += sizes[j];
            }
            // Write the serialized tuple directly to the file
            outputFile.write(tupleBuffer.data(), tupleOffset);
        }


        // Push the buffer ID back to the free buffer queue
        runtimeEnv->freeBufferIds->push(bufferId);
    }

    outputFile.close();
    runtimeEnv->finishedWriteThreads.fetch_add(1);

    spdlog::get("XDBC.CSVSINK")->info("CSV Writer/Serializer thread {} finished", thr);
}

