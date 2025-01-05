#include "PQSink.h"
#include <spdlog/spdlog.h>
#include <fstream>
#include <filesystem>
#include "spdlog/sinks/stdout_color_sinks.h"

PQSink::PQSink(const std::string &baseFilename, xdbc::RuntimeEnv *runtimeEnv)
        : baseFilename(baseFilename), runtimeEnv(runtimeEnv) {
    bufferPool = runtimeEnv->bp;
    auto console = spdlog::stdout_color_mt("XDBC.PQSINK");

    std::string folderPath = baseFilename + "_" + runtimeEnv->table;

    try {
        // Check if the folder exists
        if (std::filesystem::exists(folderPath)) {
            spdlog::get("XDBC.PQSINK")->info("Folder exists, deleting: {}", folderPath);
            std::filesystem::remove_all(folderPath); // Delete the folder and its contents
        }

        std::filesystem::create_directories(folderPath);
        spdlog::get("XDBC.PQSINK")->info("Created directory: {}", folderPath);

    }
    catch (const std::filesystem::filesystem_error &e) {
        spdlog::get("XDBC.PQSINK")->error("Error managing folder: {}", e.what());
    }


}

void PQSink::serialize(int thr) {
    runtimeEnv->pts->push(
            xdbc::ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "ser", "start"});

    size_t writtenBuffers = 0;

    if (runtimeEnv->skip_serializer) {

        while (true) {
            int bufferId = runtimeEnv->decompressedBufferIds->pop();
            runtimeEnv->pts->push(
                    xdbc::ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "ser", "pop"});

            if (bufferId == -1) break;

            runtimeEnv->pts->push(
                    xdbc::ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "ser", "push"});

            runtimeEnv->serializedBufferIds->push(bufferId);
            writtenBuffers++;
        }

    } else {
        spdlog::get("XDBC.PQSINK")->error("PQSINK currently does not support serialization");
    }
    runtimeEnv->pts->push(
            xdbc::ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "ser", "end"});

    runtimeEnv->finishedSerializerThreads.fetch_add(1);
    if (runtimeEnv->finishedSerializerThreads == runtimeEnv->ser_parallelism) {
        for (int i = 0; i < runtimeEnv->write_parallelism; ++i) {
            runtimeEnv->serializedBufferIds->push(-1); // Termination signal
        }
    }
}

void PQSink::write(int thr) {

    //for now each buffer is a pq file
    runtimeEnv->pts->push(xdbc::ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "write", "start"});

    spdlog::get("XDBC.PQSINK")->info("PQ Writer started thread {}", thr);

    size_t buffersWritten = 0;

    int fileSuffix = 0;
    while (true) {
        int bufferId = runtimeEnv->serializedBufferIds->pop();
        runtimeEnv->pts->push(
                xdbc::ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "write", "pop"});

        if (bufferId == -1) break;

        std::string folderPath = baseFilename + "_" + runtimeEnv->table;


        std::string fileName = folderPath + "/" + runtimeEnv->table + "_" + std::to_string(fileSuffix) + ".parquet";
        std::ofstream outputFile;
        outputFile.open(fileName, std::ios::out | std::ios::binary);
        if (!outputFile.is_open()) {
            throw std::runtime_error("Failed to open output file: " + fileName);
        }
        fileSuffix++;

        const auto &serializedBuffer = (*bufferPool)[bufferId];
        auto header = *reinterpret_cast<const xdbc::Header *>(serializedBuffer.data());

        const char *dataPtr = reinterpret_cast<const char *>(serializedBuffer.data() + sizeof(xdbc::Header));
        outputFile.write(dataPtr, header.totalSize);

        runtimeEnv->pts->push(
                xdbc::ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "write", "push"});

        runtimeEnv->freeBufferIds->push(bufferId);
        buffersWritten++;
        outputFile.close();
    }


    runtimeEnv->finishedWriteThreads.fetch_add(1);
    runtimeEnv->pts->push(xdbc::ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "write", "end"});
    spdlog::get("XDBC.PQSINK")->info("PQ Writer thread {} wrote buffers: {}", thr, buffersWritten);

}