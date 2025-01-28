#include "PQSink.h"
#include <spdlog/spdlog.h>
#include <fstream>
#include <filesystem>
#include <arrow/api.h>
#include <arrow/ipc/reader.h>
#include <arrow/io/memory.h>
#include <parquet/stream_writer.h>
#include <parquet/schema.h>
#include <arrow/io/api.h>
#include <parquet/arrow/writer.h>
#include "arrow/util/type_fwd.h"
#include "spdlog/sinks/stdout_color_sinks.h"

std::shared_ptr<parquet::schema::GroupNode>
CreateParquetSchema(const std::vector<xdbc::SchemaAttribute> &schemaAttributes) {
    parquet::schema::NodeVector fields;

    for (const auto &attr: schemaAttributes) {
        if (attr.tpe == "INT") {
            fields.push_back(parquet::schema::PrimitiveNode::Make(
                    attr.name, parquet::Repetition::REQUIRED, parquet::Type::INT32, parquet::ConvertedType::INT_32));
        } else if (attr.tpe == "DOUBLE") {
            fields.push_back(parquet::schema::PrimitiveNode::Make(
                    attr.name, parquet::Repetition::REQUIRED, parquet::Type::DOUBLE, parquet::ConvertedType::NONE));
        } else if (attr.tpe == "STRING") {
            fields.push_back(parquet::schema::PrimitiveNode::Make(
                    attr.name, parquet::Repetition::REQUIRED, parquet::Type::BYTE_ARRAY, parquet::ConvertedType::UTF8));
        } else if (attr.tpe == "CHAR") {
            if (attr.size <= 0) {
                throw std::invalid_argument("Fixed-size STRING/CHAR must have a positive size.");
            }
            fields.push_back(parquet::schema::PrimitiveNode::Make(
                    attr.name, parquet::Repetition::REQUIRED, parquet::Type::FIXED_LEN_BYTE_ARRAY,
                    parquet::ConvertedType::NONE, attr.size));
        } else {
            throw std::invalid_argument("Unsupported type: " + attr.tpe);
        }
    }

    return std::static_pointer_cast<parquet::schema::GroupNode>(
            parquet::schema::GroupNode::Make("schema", parquet::Repetition::REQUIRED, fields));
}


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

    std::vector<size_t> sizes(runtimeEnv->schema.size());
    for (size_t i = 0; i < runtimeEnv->schema.size(); ++i) {

        if (runtimeEnv->schema[i].tpe[0] == 'I') {
            sizes[i] = 4; // sizeof(int)
        } else if (runtimeEnv->schema[i].tpe[0] == 'D') {
            sizes[i] = 8; // sizeof(double)
        } else if (runtimeEnv->schema[i].tpe[0] == 'C') {
            sizes[i] = 1; // sizeof(char)
        } else if (runtimeEnv->schema[i].tpe[0] == 'S') {
            sizes[i] = runtimeEnv->schema[i].size;
        }
    }

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

    } else if (runtimeEnv->iformat == 3) { // Format == 3: Arrow
        //TODO: check
        spdlog::get("XDBC.PQSINK")->info("PQSINK Parquet serialization started.");
        while (true) {
            int bufferId = runtimeEnv->decompressedBufferIds->pop();
            int outBufferId = runtimeEnv->freeBufferIds->pop();
            auto &outBuffer = (*bufferPool)[outBufferId];
            runtimeEnv->pts->push(
                    xdbc::ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "ser", "pop"});
            if (bufferId == -1) break;
            const auto &inBufferPtr = (*bufferPool)[bufferId];
            auto header = *reinterpret_cast<const xdbc::Header *>(inBufferPtr.data());

            const auto *bufferData = reinterpret_cast<const uint8_t *>(inBufferPtr.data() + sizeof(xdbc::Header));
            auto arrowBuffer = std::make_shared<arrow::Buffer>(bufferData, header.totalSize);

            auto bufferReader = std::make_shared<arrow::io::BufferReader>(arrowBuffer);
            std::shared_ptr<arrow::ipc::RecordBatchFileReader> fileReader;
            auto fileReaderResult = arrow::ipc::RecordBatchFileReader::Open(bufferReader);
            if (!fileReaderResult.ok()) {
                spdlog::error("Error opening RecordBatchFileReader: {}", fileReaderResult.status().ToString());
                return;
            }
            fileReader = fileReaderResult.ValueOrDie();

            // Assuming the file contains one RecordBatch, read it
            std::shared_ptr<arrow::RecordBatch> recordBatch;
            auto recordBatchResult = fileReader->ReadRecordBatch(0);
            if (!recordBatchResult.ok()) {
                spdlog::error("Error reading RecordBatch: {}", recordBatchResult.status().ToString());
                return;
            }
            recordBatch = recordBatchResult.ValueOrDie();

            // Step 2: Prepare to write the RecordBatch as a Parquet file into memory
            auto outputBuffer = std::make_shared<arrow::io::FixedSizeBufferWriter>(
                    std::make_shared<arrow::Buffer>(reinterpret_cast<uint8_t *>(outBuffer.data()),
                                                    runtimeEnv->buffer_size * 1024 - sizeof(xdbc::Header))
            );

            // Step 3: Configure Parquet writer properties
            auto writerProperties = parquet::WriterProperties::Builder()
                    .compression(arrow::Compression::SNAPPY)->build();
            auto arrowWriterProperties = parquet::ArrowWriterProperties::Builder()
                    .store_schema()->build();

            // Step 4: Write the RecordBatch as a Parquet file
            auto tableResult = arrow::Table::FromRecordBatches({recordBatch});
            if (!tableResult.ok()) {
                spdlog::error("Error converting RecordBatch to Table: {}", tableResult.status().ToString());
                return;
            }
            auto table = tableResult.ValueOrDie();

            auto writeStatus = parquet::arrow::WriteTable(
                    *table,                                 // Arrow table
                    arrow::default_memory_pool(),          // Memory pool
                    outputBuffer,                          // Output stream
                    /*chunk_size=*/3,                      // Chunk size
                    writerProperties,                      // Writer properties
                    arrowWriterProperties                  // Arrow writer properties
            );

            if (!writeStatus.ok()) {
                spdlog::error("Error writing Parquet file: {}", writeStatus.ToString());
            }

            // Step 5: Finalize the buffer writer
            auto closeStatus = outputBuffer->Close();
            if (!closeStatus.ok()) {
                spdlog::error("Error finalizing output buffer: {}", closeStatus.ToString());
            }

            runtimeEnv->pts->push(
                    xdbc::ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "ser", "push"});
            runtimeEnv->serializedBufferIds->push(outBufferId);
            runtimeEnv->freeBufferIds->push(bufferId);
        }

    } else if (runtimeEnv->iformat == 2) {

        while (true) {
            int inBufferId = runtimeEnv->decompressedBufferIds->pop();

            if (inBufferId == -1)
                break;
            int outBufferId = runtimeEnv->freeBufferIds->pop();
            auto &inBuffer = (*bufferPool)[inBufferId];
            auto &outBuffer = (*bufferPool)[outBufferId];

            auto header = *reinterpret_cast<const xdbc::Header *>(inBuffer.data());
            const char *basePtr = reinterpret_cast<const char *>(inBuffer.data() + sizeof(xdbc::Header));

            std::vector<size_t> columnOffsets(runtimeEnv->schema.size());
            size_t totalRowSize = 0;
            for (size_t j = 0; j < runtimeEnv->schema.size(); ++j) {
                columnOffsets[j] = totalRowSize;
                totalRowSize += sizes[j];
            }

            std::vector<const char *> columnStartPointers(runtimeEnv->schema.size());
            size_t cumulativeOffset = 0;
            for (size_t k = 0; k < runtimeEnv->schema.size(); ++k) {
                columnStartPointers[k] = basePtr + cumulativeOffset;
                cumulativeOffset += runtimeEnv->tuples_per_buffer * sizes[k]; // Move by the total size of this column

            }

            {
                auto outputBuffer = std::make_shared<arrow::MutableBuffer>(
                        reinterpret_cast<uint8_t *>(outBuffer.data()),
                        runtimeEnv->buffer_size * 1024);
                auto bufferWriter = std::make_shared<arrow::io::FixedSizeBufferWriter>(outputBuffer);

                auto parquetSchema = CreateParquetSchema(runtimeEnv->schema);
                parquet::WriterProperties::Builder writerPropertiesBuilder;
                //writerPropertiesBuilder.compression(parquet::Compression::SNAPPY);
                parquet::StreamWriter streamWriter{
                        parquet::ParquetFileWriter::Open(bufferWriter, parquetSchema, writerPropertiesBuilder.build())};

                //streamWriter.SetMaxRowGroupSize(header.totalTuples);
                for (size_t i = 0; i < header.totalTuples; ++i) {

                    for (size_t j = 0; j < runtimeEnv->schema.size(); ++j) {
                        const char *dataPtr;
                        if (runtimeEnv->iformat == 1) {
                            dataPtr = basePtr + i * totalRowSize + columnOffsets[j];
                        } else if (runtimeEnv->iformat == 2) {
                            dataPtr = columnStartPointers[j] + i * sizes[j];
                        }

                        const auto &attr = runtimeEnv->schema[j];
                        if (attr.tpe[0] == 'I') {
                            int32_t value = *reinterpret_cast<const int32_t *>(dataPtr);
                            streamWriter << 1;
                        } else if (attr.tpe[0] == 'D') {
                            double value = *reinterpret_cast<const double *>(dataPtr);
                            streamWriter << 2.5;
                        } else if (attr.tpe[0] == 'S') {
                            std::string value(dataPtr, attr.size); // Fixed-size string
                            streamWriter << "test";
                        } else if (attr.tpe[0] == 'C') {
                            char t; // Single character as string
                            streamWriter << 'a';
                        } else {
                            throw std::invalid_argument("Unsupported type: " + attr.tpe);
                        }

                    }

                    streamWriter << parquet::EndRow;
                }

                //streamWriter << parquet::EndRowGroup;

                // Finalize and close the buffer writer
                auto closeStatus = bufferWriter->Close();
                size_t totalSerializedBytes = outputBuffer->size();

                // Update the header with the total size
                auto *outHeader = reinterpret_cast<xdbc::Header *>(outBuffer.data());
                outHeader->totalSize = totalSerializedBytes;
            }

            runtimeEnv->serializedBufferIds->push(outBufferId);
            runtimeEnv->freeBufferIds->push(inBufferId);
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