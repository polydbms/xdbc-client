#include "CSVSink.h"
#include <fstream>
#include <spdlog/spdlog.h>
#include "spdlog/sinks/stdout_color_sinks.h"
#include <sstream>
#include "parquet/arrow/reader.h"
#include <arrow/array.h>
#include <arrow/table.h>
#include <arrow/type.h>
#include <arrow/ipc/api.h> // For serialization/deserialization
#include <arrow/io/api.h>
#include "deserializers_parquet.h"

CsvSink::CsvSink(std::string baseFilename, xdbc::RuntimeEnv *runtimeEnv)
    : baseFilename(std::move(baseFilename)), runtimeEnv(runtimeEnv)
{
    bufferPool = runtimeEnv->bp;
    auto console = spdlog::stdout_color_mt("XDBC.CSVSINK");
}

void CsvSink::serialize(int thr)
{
    runtimeEnv->pts->push(xdbc::ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "ser", "start"});

    size_t writtenBuffers = 0;
    size_t writtenTuples = 0;

    spdlog::get("XDBC.CSVSINK")->info("CSV Serializer started thread {}", thr);

    if (runtimeEnv->skip_serializer)
    {

        while (true)
        {
            int bufferId = runtimeEnv->decompressedBufferIds->pop();
            runtimeEnv->pts->push(
                xdbc::ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "ser", "pop"});

            if (bufferId == -1)
                break;

            runtimeEnv->pts->push(
                xdbc::ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "ser", "push"});

            runtimeEnv->serializedBufferIds->push(bufferId);
            writtenBuffers++;
        }
    }
    else
    {
        const auto &schema = runtimeEnv->schema;
        size_t schemaSize = schema.size();

        // Precompute sizes, serializers, and maximum tuple size
        std::vector<size_t> sizes(schemaSize);
        std::vector<char> delimiters(schemaSize);
        using SerializeFunc = size_t (*)(const void *, char *, size_t, char);
        std::vector<SerializeFunc> serializers(schemaSize);

        using ParquetSerializeFunc = size_t (*)(parquet::StreamReader &stream, char *, size_t, char);
        std::vector<ParquetSerializeFunc> parquetSerializers(schemaSize);

        size_t maxTupleSize = 0;
        for (size_t i = 0; i < schemaSize; ++i)
        {

            if (schema[i].tpe[0] == 'I')
            {
                sizes[i] = 4; // sizeof(int)
                serializers[i] = SerializeAttribute<int>;
                parquetSerializers[i] = SerializeParquetAttribute<int>;
                maxTupleSize += 12; // Pessimistic size for integer serialization
            }
            else if (schema[i].tpe[0] == 'D')
            {
                sizes[i] = 8; // sizeof(double)
                serializers[i] = SerializeAttribute<double>;
                parquetSerializers[i] = SerializeParquetAttribute<double>;
                maxTupleSize += 24; // Pessimistic size for double serialization
            }
            else if (schema[i].tpe[0] == 'C')
            {
                sizes[i] = 1; // sizeof(char)
                serializers[i] = SerializeAttribute<char>;
                parquetSerializers[i] = SerializeParquetAttribute<std::string>;
                maxTupleSize += 2; // Single character + delimiter
            }
            else if (schema[i].tpe[0] == 'S')
            {
                sizes[i] = schema[i].size;
                serializers[i] = SerializeAttribute<const char *>;
                parquetSerializers[i] = SerializeParquetAttribute<std::string>;
                maxTupleSize += schema[i].size + 1; // Fixed string size + delimiter
            }

            delimiters[i] = (i == schemaSize - 1) ? '\n' : ','; // Newline for the last attribute, commas for others
        }

        // TODO: only for format 1
        std::vector<size_t> columnOffsets(schemaSize);
        size_t totalRowSize = 0;
        for (size_t j = 0; j < schemaSize; ++j)
        {
            columnOffsets[j] = totalRowSize;
            totalRowSize += sizes[j];
        }

        // TODO: only for format 3 (arrow)
        std::vector<std::function<const char *(int)>> dataExtractors(schemaSize);

        size_t bufferSizeInBytes = runtimeEnv->buffer_size * 1024;

        while (true)
        {

            int bufferId = runtimeEnv->decompressedBufferIds->pop();

            if (bufferId == -1)
                break;

            runtimeEnv->pts->push(
                xdbc::ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "ser", "pop"});

            const auto &inBufferPtr = (*bufferPool)[bufferId];
            auto header = *reinterpret_cast<const xdbc::Header *>(inBufferPtr.data());
            if (header.totalTuples > runtimeEnv->tuples_per_buffer || header.totalSize > runtimeEnv->buffer_size * 1024)
                spdlog::get("XDBC.CSVSINK")->error("Size of buffer larger than expected tuples:{}/{}, size {}/{}", header.totalTuples, runtimeEnv->tuples_per_buffer, header.totalSize, runtimeEnv->buffer_size * 1024);

            const char *basePtr = reinterpret_cast<const char *>(inBufferPtr.data() + sizeof(xdbc::Header));

            if (header.intermediateFormat == 1 || header.intermediateFormat == 2 || header.intermediateFormat == 3)
            {
                // spdlog::get("XDBC.CSVSINK")->info("using iformat 1,2,3");

                std::vector<std::shared_ptr<arrow::Array>> arrays; // To store Arrow arrays for `iformat == 3`

                if (runtimeEnv->iformat == 3)
                {
                    // Deserialize Arrow RecordBatch from raw memory
                    const auto *bufferData = reinterpret_cast<const uint8_t *>(inBufferPtr.data() +
                                                                               sizeof(xdbc::Header));

                    auto arrowBuffer = std::make_shared<arrow::Buffer>(bufferData, header.totalSize);
                    auto bufferReader = std::make_shared<arrow::io::BufferReader>(arrowBuffer);

                    // Open a FileReader or StreamReader
                    auto reader = arrow::ipc::RecordBatchFileReader::Open(bufferReader).ValueOrDie();
                    auto arrowSchema = reader->schema();

                    auto recordBatch = reader->ReadRecordBatch(0).ValueOrDie();

                    // Extract column arrays
                    arrays = recordBatch->columns();

                    // Precompute accessors for Arrow arrays
                    for (size_t j = 0; j < schemaSize; ++j)
                    {
                        switch (arrays[j]->type_id())
                        {
                        case arrow::Type::INT32:
                        {
                            auto intArray = std::static_pointer_cast<arrow::Int32Array>(arrays[j]);
                            dataExtractors[j] = [intArray](int i)
                            {
                                return reinterpret_cast<const char *>(intArray->raw_values() + i);
                            };
                            break;
                        }
                        case arrow::Type::DOUBLE:
                        {
                            auto doubleArray = std::static_pointer_cast<arrow::DoubleArray>(arrays[j]);
                            dataExtractors[j] = [doubleArray](int i)
                            {
                                return reinterpret_cast<const char *>(doubleArray->raw_values() + i);
                            };
                            break;
                        }
                        case arrow::Type::STRING:
                        {
                            auto stringArray = std::static_pointer_cast<arrow::StringArray>(arrays[j]);
                            dataExtractors[j] = [stringArray](int i)
                            {
                                return stringArray->GetString(i).c_str();
                            };
                            break;
                        }
                        case arrow::Type::FIXED_SIZE_BINARY:
                        {
                            auto fixedArray = std::static_pointer_cast<arrow::FixedSizeBinaryArray>(arrays[j]);
                            dataExtractors[j] = [fixedArray](int i)
                            {
                                return reinterpret_cast<const char *>(fixedArray->GetValue(i));
                            };
                            break;
                        }
                        default:
                            throw std::runtime_error("Unsupported Arrow array type for serialization.");
                        }
                    }
                }

                int serializedBufferId = runtimeEnv->freeBufferIds->pop();

                auto &outBuffer = (*bufferPool)[serializedBufferId];
                char *writePtr = reinterpret_cast<char *>(outBuffer.data() + sizeof(xdbc::Header));
                size_t totalSerializedBytes = 0;

                std::vector<const char *> columnStartPointers(schemaSize);
                size_t cumulativeOffset = 0;
                for (size_t k = 0; k < schemaSize; ++k)
                {
                    columnStartPointers[k] = basePtr + cumulativeOffset;
                    // TODO: check this, maybe write header.totalTuples instead of tuples_per_buffer
                    cumulativeOffset +=
                        runtimeEnv->tuples_per_buffer * sizes[k]; // Move by the total size of this column
                }

                for (size_t i = 0; i < header.totalTuples; ++i)
                {
                    if (totalSerializedBytes + maxTupleSize > bufferSizeInBytes)
                    {
                        // Buffer is full, push it to the queue
                        xdbc::Header head{};
                        head.totalSize = totalSerializedBytes;
                        std::memcpy(outBuffer.data(), &head, sizeof(xdbc::Header));
                        runtimeEnv->pts->push(
                            xdbc::ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "ser",
                                                      "push"});
                        runtimeEnv->serializedBufferIds->push(serializedBufferId);

                        // Fetch a new buffer
                        serializedBufferId = runtimeEnv->freeBufferIds->pop();
                        outBuffer = (*bufferPool)[serializedBufferId];
                        writePtr = reinterpret_cast<char *>(outBuffer.data() + sizeof(xdbc::Header));
                        totalSerializedBytes = 0;
                        writtenBuffers++;
                    }

                    for (size_t j = 0; j < schemaSize; ++j)
                    {
                        const char *dataPtr;
                        if (runtimeEnv->iformat == 1)
                        {
                            dataPtr = basePtr + i * totalRowSize + columnOffsets[j];
                        }
                        else if (runtimeEnv->iformat == 2)
                        {
                            dataPtr = columnStartPointers[j] + i * sizes[j];
                        }
                        else if (runtimeEnv->iformat == 3)
                        {
                            dataPtr = dataExtractors[j](i);
                        }

                        totalSerializedBytes += serializers[j](
                            dataPtr, writePtr + totalSerializedBytes, sizes[j], delimiters[j]);
                    }
                }
                writtenTuples += header.totalTuples;

                // Write any remaining data to the buffer
                if (totalSerializedBytes > 0)
                {

                    xdbc::Header head{};
                    head.totalSize = totalSerializedBytes;
                    std::memcpy(outBuffer.data(), &head, sizeof(xdbc::Header));
                    runtimeEnv->pts->push(
                        xdbc::ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "ser", "push"});

                    runtimeEnv->serializedBufferIds->push(serializedBufferId);
                    writtenBuffers++;
                }
            }
            if (header.intermediateFormat == 4)
            {
                // spdlog::get("XDBC.CSVSINK")->info("using iformat 1,2,3");
                auto writeBuff = runtimeEnv->freeBufferIds->pop();

                char *writeBuffPtr = reinterpret_cast<char *>((*bufferPool)[writeBuff].data() + sizeof(xdbc::Header));

                int numRows = 0;
                size_t parquetFileSize;

                parquetFileSize = header.totalSize;

                auto arrow_buffer = std::make_shared<arrow::Buffer>(reinterpret_cast<const uint8_t *>(basePtr),
                                                                    parquetFileSize);
                auto buffer_reader = std::make_shared<arrow::io::BufferReader>(arrow_buffer);

                // Initialize the StreamReader
                parquet::StreamReader stream{parquet::ParquetFileReader::Open(buffer_reader)};

                size_t totalSerializedBytes = 0;
                while (!stream.eof())
                {

                    for (size_t j = 0; j < schemaSize; ++j)
                    {
                        totalSerializedBytes += parquetSerializers[j](stream, writeBuffPtr + totalSerializedBytes,
                                                                      sizes[j], delimiters[j]);
                    }
                    stream >> parquet::EndRow;
                    numRows++;

                    if (totalSerializedBytes + 1000 > runtimeEnv->buffer_size * 1024)
                    {
                        xdbc::Header head{};
                        head.totalSize = totalSerializedBytes;
                        head.totalTuples = numRows;

                        std::memcpy((*bufferPool)[writeBuff].data(), &head, sizeof(xdbc::Header));

                        runtimeEnv->pts->push(
                            xdbc::ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "ser",
                                                      "push"});

                        runtimeEnv->serializedBufferIds->push(writeBuff);

                        writeBuff = runtimeEnv->freeBufferIds->pop();
                        writeBuffPtr = reinterpret_cast<char *>((*bufferPool)[writeBuff].data() + sizeof(xdbc::Header));

                        totalSerializedBytes = 0;
                        numRows = 0;
                    }
                }

                // write remaining
                if (totalSerializedBytes > 0)
                {
                    xdbc::Header head{};
                    head.totalSize = totalSerializedBytes;
                    head.totalTuples = numRows;

                    std::memcpy((*bufferPool)[writeBuff].data(), &head, sizeof(xdbc::Header));
                    runtimeEnv->serializedBufferIds->push(writeBuff);
                }
            }

            // Release decompressed buffer back to freeBufferIds
            runtimeEnv->freeBufferIds->push(bufferId);
        }
    }

    runtimeEnv->pts->push(xdbc::ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "ser", "end"});

    spdlog::get("XDBC.CSVSINK")->info("CSV Serializer stopping thread {}, written buffers: {}, tuples: {}", thr, writtenBuffers, writtenTuples);

    runtimeEnv->finishedSerializerThreads.fetch_add(1);
    // if (runtimeEnv->finishedSerializerThreads == runtimeEnv->ser_parallelism)
    // {
    //     for (int i = 0; i < runtimeEnv->write_parallelism; ++i)
    //     {
    //         // runtimeEnv->serializedBufferIds->push(-1); // Termination signal
    //     }
    // }
}

void CsvSink::write(int thr)
{

    runtimeEnv->pts->push(xdbc::ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "write", "start"});

    spdlog::get("XDBC.CSVSINK")->info("CSV Writer started thread {}", thr);
    std::ofstream outputFile;
    std::string fileName = baseFilename + "_thread_" + std::to_string(thr) + ".csv";
    size_t buffersWritten = 0;

    outputFile.open(fileName, std::ios::out | std::ios::binary);
    if (!outputFile.is_open())
    {
        throw std::runtime_error("Failed to open output file: " + fileName);
    }

    while (true)
    {
        int bufferId = runtimeEnv->serializedBufferIds->pop();
        runtimeEnv->pts->push(
            xdbc::ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "write", "pop"});

        // spdlog::get("XDBC.CSVSINK")->info("CSV Writer {} got serialized buff {}", thr, bufferId);

        if (bufferId == -1)
            break;

        const auto &serializedBuffer = (*bufferPool)[bufferId];
        auto header = *reinterpret_cast<const xdbc::Header *>(serializedBuffer.data());

        const char *dataPtr = reinterpret_cast<const char *>(serializedBuffer.data() + sizeof(xdbc::Header));
        outputFile.write(dataPtr, header.totalSize);

        runtimeEnv->pts->push(
            xdbc::ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "write", "push"});

        runtimeEnv->freeBufferIds->push(bufferId);
        buffersWritten++;
    }

    outputFile.close();
    runtimeEnv->finishedWriteThreads.fetch_add(1);
    runtimeEnv->pts->push(xdbc::ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "write", "end"});
    spdlog::get("XDBC.CSVSINK")->info("CSV Writer thread {} wrote buffers: {}", thr, buffersWritten);
}