#ifndef CSV_SINK_H
#define CSV_SINK_H

#include "../../xdbc/SinkInterface.h"
#include "../../xdbc/RuntimeEnv.h"
#include <fstream>
#include <vector>
#include <string>
#include <cstddef>
#include <cstring>

class CsvSink : public SinkInterface {
private:
    xdbc::RuntimeEnv *runtimeEnv;
    std::vector<std::vector<std::byte>> *bufferPool;
    std::string baseFilename;

public:
    CsvSink(std::string baseFilename, xdbc::RuntimeEnv *runtimeEnv);

    void serialize(int thr) override;

    void write(int thr) override;

};

template<typename T>
inline size_t SerializeAttribute(const void *data, char *buffer, size_t len, char delimiter);

// Specialization for `int`
template<>
inline size_t SerializeAttribute<int>(const void *data, char *buffer, size_t, char delimiter) {
    int bytesWritten = sprintf(buffer, "%d", *reinterpret_cast<const int *>(data)); // Serialize integer
    buffer[bytesWritten] = delimiter; // Append delimiter
    return bytesWritten + 1; // Include the delimiter
}

// Specialization for `double`
template<>
inline size_t SerializeAttribute<double>(const void *data, char *buffer, size_t, char delimiter) {
    int bytesWritten = sprintf(buffer, "%.2f", *reinterpret_cast<const double *>(data)); // Serialize double
    buffer[bytesWritten] = delimiter; // Append delimiter
    return bytesWritten + 1; // Include the delimiter
}

// Specialization for `char`
template<>
inline size_t SerializeAttribute<char>(const void *data, char *buffer, size_t, char delimiter) {
    buffer[0] = *reinterpret_cast<const char *>(data); // Serialize single character
    buffer[1] = delimiter; // Append delimiter
    return 2; // 1 char + 1 delimiter
}

// Specialization for fixed-size strings (e.g., STRING(n))
template<>
inline size_t SerializeAttribute<const char *>(const void *data, char *buffer, size_t len, char delimiter) {
    const char *str = reinterpret_cast<const char *>(data);

    // Find the actual length of the string (up to null termination or max length)
    size_t actualLen = strnlen(str, len);

    // Copy only up to the actual length
    memcpy(buffer, str, actualLen);

    // Add the delimiter directly after the valid part of the string
    buffer[actualLen] = delimiter;

    // Return the total bytes written (actual length + 1 for the delimiter)
    return actualLen + 1;
}


#endif // CSV_SINK_H
