#ifndef XDBC_PQSINK_H
#define XDBC_PQSINK_H

#include "../../xdbc/SinkInterface.h"
#include "../../xdbc/RuntimeEnv.h"

class PQSink : public SinkInterface {
private:
    xdbc::RuntimeEnv *runtimeEnv;
    std::vector<std::vector<std::byte>> *bufferPool;
    std::string baseFilename;

public:
    PQSink(const std::string& baseFilename, xdbc::RuntimeEnv *runtimeEnv);

    void serialize(int thr) override;

    void write(int thr) override;

};


#endif //XDBC_PQSINK_H
