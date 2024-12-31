#ifndef SINK_INTERFACE_H
#define SINK_INTERFACE_H

#include <vector>
#include <cstddef> // for std::byte

class SinkInterface {
public:
    virtual ~SinkInterface() = default;

    virtual void serialize(int thr) = 0;

    virtual void write(int thr) = 0;

};

#endif // SINK_INTERFACE_H
