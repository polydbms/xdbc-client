#ifndef BUFFERGENERATOR_H
#define BUFFERGENERATOR_H

#include <vector>
#include <unordered_map>
#include <queue>
#include <mutex>
#include <stdexcept>
#include "customQueue.h"

class BufferGenerator
{
public:
    // Constructor
    BufferGenerator()
        : max_bufID(0) {}

    // Create a buffer and map it to the given token_bufferID
    std::vector<std::byte> &createBuffer(int token_bufferID, size_t bufSize)
    {
        std::lock_guard<std::mutex> lock(mutex);
        usedBuffers[token_bufferID] = std::vector<std::byte>(bufSize);
        return usedBuffers[token_bufferID];
    }

    // Get a reference to the buffer mapped to the token_bufferID
    std::vector<std::byte> &getBuffer(int token_bufferID)
    {
        std::lock_guard<std::mutex> lock(mutex);
        if (usedBuffers.find(token_bufferID) == usedBuffers.end())
        {
            throw std::runtime_error("Invalid buffer ID");
        }
        return usedBuffers[token_bufferID];
    }

    // Get the current buffer size
    size_t getBufferSize() const { return bufferSize; }

    // Get the current buffer size
    void setBufferSize(size_t new_bufSize)
    {
        std::lock_guard<std::mutex> lock(mutex);
        bufferSize = new_bufSize;
    }

    // Add free buffers to the external queue
    void add_freeBuffers(std::shared_ptr<customQueue<int>> freeBufferIds, int change_freeBuf)
    {
        std::lock_guard<std::mutex> lock(mutex);
        for (int i = max_bufID; i < (max_bufID + change_freeBuf); ++i)
        {
            freeBufferIds->push(i);
        }
        max_bufID += change_freeBuf;
    }

    // Remove free buffers from the external queue
    void remove_freeBuffers(std::shared_ptr<customQueue<int>> freeBufferIds, int change_freeBuf)
    {
        std::lock_guard<std::mutex> lock(mutex);
        for (int i = 0; i < change_freeBuf; ++i)
        {
            if (freeBufferIds->size() > 0)
            {
                freeBufferIds->pop();
            }
        }
    }

private:
    size_t bufferSize;                                           // Current size of each buffer
    int max_bufID;                                               // Highest buffer ID assigned
    std::unordered_map<int, std::vector<std::byte>> usedBuffers; // Map of bufferID to buffer
    std::mutex mutex;                                            // Mutex for thread safety
};

#endif // BUFFERGENERATOR_H
