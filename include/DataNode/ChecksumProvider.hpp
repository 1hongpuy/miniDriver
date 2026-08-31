#pragma once

#include "DataNode/ChunkWriteTypes.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

namespace miniKV::datanode {

class ChecksumContext {
public:
    virtual ~ChecksumContext() = default;
    virtual bool update(const char* bytes, size_t size) = 0;
    virtual bool finalizeHex(std::string& digest) = 0;
};

class ChecksumProvider {
public:
    virtual ~ChecksumProvider() = default;
    virtual ChunkChecksumType type() const = 0;
    virtual std::unique_ptr<ChecksumContext> create() const = 0;
};

const ChecksumProvider& checksumProvider(ChunkChecksumType type);
bool isSupportedChecksumType(ChunkChecksumType type);
bool validateChecksumDigest(ChunkChecksumType type, const std::string& digest);
std::string crc32cHex(const char* bytes, size_t size);

}  // namespace miniKV::datanode
