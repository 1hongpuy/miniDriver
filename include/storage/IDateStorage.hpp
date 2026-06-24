#pragma once
#include <cstddef>
#include <string>




namespace miniKV {
namespace storage {

class IDateStorage{
public:
    virtual ~IDateStorage() = default;

    virtual bool init() = 0;

    virtual bool writeData(const std::string& hash, const char* data, size_t len, size_t offset = 0) = 0;

    virtual bool readData(const std::string& hash, std::string& out_data, size_t len, size_t offset = 0) = 0;

    virtual std::string filePath(const std::string& hash) const  = 0;

    virtual bool removeByHash(const std::string& hash) const = 0;
};


}
}





















