#pragma once

#include <string>



namespace miniKV {

namespace  storage{

class IStorage{
public:
    virtual ~IStorage() = default;
    virtual bool set(const std::string& key, const std::string& value) = 0;
    virtual std::string get(const std::string& key) = 0;
    virtual bool del(const std::string& key) = 0;
};




}

}




















