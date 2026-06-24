#pragma once
#include <cstddef>
#include <string>
#include <vector>

namespace miniKV {
namespace storage {

struct MetaInfo{
    std::string file_name;
    std::string file_hash;
    std::string file_path;
    std::string file_author;
    std::string file_time;
    size_t      file_size;
    std::string content_type;
};

class IMetaStorage{
public:
    //纯虚函数，代表只是个接口，没有实际实现，所以对于派生类来说，需要实现具体函数，不然无法创建实列
    virtual ~IMetaStorage() = default;
    //初始化数据库连接
    virtual bool init() = 0;
    //保存元数据
    virtual bool saveMeta(const std::string& key, const MetaInfo& meta) = 0;
    //读取元数据
    virtual bool getMeta(const std::string& key, MetaInfo& meta) = 0;

    virtual bool removeMate(const std::string& key) = 0;

    virtual bool moveFile(const std::string& hash, const std::string& newPath) = 0;

    virtual std::vector<MetaInfo> listByPath(const std::string& path) const = 0;

    virtual std::vector<std::string> listSubDirs(const std::string& path) const = 0;
    
    virtual std::vector<MetaInfo> listAll() const = 0;

    virtual bool getMetaByPath(const std::string& path,
                               const std::string& name,
                               MetaInfo& meta) = 0;

    virtual bool removeByPath(const std::string& path,
                              const std::string& name) = 0;
    
    virtual bool moveByPath(const std::string& oldPath,
                            const std::string& oldName,
                            const std::string& newPath,
                            const std::string& newName) = 0;
};

}
}























