#pragma once


#include "storage/IDateStorage.hpp"
#include "storage/IMetaStorage.hpp"
#include <cstddef>
#include <ios>
#include <string>
#include <fstream>
#include <sys/stat.h> 
#include <cerrno>
#include <cstring>
#include <unistd.h>

/*
stat 提供 struct stat 结构和 stat() 函数，
用于获取文件的元信息：大小、权限、修改时间、是否为目录等
还有目录一般规范为/结尾，这样在大量并发读写的时候，都不会影响，判断这个/存不存在的问题
*/

namespace miniKV {
namespace storage {

class FileDataStorage : public IDateStorage {
public:
    explicit FileDataStorage(const std::string& rootPath) : rootPath_(rootPath)
    {
        if(!rootPath_.empty() && rootPath_.back() != '/')
        {
            rootPath_ += '/';
        }
    }

    bool init() override {
        //覆盖前面的虚函数
        return ensureDir(rootPath_);
    }

    bool writeData(const std::string& hash, const char* data, size_t len, size_t offset = 0) override
    {
        if(offset != 0) return false;

        std::string path = filePath(hash);

        struct stat st;
        if(stat(path.c_str(), &st) == 0 && (size_t)st.st_size == len)
        {
            return true;
        }


        std::string dir = path.substr(0, path.find_last_of('/'));
        if(!ensureDir(dir))
        {
            return false;
        }

        std::ofstream file(path, std::ios::binary | std::ios::trunc);
        if(!file.is_open()) return false;

        file.write(data, len);
        file.close();

        return file.good();
    }

    bool readData(const std::string& hash, std::string& out_data, size_t len, size_t offset = 0) override
    {
        std::string path = filePath(hash);

        std::ifstream file(path, std::ios::binary);
        if(!file.is_open()) return false;

        file.seekg(0, std::ios::end);
        size_t fileSize = file.tellg();

        if(offset > fileSize)
        {
            out_data.clear();
            return true;
        }

        if(offset + len > fileSize)
        {
            len = fileSize - offset;
        }

        file.seekg(offset);
        out_data.resize(len);
        file.read(out_data.data(), len); //这里频繁的用户态和内核态切换
        file.close();

        return  file.good() || file.eof();
    }


    std::string filePath(const std::string& hash) const override
    {
        std::string dir = rootPath_ + hash.substr(0, 2) + '/';
        return dir + hash;
        //到达最后了只是文件了，所以这个时候不需要这个/了
    }

    bool removeByHash(const std::string& hash) const override
    {
        std::string diskPath = filePath(hash);
        if(::access(diskPath.c_str(), F_OK) != 0) return true;
        return ::unlink(diskPath.c_str()) == 0;
    }

private:
    bool ensureDir(const std::string& path) const {
        std::string dir = path;

        if(!dir.empty() && dir.back() == '/')
        {
            dir.pop_back();
        }

        if(dir.empty()) return true;

        struct stat st;

        //c_str都是把string改变成char给c语言函数兼容
        if(stat(dir.c_str(), &st) == 0)
        {
            //文件已经创建完成
            return S_ISDIR(st.st_mode);
        }

        size_t pos = dir.find_last_of('/');
        if(pos != std::string::npos)
        {
            if(!ensureDir(dir.substr(0, pos))) return false;
        }
        //-1 if an error occurred
        if(mkdir(dir.c_str(), 0755) != 0)
        {
            if(errno != EEXIST) return false;
        }
        return true;
    }

    std::string rootPath_;
};
    
}
}

















