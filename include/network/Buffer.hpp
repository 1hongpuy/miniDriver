#pragma once

#include <cstddef>
#include <sys/types.h>
#include <vector>
#include <string>
#include <algorithm>
#include <assert.h>


namespace miniKV {
namespace network {

class Buffer{
public:
    static const size_t kInitialSize = 1024;

    explicit Buffer(size_t initialSize = kInitialSize) : buffer_(initialSize), readerIndex_(0), writeIndex_(0){}

    size_t readableBytes() const {return writeIndex_ - readerIndex_; }

    size_t writableBytes() const {return buffer_.size() - writeIndex_; }

    const char* peek() const {return &buffer_[readerIndex_];}

    void retrieve(size_t len) //读取数据移动读指针
    {
        assert(len <= readableBytes());
        if(len < readableBytes())
        {
            readerIndex_ += len;
        }
        else {
            readerIndex_ = 0;
            writeIndex_ = 0;
        }
    }
    std::string retrieveAsString(size_t len)
    {
        std::string result(peek(), len);
        retrieve(len);
        return result;
    }

    void append(const char* data, size_t len)
    {
        ensureWritableBytes(len);
        std::copy(data, data+len, &buffer_[writeIndex_]);
        writeIndex_ += len;
    }
    ssize_t readFD(int fd, int* savedErrno);
private:
    //扩容
    void ensureWritableBytes(size_t len)
    {
        if(writableBytes() < len)
        {
            makeSpace(len);
        }
    }

    void makeSpace(size_t len)
    {
        if(writableBytes() + readerIndex_ < len)
        {
            buffer_.resize(writeIndex_ + len);
        }
        else {
            size_t readable = readableBytes();
            std::copy(&buffer_[readerIndex_], &buffer_[writeIndex_], &buffer_[0]);
            readerIndex_ = 0;
            writeIndex_ = readable;
        }
    }

    std::vector<char> buffer_;
    size_t readerIndex_;
    size_t writeIndex_;
};

}



}




















