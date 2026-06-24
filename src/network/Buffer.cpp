#include "network/Buffer.hpp"
#include <cerrno>
#include <cstddef>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>


namespace miniKV {

namespace network {

ssize_t Buffer::readFD(int fd, int *savedErrno)
{
    char extraBuf[65536];
    struct iovec vec[2];
    const size_t writable = writableBytes();

    vec[0].iov_base = &buffer_[writeIndex_];
    vec[0].iov_len  = writable;

    vec[1].iov_base = extraBuf;
    vec[1].iov_len  = sizeof(extraBuf);

    const int iovcnt = (writable <= sizeof(extraBuf)) ? 2 : 1;
    const ssize_t n = readv(fd, vec, iovcnt);

    if(n < 0) //error
    {
        *savedErrno = errno;
    }
    else if(static_cast<int>(n) <= writable)
    {
        writeIndex_ += n;
    }
    else {
        writeIndex_ = buffer_.size();
        append(extraBuf,  n - writable);
    }

    return n;
}

}
}





















