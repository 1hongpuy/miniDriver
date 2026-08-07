#include "network/Acceptor.hpp"
#include "network/EventLoop.hpp"
#include <asm-generic/socket.h>
#include <cstdlib>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h> //UNIX 系统标准调用
#include <fcntl.h>
#include <iostream>
#include <cstring>
#include <cerrno>


namespace miniKV {
namespace network {

static int createNonblocking() {
    int sockFd = socket(AF_INET, SOCK_NONBLOCK | SOCK_STREAM | SOCK_CLOEXEC, 0);
    //ipv4，可靠连接，非阻塞
    if(sockFd < 0)
    {
        //error
        std::cerr << "Acceptor::createNonblocking failed!" << std::endl;
        abort(); //结束进程
    } 
    return sockFd;
}    

Acceptor::Acceptor(EventLoop* loop, int port)
    :loop_(loop), 
     acceptFd_(createNonblocking()),
     acceptChannel_(loop, acceptFd_),
     listening_(false)
{
    int optval = 1;//端口复用，每次断开连接后，该tcp端口会在2TIME_WAIT等待后释放，所以无法快速重启，设置为端口复用就可以了
    ::setsockopt(acceptFd_, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(optval));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons(port);

    if(::bind(acceptFd_, (struct sockaddr*)&addr, sizeof(addr)) < 0){
        std::cerr << "Acceptor bind failed" << std::endl;
        abort();
    }
    
    acceptChannel_.setReadCallback([this](){handleRead();});
}

Acceptor::~Acceptor()
{
    acceptChannel_.disableAll();
    acceptChannel_.remove();
    ::close(acceptFd_);
}

void Acceptor::listen(){
    listening_ = true;

    if(::listen(acceptFd_, SOMAXCONN) < 0 ){
        //listen失败
        std::cerr << "Acceptor listen failed!" << std::endl;
        abort();
    }
    acceptChannel_.enableReading();
}

void Acceptor::stop()
{
    if(!listening_) return;
    listening_ = false;
    acceptChannel_.disableAll();
}

void Acceptor::handleRead(){
    for(;;) {
        struct sockaddr_in clientAddr {};
        socklen_t addrLen = sizeof(clientAddr);
        const int connfd = ::accept4(acceptFd_,
            reinterpret_cast<struct sockaddr*>(&clientAddr), &addrLen,
            SOCK_NONBLOCK | SOCK_CLOEXEC);
        if(connfd >= 0) {
            int optval = 1;
            ::setsockopt(connfd, IPPROTO_TCP, TCP_NODELAY, &optval, sizeof(optval));
            int sndbuf = 256 * 1024;
            ::setsockopt(connfd, SOL_SOCKET, SO_SNDBUF, &sndbuf, sizeof(sndbuf));

            if(newConnectionCallback_) newConnectionCallback_(connfd, clientAddr);
            else ::close(connfd);
            continue;
        }
        if(errno == EINTR) continue;
        if(errno != EAGAIN && errno != EWOULDBLOCK) {
            std::cerr << "Acceptor::accept4 failed errno=" << errno << std::endl;
        }
        break;
    }
}

}


}












