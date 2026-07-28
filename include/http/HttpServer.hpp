#pragma once

#include "network/TcpConnection.hpp"
#include "network/TcpServer.hpp"
#include "network/EventLoop.hpp"
#include "network/Buffer.hpp"
#include "http/HttpRequest.hpp"
#include "http/HttpResponse.hpp"
#include "http/HttpContext.hpp"
#include "http/DeferredResponse.hpp"
#include "utils/ThreadPool.hpp"
#include <algorithm>
#include <asm-generic/errno-base.h>
#include <asm-generic/errno.h>
#include <cctype>
#include <cerrno>
#include <cstddef>
#include <map>
#include <memory>
#include <functional>
#include <sys/sendfile.h>
#include <fcntl.h>
#include <sys/types.h>
#include <unistd.h>


namespace miniKV {
namespace http {

class HttpServer {
public:
    using HttpCallback = std::function<void(const HttpRequest&, 
                                            HttpResponse*, 
                                            const network::TcpConnectionPtr&,
                                            const DeferredResponse::Ptr&)>;
    using StreamCheck = std::function<bool(const HttpRequest&)>;

    using BodyStreamSetup = std::function<void(HttpContext*, const HttpRequest&, 
                                          const network::TcpConnectionPtr&)>;

    HttpServer(network::EventLoop* loop, utils::ThreadPool* pool, int port)
        : loop_(loop),
          threadPool_(pool),
          server_(loop, port),
          httpCallback_(nullptr)
    {
        server_.setConnectionCallback([this](const network::TcpConnectionPtr& conn){
            onConnection(conn);
        });
        server_.setMessageCallback([this](const network::TcpConnectionPtr& conn, network::Buffer* buf){
            onMessage(conn, buf);
        });
    }

    void setHttpCallback(HttpCallback cb) {httpCallback_ = std::move(cb);}
    void setStreamCheck(StreamCheck cb) { streamCheck_ = std::move(cb); };
    void setBodyStreamSetup(BodyStreamSetup cb) { bodyStreamSetup_ = std::move(cb); }
    void start() { server_.start(); }

    network::EventLoop*  loop() { return loop_;}
    utils::ThreadPool*   threadPool()  {return threadPool_; }

private:
    void onConnection(const network::TcpConnectionPtr& conn)
    {
        if(conn->connected()) //是否已连接
        {
            int fd = conn->fd();
            contexts_[fd] = std::make_shared<HttpContext>();
        }
        else {
            contexts_.erase(conn->fd());
        }
    }

    void onMessage(const network::TcpConnectionPtr& conn, network::Buffer* buf)
    {
        auto it = contexts_.find(conn->fd());
        if(it == contexts_.end()) return ;

        HttpContext* ctx = it->second.get();

        if(ctx->bodyPause())
        {
            ctx->resumeBody();
        }

        while(buf->readableBytes() > 0)
        {
            if(!ctx->parseRequest(buf)) //error或者没有完
            {
                if(ctx->bodyPause())
                {
                    conn->pauseRead();
                    return;
                }
                if(ctx->isError())
                {
                    sendError(conn, HttpResponse::k400BadRequest, "Bad Request");
                    contexts_.erase(conn->fd());
                    conn->shutdown();
                    return ;
                }
                if(ctx->headersReady())
                {
                    //路由表
                    const HttpRequest& req = ctx->request();

                    if(streamCheck_ && streamCheck_(req) && bodyStreamSetup_)
                    {
                        bodyStreamSetup_(ctx, req, conn);
                    }
                    else {
                        ctx->setExpectBody();
                    }
                    if (!ctx->gotAll()) continue;
                }
                else {
                    return;
                }
            }

            if(ctx->gotAll())
            {
                HttpRequest req;
                req.swap(ctx->request());
                ctx->reset();

                bool keepAlive = isKeepAlive(req);

                if(httpCallback_ && threadPool_)
                {
                    auto deferred = DeferredResponse::create(loop_, conn, !keepAlive);
                    threadPool_->enqueue([this, conn, req = std::move(req), ctxPtr = it->second, deferred]()mutable {
                        auto resp = std::make_shared<HttpResponse>();
                        resp->setCloseConnection(!isKeepAlive(req));

                        httpCallback_(req, resp.get(), conn, deferred);

                        loop_->queueInLoop([this, conn, resp, ctxPtr](){
                            std::cout << "[SEND] isSendFile=" << resp->isSendFile() 
                            << " path=" << resp->bodyFilePath() 
                            << " size=" << resp->bodyFileSize() << std::endl;
                            network::Buffer outBuf;
                            resp->appendToBuffer(&outBuf);
                            conn->send(std::string(outBuf.peek(), outBuf.readableBytes()));
                            
                            if(resp->isSendFile())
                            {
                                conn->startSendFile(resp->bodyFilePath(), 
                                                    resp->bodyFileSize());
                            }

                            if(resp->closeConnection())
                            {
                                conn->shutdown();
                            }
                        });
                    });
                }
                else if(httpCallback_) {
                    HttpResponse resp;
                    resp.setCloseConnection(!keepAlive);
                    auto deferred = DeferredResponse::create(loop_, conn, !keepAlive);
                    httpCallback_(req, &resp, conn, deferred);
                    if(deferred->deferred()) return;

                    network::Buffer outBuf;
                    resp.appendToBuffer(&outBuf);
                    conn->send(std::string(outBuf.peek(), outBuf.readableBytes()));
                    if(resp.closeConnection())
                    {
                        conn->shutdown();
                    }
                }
                if(!keepAlive) return;
            }
        }
    }


    void sendError(const network::TcpConnectionPtr& conn,
                   HttpResponse::HttpStatusCode code,
                   const std::string& message)
    {
        HttpResponse resp;
        resp.setStatusCode(code);
        resp.setBody(message);
        resp.setCloseConnection(true);//传输已经脏了

        network::Buffer buf;
        resp.appendToBuffer(&buf);
        conn->send(std::string(buf.peek(), buf.readableBytes()));
    }

    static bool isKeepAlive(const HttpRequest& req) {
        std::string conn = req.getHeader("Connection");
        for(auto& c : conn) c = std::tolower(c);
        if(conn == "keep-alive") return true;
        if(conn == "close") return false;

        return req.version() == "HTTP/1.1";
    }

private:
    network::EventLoop* loop_;
    utils::ThreadPool* threadPool_;
    network::TcpServer server_;

    HttpCallback    httpCallback_;
    BodyStreamSetup bodyStreamSetup_;
    StreamCheck     streamCheck_;

    std::map<int, std::shared_ptr<HttpContext>> contexts_;
};

}


}


