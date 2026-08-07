#pragma once

#include "network/TcpConnection.hpp"
#include "network/TcpServer.hpp"
#include "network/EventLoop.hpp"
#include "network/Buffer.hpp"
#include "http/HttpRequest.hpp"
#include "http/HttpResponse.hpp"
#include "http/HttpContext.hpp"
#include "http/DeferredResponse.hpp"
#include "utils/AsyncLogger.hpp"
#include "utils/ThreadPool.hpp"
#include <algorithm>
#include <any>
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
    // Applies protocol-specific headers to parser-level errors before the
    // connection is closed. DataNode uses this to keep CORS visible on a
    // rejected streaming request.
    using ErrorResponseDecorator = std::function<void(const HttpRequest&, HttpResponse*)>;

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

    ~HttpServer() { server_.stop(); }

    void setHttpCallback(HttpCallback cb) {httpCallback_ = std::move(cb);}
    void setStreamCheck(StreamCheck cb) { streamCheck_ = std::move(cb); };
    void setBodyStreamSetup(BodyStreamSetup cb) { bodyStreamSetup_ = std::move(cb); }
    void setErrorResponseDecorator(ErrorResponseDecorator cb) { errorResponseDecorator_ = std::move(cb); }
    void setThreadNum(size_t count) { server_.setThreadNum(count); }
    void start() { server_.start(); }

    network::EventLoop*  loop() { return loop_;}
    utils::ThreadPool*   threadPool()  {return threadPool_; }

private:
    void onConnection(const network::TcpConnectionPtr& conn)
    {
        if(conn->connected()) //是否已连接
        {
            conn->setContext(std::make_shared<HttpContext>());
        }
    }

    void onMessage(const network::TcpConnectionPtr& conn, network::Buffer* buf)
    {
        const auto* holder =
            std::any_cast<std::shared_ptr<HttpContext>>(&conn->context());
        if(holder == nullptr || !*holder) return;
        auto context = *holder;
        HttpContext* ctx = context.get();

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
                    miniKV::utils::logWarn("event=http_stream_body_aborted method=" +
                                            ctx->request().methodString() + " path=" +
                                            ctx->request().path());
                    sendError(conn, HttpResponse::k400BadRequest, "Bad Request", ctx->request());
                    conn->clearContext();
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
                    auto deferred = DeferredResponse::create(conn, !keepAlive);
                    threadPool_->enqueue([this, conn, req = std::move(req), context, deferred]()mutable {
                        auto resp = std::make_shared<HttpResponse>();
                        resp->setCloseConnection(!isKeepAlive(req));

                        httpCallback_(req, resp.get(), conn, deferred);

                        conn->ownerLoop()->queueInLoop([conn, resp, context](){
                            miniKV::utils::logDebug(
                                "event=http_response_send file_body=" +
                                std::to_string(resp->isSendFile()) + " file_size=" +
                                std::to_string(resp->bodyFileSize()));
                            network::Buffer outBuf;
                            resp->appendToBuffer(&outBuf);
                            conn->send(std::string(outBuf.peek(), outBuf.readableBytes()));
                            
                            if(resp->isSendFile())
                            {
                                conn->startSendFile(resp->bodyFilePath(), 
                                                    resp->bodyFileOffset(),
                                                    resp->bodyFileSize(),
                                                    resp->fileCompleteCallback());
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
                    auto deferred = DeferredResponse::create(conn, !keepAlive);
                    httpCallback_(req, &resp, conn, deferred);
                    if(deferred->deferred()) return;

                    network::Buffer outBuf;
                    resp.appendToBuffer(&outBuf);
                    conn->send(std::string(outBuf.peek(), outBuf.readableBytes()));
                    if(resp.isSendFile())
                    {
                        conn->startSendFile(resp.bodyFilePath(), resp.bodyFileOffset(),
                                            resp.bodyFileSize(),
                                            resp.fileCompleteCallback());
                    }
                    if(resp.closeConnection())
                    {
                        conn->shutdown();
                        return;
                    }
                }
                if(!keepAlive) return;
            }
        }
    }


    void sendError(const network::TcpConnectionPtr& conn,
                   HttpResponse::HttpStatusCode code,
                   const std::string& message,
                   const HttpRequest& request)
    {
        HttpResponse resp;
        resp.setStatusCode(code);
        resp.setBody(message);
        resp.setCloseConnection(true);//传输已经脏了
        if(errorResponseDecorator_) errorResponseDecorator_(request, &resp);

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
    ErrorResponseDecorator errorResponseDecorator_;

};

}


}
