#pragma once

#include "http/HttpRequest.hpp"
#include "network/Buffer.hpp"
#include "utils/AsyncLogger.hpp"
#include <algorithm>
#include <cstddef>
#include <cstring>
#include <functional>
#include <memory>


/*
GET / HTTP/1.1\r\n
Host: 127.0.0.1:8888\r\n
Connection: keep-alive\r\n
Accept: text/html,application/xhtml+xml\r\n
User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64)\r\n
\r\n
*/

namespace miniKV {
namespace http {

class HttpContext{
public:
    enum State{
        kExpectRequestLine,
        kExpectHeaders,
        kHeadersComplete,     // Header解析完
        kExpectBody,          //小 body
        kStreamingBody,       //大 body
        kGotAll,
        kError
    };

    enum class BodyConsumeResult {
        kContinue,
        kPause,
        kAbort
    };
    using BodyDataCallback = std::function<BodyConsumeResult(const char* data, size_t len)>;
    HttpContext() : state_(kExpectRequestLine), bodyTotal(0), bodyReceived_(0) {}
    void setUserData(std::shared_ptr<void> d) { request_.setUserData(d); }

    std::shared_ptr<void> userData() { return request_.userData();}

    void setExpectBody() {
        state_ = (request_.contentLength() > 0) ? kExpectBody : kGotAll;
    }
    //核心方法
    bool parseRequest(network::Buffer* buf)
    { 
        //要 HttpServer 介入的用 return true，自己内部处理的用 hasMore = ...; break。
        bool hasMore = true;
        while(hasMore){
            switch(state_) {
                case kExpectRequestLine:
                    hasMore = parseRequestLine(buf);
                    break;
                case kExpectHeaders:
                    hasMore = parseHeaders(buf);
                    break;
                case kHeadersComplete:
                    return false;
                case kExpectBody:
                    hasMore = parseBoby(buf);
                    break;
                case kStreamingBody:
                    hasMore = streamBody(buf);
                    break;
                case kGotAll:
                    return true;
                case kError:
                    return false;
            }
        }
        return state_ == kGotAll;
    }

    bool gotAll()  const { return state_ == kGotAll; }
    bool isError() const { return state_ == kError; }
    bool headersReady() const { return state_ == kHeadersComplete; }
    bool bodyPause() const { return bodyPaused_; }
    State state()  const { return state_; }
    HttpRequest& request() { return request_; }

    void reset() {
        state_ = kExpectRequestLine;
        HttpRequest empty;
        request_.swap(empty);
        bodyCallback_ = nullptr;
        bodyReceived_ = 0;
        bodyTotal     = 0;
        bodyPaused_ = false;
    }

   

    void setBodyCallback(size_t totalLen, BodyDataCallback cb) {
        bodyTotal     = request_.contentLength();
        bodyReceived_ = 0;
        bodyCallback_ = cb;
        bodyPaused_   = false;
        state_        = kStreamingBody;
    }

    void setMaxStreamBodyBytes(size_t bytes) {
        if(bytes > 0) maxStreamBodyBytes_ = bytes;
    }

    void resumeBody()
    {
        if(state_ == kStreamingBody)
        {
            bodyPaused_ = false;
        }
    }

    void setGotAll() {
        state_ = kGotAll;
    }
private:

    //解析请求行
    bool parseRequestLine(network::Buffer* buf)
    {
        if(buf->readableBytes() < 4)
        {
            return false;
        }

        const char* crlf = findCRLF(buf->peek(), buf->readableBytes());
        if(!crlf)
        {
            return false;//
        }

        const char* start = buf->peek();
        const char* end   = crlf;

        const char* spacel = std::find(start, end, ' ');
        if(spacel == end) //
        {
            state_ = kError;
            return false;
        }

        if(!request_.setMethod(start, spacel))
        {
            state_ = kError;
            return false;
        }

        const char* space2 = std::find(spacel+1, end, ' ');
        if(space2 == end) //
        {
            state_ = kError;
            return false;
        }

        const char* queryStart = std::find(spacel+1, space2, '?');
        if(queryStart != space2) //找到？
        {
            request_.setPath(spacel+1, queryStart);
            request_.setQuery(queryStart+1, space2);
        } else {
            request_.setPath(spacel+1, space2);
        }

        request_.setVersion(space2+1, end);

        buf->retrieve((crlf - start) + 2);
        
        state_ = kExpectHeaders;
        miniKV::utils::logDebug("event=http_request_parsed path=" + request_.path() +
                                " query=" + request_.query());
        return true;
    }

    //解析头部
    //格式：Field-Name：Value\r\n
    //对于状态机来说，每次都调用一行来读取
    bool parseHeaders(network::Buffer* buf)
    {
        const char* start = buf->peek();

        if(buf->readableBytes() >= 2 && start[0] == '\r' && start[1] == '\n')
        {
            buf->retrieve(2);

            state_ = kHeadersComplete;
            return true;
            // size_t contentLen = request_.contentLength();
            // if(contentLen > 0) 
            // {
            //     state_ = kExpectBody;
            //     return true;
            // }
            // else { //？？
            //     state_ = kGotAll;
            //     return false;  // 解析完成，不需要继续循环
            // }
        }

        const char* crlf = findCRLF(start, buf->readableBytes());
        if(!crlf)
        {
            return false; //还没接收到这个\r\n
        }

        const char* colon = std::find(start, crlf, ':');
        if(colon == crlf)
        {
            state_ = kError;
            return false;
        }

        request_.addHeader(start, colon,crlf); //warming
        buf->retrieve((crlf - start) + 2);
        return true;
    }

    //解析Body
    bool parseBoby(network::Buffer* buf) {
        size_t contentLen = request_.contentLength();
        if(buf->readableBytes() < contentLen)
        {
            return false;
        }

        request_.setBody(std::string(buf->peek(), contentLen));
        buf->retrieve(contentLen);
        state_ = kGotAll;
        return false;
    }

    bool streamBody(network::Buffer* buf) {
        if(bodyPaused_) //暂停
        {
            return false;
        }

        size_t availl = buf->readableBytes();
        if(availl == 0) return false;
        size_t toFeed = std::min({availl, bodyTotal - bodyReceived_, maxStreamBodyBytes_});
        if(!bodyCallback_)
        {
            state_ = kError;
            return false;
        }

        const BodyConsumeResult result = bodyCallback_(buf->peek(), toFeed);
        if(result == BodyConsumeResult::kAbort)
        {
            state_ = kError;
            return false;
        }

        buf->retrieve(toFeed);
        bodyReceived_ += toFeed;

        if(bodyReceived_ >= bodyTotal)
        {
            state_ = kGotAll;
            return true;
        }

        if(result == BodyConsumeResult::kPause)
        {
            bodyPaused_ = true;
            return false;
        }
        // The current Buffer may already contain further body bytes from the
        // same read(2). Keep parsing until it is empty, the stream pauses, or
        // the declared Content-Length has been consumed.
        return true;
    }

private:
    //工具
    //在Start和end之间查找\r\n
    static const char* findCRLF(const char* start, size_t len)
    {
        if(len < 2) return nullptr;
        const char* end = start + len - 1;
        for(const char* p = start; p < end; ++p)
        {
            if(p[0] == '\r' && p[1] == '\n')
            {
                return p;
            }
        }
        return nullptr;
    }

private:
    BodyDataCallback bodyCallback_;
    size_t bodyReceived_ = 0;
    size_t bodyTotal     = 0;
    bool   bodyPaused_   = false;
    size_t maxStreamBodyBytes_ = 64 * 1024;

    State state_;
    HttpRequest request_;
};

} 
}

























