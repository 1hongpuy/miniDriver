#pragma once


#include <cstddef>
#include <functional>
#include <string>
#include <map>
#include <utility>
#include <vector>
#include <sys/types.h>
#include "sstream"
#include "network/Buffer.hpp"




namespace miniKV {
namespace network {
struct SendFileResult;
struct SendFileSegment;
}
namespace http {
class HttpResponse{
public:
    using FileCompleteCallback =
        std::function<void(const network::SendFileResult&)>;

    enum HttpStatusCode{
        kUnknown,
        k200Ok = 200, //成功
        k201Created = 201,
        k206PartialContent = 206, //返回部分内容
        k302Found = 302,       //分享链接跳转到实际文件
        k304NotModified = 304, //资源未修改，使用缓存
        k400BadRequest = 400,  //请求有误
        k403Forbidden = 403,   //禁止访问
        k404NotFound = 404,    //资源不存在
        k405MethodNotAllowed = 405, //方法不被允许，有这个资源，不是不允许这么方法操作
        k413PayloadTooLarge = 413, //请求体过大
        k416RangeNotSatisfiable = 416,
        k409Conflict = 409,
        k500InternalServerError = 500, //网络内部错误
        k501NotImplemented = 501, //功能为实现
        k503ServiceUnavailable = 503, //暂时无可用容量，可稍后重试
    };
    HttpResponse() : statusCode_(kUnknown),closeConnection_(true) {}

    void setStatusCode(HttpStatusCode code) {statusCode_ = code;}
    void setCloseConnection(bool on) {closeConnection_ = on;}
    bool closeConnection() const {return closeConnection_;}

    void setContentType(const std::string& type)
    {
        addHeader("Content-Type", type);
    }
    void setBody(const std::string& body)
    {
        body_ = body;
        addHeader("Content-Length", std::to_string(body_.size()));
    }

    void setFileBody(const std::string& filePath, size_t fileSize)
    {
        setFileBody(filePath, 0, fileSize, {});
    }

    void setFileBody(const std::string& filePath, off_t fileOffset, size_t fileSize)
    {
        setFileBody(filePath, fileOffset, fileSize, {});
    }

    void setFileBody(const std::string& filePath, off_t fileOffset, size_t fileSize,
                     FileCompleteCallback callback)
    {
        bodyFilePath_ = filePath;
        bodyFileOffset_ = fileOffset;
        bodyFileSize_ = fileSize;
        fileCompleteCallback_ = std::move(callback);
        addHeader("Content-Length", std::to_string(fileSize));
    }

    struct FileSegment {
        std::string filePath;
        off_t fileOffset = 0;
        size_t fileSize = 0;
    };

    void setFileBodies(std::vector<FileSegment> segments, size_t totalSize,
                       FileCompleteCallback callback = {})
    {
        bodyFileSegments_ = std::move(segments);
        bodyFilePath_.clear();
        bodyFileOffset_ = 0;
        bodyFileSize_ = totalSize;
        fileCompleteCallback_ = std::move(callback);
        addHeader("Content-Length", std::to_string(totalSize));
    }

    bool isSendFile() const { return !bodyFilePath_.empty() || !bodyFileSegments_.empty(); }
    bool hasFileBodySequence() const { return !bodyFileSegments_.empty(); }
    const std::string& bodyFilePath() const { return bodyFilePath_; }
    off_t bodyFileOffset() const { return bodyFileOffset_; }
    size_t bodyFileSize() const { return bodyFileSize_; }
    const FileCompleteCallback& fileCompleteCallback() const {
        return fileCompleteCallback_;
    }
    const std::vector<FileSegment>& bodyFileSegments() const { return bodyFileSegments_; }

    void addHeader(const std::string& key,  const std::string& value)
    {
        headers_[key] = value;
    }

    /*
    HTTP/1.1 200 OK\r\n              ← 状态行
    Content-Type: image/jpeg\r\n     ← 头部字段（setContentType 设置的）
    Content-Length: 20480\r\n        ← 头部字段（setBody 自动设置的）
    \r\n                              ← 空行（头部和body的分隔）
    [20KB 的 JPEG 二进制数据]         ← 消息体（setBody 存储的）
    */
    void appendToBuffer(network::Buffer* output) const{
        
        std::string statusLine = "HTTP/1.1 ";
        statusLine += std::to_string(statusCode_);
        statusLine += " ";
        statusLine += statusMessage();
        statusLine += "\r\n";
        output->append(statusLine.data(), statusLine.size());

        for(const auto& kv : headers_)
        {
            std::string header = kv.first + ": " + kv.second + "\r\n";
            output->append(header.data(), header.size());
        }

        output->append("\r\n", 2);

        if(!body_.empty())
        {
            output->append(body_.data(), body_.size());
        }
    }

private:
    std::string statusMessage() const{
        switch(statusCode_){
            case 200: return "OK";
            case 201: return "Created";
            case 206: return "Partial Content";
            case 302: return "Found";
            case 304: return "Not MOdified";
            case 400: return "Bad Requeset";
            case 403: return "Forbidden";
            case 404: return "Not Found";
            case 405: return "Method Not Allowed";
            case 413: return "Payload Too Large";
            case 416: return "Range Not Satisfiable";
            case 409: return "Conflict";
            case 500: return "Internal Server Error";
            case 501: return "Not Implemented";
            case 503: return "Service Unavailable";
            default: return "Unknown";
        }
    }

    HttpStatusCode statusCode_;
    std::map<std::string, std::string> headers_;
    std::string body_;
    bool closeConnection_;
    //1 -- 短连接， 0 --长连接

    //零拷贝发送变量
    std::string bodyFilePath_;
    off_t       bodyFileOffset_ = 0;
    size_t      bodyFileSize_ = 0;
    std::vector<FileSegment> bodyFileSegments_;
    FileCompleteCallback fileCompleteCallback_;
};
    

}
    
}
