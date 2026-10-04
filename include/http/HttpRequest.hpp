#pragma once


#include <cstddef>
#include <memory>
#include <string>
#include <map>
#include <algorithm>
#include <cctype>
#include <cstring>
#include <strings.h>
#include <tuple>


namespace miniKV {
namespace http {

class HttpRequest{
public:
    enum Method {
        kInvalid, //解析失效
        kGet,     //获取资源
        kPost,    //提交数据
        kHead,    //只获取头部，不返回boby
        kPut,     //替换资源
        kDelete,  //删除资源
        kOptions  //CORS 预检
    };

    HttpRequest() : method_(kInvalid) {}

    bool setMethod(const char* start, const char* end){
        size_t len = end - start;
        if(len == 3 && ::strncasecmp(start, "GET", 3) == 0){
            method_ = kGet;
        }
        else if(len == 4 && ::strncasecmp(start, "POST", 4) == 0)
        {
            method_ = kPost;
        }
        else if(len == 4 && ::strncasecmp(start, "HEAD", 4) == 0)
        {
            method_ = kHead;
        }
        else if(len == 3 && ::strncasecmp(start, "PUT", 3) == 0)
        {
            method_ = kPut;
        }
        else if(len == 6 && ::strncasecmp(start, "DELETE", 6) == 0)
        {
            method_ = kDelete;
        }
        else if(len == 7 && ::strncasecmp(start, "OPTIONS", 7) == 0)
        {
            method_ = kOptions;
        }
        else{
            method_ = kInvalid;
            return false;
        }
        return true;
    }

    void setPath(const char* start, const char* end)
    {   
        path_.assign(start, end);
    }
    void setQuery(const char* start, const char* end)
    {   
        query_.assign(start, end);
    }
    void setVersion(const char* start, const char* end)
    {   
        version_.assign(start, end);
    }

    void addHeader(const char* start, const char* colon, const char* end)
    {
        std::string field(start, colon);

        const char* valueStart = colon + 1;

        while(valueStart < end && (*valueStart == ' ' || *valueStart == '\t'))
        {
            ++valueStart;
        }
        std::string value(valueStart, end);
        headers_[field] = value;
    }

    void setBody(const std::string& body){
        body_ = body;
        bodyRef_ = nullptr;
        bodyLen_ = 0;
    }

    void setBodyRef(const char* data, size_t len) {
        bodyRef_ = data;
        bodyLen_ = len;
        body_.clear();
    }

    bool hasBodyRef() const { return bodyRef_ != nullptr; }
    const char* bodyPtr() const { return bodyRef_; }
    size_t bodyLen() const {return hasBodyRef() ? bodyLen_ : body_.size(); }

    //获取数据的首地址
    
    const char* bodyData() const {
        return hasBodyRef() ? bodyRef_ : body_.data() ;
    }
    void setUserData(std::shared_ptr<void> d) { userData_ = std::move(d); }
    std::shared_ptr<void> userData() const {return userData_;}
    void swap(HttpRequest& other)
    {
        //设计目的就是高效的转换一个httpRequest这样可以不用一直创建新的
        std::swap(method_, other.method_);
        query_.swap(other.query_);
        path_.swap(other.path_);
        version_.swap(other.version_);
        headers_.swap(other.headers_);
        body_.swap(other.body_);
        std::swap(bodyRef_, other.bodyRef_);
        std::swap(bodyLen_, other.bodyLen_);
        std::swap(userData_, other.userData_);
    }

    Method method() const {return method_;}
    const std::string& path() const {return path_;}
    const std::string& query() const {return query_;}
    const std::string& version() const {return version_;}
    const std::map<std::string, std::string>& headers() {
        return headers_;
    }

    //如果对象是在函数内部新出生的（临时构造的），绝对、绝对、绝对不能返回它的引用（&）或指针（*）。只能按值返回（Return by Value）。”
    std::string body() const {
        if(hasBodyRef()) return std::string(bodyRef_, bodyLen_);
        return body_;
    }

    std::string methodString() const {
        switch(method_)
        {
            case kGet:  return "GET";
            case kPost: return "POST";
            case kHead: return "HEAD";
            case kPut:  return "PUT";    
            case kDelete: return "DELETE";
            case kOptions: return "OPTIONS";
            default:      return "UNKNOWN";
        }
    }

    std::string getHeader(const std::string& field) const{
        const std::string wanted = normalizedHeaderName(field);
        for(const auto& [name, value] : headers_) {
            if(normalizedHeaderName(name) == wanted) return value;
        }
        return "";
    }

    bool hashHeader(const std::string& field) const {
        const std::string wanted = normalizedHeaderName(field);
        for(const auto& [name, value] : headers_) {
            if(normalizedHeaderName(name) == wanted) return true;
        }
        return false;
    }

    size_t contentLength() const {
        std::string val = getHeader("Content-Length");

        if(val.empty()) return 0;

        try {
            return static_cast<size_t>(std::stoull(val));
        }catch(...){
            return 0;
        }
    }

    std::string contentType() const{
        return getHeader("Content-Type");
    }

private:
    static std::string normalizedHeaderName(const std::string& name) {
        std::string normalized;
        normalized.reserve(name.size());
        for(const unsigned char c : name) {
            normalized.push_back(static_cast<char>(std::tolower(c)));
        }
        return normalized;
    }

    Method method_;
    std::string path_;
    std::string query_;
    std::string version_;
    std::map<std::string, std::string> headers_;
    //大文件加入零拷贝
    std::string body_;
    const char* bodyRef_ = nullptr;
    size_t      bodyLen_ = 0;
    std::shared_ptr<void> userData_;
};

}
}























