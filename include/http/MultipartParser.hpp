#pragma once


#include <cctype>
#include <cstddef>
#include <functional>
#include <string>
#include <cstring>
#include <vector>
#include <cstdio>


/*
(这里是 Preamble - 废话区，会被 handlePreamble 跳过)
This is some random text before the real data...

------MyBoundary\r\n                               <-- 首次边界 (firstBoundary_)
Content-Disposition: form-data; name="file1"; filename="hello.txt"\r\n <-- Part Headers
Content-Type: text/plain\r\n                                           <-- Part Headers
\r\n                                               <-- 空行 (Header 结束标志)
Hello, this is the content of the first file!      <-- Part Body (数据段1)
\r\n------MyBoundary\r\n                           <-- 普通边界 (fullBoundary_)
Content-Disposition: form-data; name="file2"; filename="pixel.png"\r\n <-- Part Headers
Content-Type: image/png\r\n                                            <-- Part Headers
\r\n                                               <-- 空行 (Header 结束标志)
PNG\r\nIHDR...[这里是图片的二进制数据]...IEND      <-- Part Body (数据段2)
\r\n------MyBoundary--\r\n                         <-- 最终边界 (finalBoundary_)

(这里是 Epilogue - 结尾废话，解析器执行到 kDone 后不再理会)
Goodbye!
*/

namespace miniKV {
namespace http {

class MultipartParser {
public:
    using PartHeaderCallback = std::function<void(const std::string& filename,
                                                  const std::string& contentType)>;
    using DataCallback = std::function<void(const char* data, size_t len)>;    
    using PartEndCallback = std::function<void()>;
    
    MultipartParser() : state_(kExpectPreamble),                
                        lookBehindLen_(0),
                        partDataStarted_(false) //接收文件的开始
                        {}

        void setBoundary(const std::string& boundary) {
            fullBoundary_ = "\r\n--" + boundary;
            firstBoundary_ = "--" + boundary;
            finalBoundary_ = "\r\n--" + boundary + "--";
            lookBehindLen_ = 0;
            lookBehind_.resize(fullBoundary_.size() + 4);
            //reserve 只修改cap，不修改size， resize 全修改
        }

        void setPartHeaderCallback(PartHeaderCallback cb)   { partHeaderCb_ = std::move(cb); }
        void setDataCallback(DataCallback cb)               { dataCb_ = std::move(cb); }
        void setPartEndCallback(PartEndCallback cb)         { partEndCb_ = std::move(cb); }

        bool feed(const char* data, size_t len) {
            const char* p = data;
            const char* end = data + len;
            //[begin, end)
            while(p < end && state_ != kDone && state_ != kError)
            {
                /*
                    kExpectPreamble,      //跳过第一个boundary之前的内容
                    kExpectPartHeaders,   //解析文件部分的头部
                    kExpectPartBody,      //接收文件数据，同时扫描下一个boundary
                    kDone,                //收到final boundary
                    kError                //解析错误
                */
                switch (state_) {
                    case kExpectPreamble: 
                        p = handlePreamble(p, end);
                        break;
                    case kExpectPartHeaders: 
                        p = handlePartHeaders(p, end);
                        break;
                    case kExpectPartBody: 
                        p = handlePartBody(p, end);
                        break;
                    case kDone: 
                    case kError:
                    break;
                }
            }
            return state_ == kDone;
        }
    
    bool isDone() const { return state_ == kDone;}
    bool isError() const { return state_ == kError;}

    void finish()
    {
        if(state_ == kExpectPartBody && partDataStarted_ && lookBehindLen_ > 0)
        {
            flushLookBehind();
        }
    }

    void reset() { //复位
        state_ = kExpectPreamble; 
        lookBehindLen_ = 0;
        partDataStarted_ = false;
        partHeaders_.clear();
        partFilename_.clear();
        partContentType_.clear();
    }

private:
    enum State{
        kExpectPreamble,      //跳过第一个boundary之前的内容
        kExpectPartHeaders,   //解析文件部分的头部
        kExpectPartBody,      //接收文件数据，同时扫描下一个boundary
        kDone,                //收到final boundary
        kError                //解析错误
    };

    // ======= 处理 Preamble ==========
    // 找到第一个 “--boundary\r\n”
    const char* handlePreamble(const char* p, const char* end)
    {
        std::string search = buildSearchBuffer(p, end);//解决分包

        size_t pos = search.find(firstBoundary_);
        if(pos == std::string::npos)
        {
            size_t keep = std::min(search.size(), firstBoundary_.size() + 2);
            //就是这个firstBoundary加上\r\n
            saveLockBehind(search.data() + search.size() - keep,  keep);
            return end;
        }

        size_t consumed = pos + firstBoundary_.size();
        state_ = kExpectPartHeaders;

        size_t consumedFromData;
        if(pos >= lookBehindLen_)
        {
            consumedFromData = pos - lookBehindLen_ + firstBoundary_.size();
        }
        else { //跨界了，就是数据是在look中， boundary的长度剪掉开始的长度剪掉了这个pos
            consumedFromData = finalBoundary_.size() - (lookBehindLen_ - pos);
        }
        lookBehindLen_ = 0;

        size_t skip = consumedFromData;
        //防止乱码和这个发送的分包
        if(skip + 2 <= (size_t)(end - p) && p[skip] == '\r' && p[skip+1] == '\n')
        {
            skip += 2;
        }
        return p + skip;
    }

      // ========== 处理 Part Headers ==========
    // 格式：
    //   Content-Disposition: form-data; name="files"; filename="photo.jpg"\r\n
    //   Content-Type: image/jpeg\r\n
    //   \r\n
    const char* handlePartHeaders(const char* p, const char* end)
    {
        while(p < end)
        {
            //找到第一个\r\n
            const char * crlf = findCRLF(p, end);
            if(!crlf)
            {
                //没有找到
                partHeaders_.append(p, end - p);
                return end;
            }

            if(crlf == p)
            {
                //空行 在开头
                p += 2;

                parsePartHraders(); //查找头部

                if(partHeaderCb_)
                {
                    partHeaderCb_(partFilename_, partContentType_);
                    //处理头部信息
                }

                state_ = kExpectPartBody;//状态告知就是下一步干啥
                partDataStarted_ = false;
                lookBehindLen_ = 0;
                return p;
            }

            partHeaders_.append(p, crlf+2 - p);
            p = crlf + 2;
        }
        return end;
    }

    // ============ 处理 Part Body ============
    const char * handlePartBody(const char* p, const char* end)
    {
        std::string search = buildSearchBuffer(p, end);
        size_t searchLen   = search.size();

        size_t posNormal   = search.find(fullBoundary_);
        size_t posFinal    = search.find(finalBoundary_);
        size_t pos         = std::string::npos;

        bool isFinal = false;
        if(posFinal != std::string::npos) {
            pos = posFinal;
            isFinal = true;
        }

        if(posNormal != std::string::npos && ((posNormal < pos) || (pos == std::string::npos)))
        {
            //一次性收到两个边界，普通边界再前面
            pos = posNormal;
            isFinal = false;
        }

        size_t safeLen = (searchLen > fullBoundary_.size() + 2) 
                            ? searchLen - fullBoundary_.size() - 2
                            : 0;
                            //安全边界 2字节
        //前面只是寻找数据，但是并没有把边界写入
        if(pos == std::string::npos || pos > safeLen)
        {
            if(lookBehindLen_ > 0 && dataCb_)
            {
                dataCb_(lookBehind_.data(), lookBehindLen_);
                lookBehindLen_ = 0;
            }
            if(safeLen > 0)
            {
                if(partDataStarted_) partDataStarted_ = true;
                if(dataCb_) {
                    dataCb_(p, safeLen);
                }
                p += safeLen;
            }

            size_t remaining = end - p;
            saveLockBehind(p, remaining);
            return end;
        }
        
        //找到了boundary
        size_t dataInSearch = pos;
        size_t dataInData = 0;

        if(lookBehindLen_ > 0)
        {
            size_t fromLB = (dataInSearch < lookBehindLen_) ? dataInSearch : lookBehindLen_;
            if(fromLB > 0 && dataCb_)
            {
                dataCb_(lookBehind_.data(), fromLB);
            }
        }
        //在这个上次的数据之后
        if(dataInSearch > lookBehindLen_)
        {
            dataInData = dataInSearch - lookBehindLen_;//处理前面的数据
            if(dataCb_ && dataInData > 0)
            {
                dataCb_(p, dataInData);
            }
            p += dataInData;
        }

        size_t boundaryLen = isFinal ? finalBoundary_.size() : fullBoundary_.size();
        size_t boundaryInData = boundaryLen;
        if(dataInSearch < lookBehindLen_)
        {
            boundaryInData = boundaryInData - (lookBehindLen_ - dataInSearch);
        }
        p += boundaryInData;

        if(boundaryInData > (size_t)(end - p + dataInData))
        {
            //我们跳过段超过了这个数据段
            p = end;
        }
        lookBehindLen_ = 0;

        if(partEndCb_)
        {
            partEndCb_();
        }

        if(isFinal) {
            state_ = kDone;
        }
        else {
            if(p + 2 <= end && p[0] == '\r' && p[1] == '\n')
            {
                p += 2;
            }
            state_ = kExpectPartHeaders;
            partHeaders_.clear();
            partFilename_.clear();
            partContentType_.clear();
        }

        return p;
    }

    void parsePartHraders()
    {
        partFilename_.clear();
        partContentType_.clear();

        size_t fnPos = partHeaders_.find("filename=\"");
        if(fnPos != std::string::npos)
        {
            fnPos += 10;
            size_t fnEnd = partHeaders_.find('"', fnPos);
            //find 的一种重载方式，就是从这个fnpos开始查找
            if(fnEnd != std::string::npos)
            {
                partFilename_ = partHeaders_.substr(fnPos, fnEnd - fnPos);
                //std::string substr(size_type pos = 0, size_type count = npos) const;
            }
        }

        //不同客户段的这个Content-Type是不一样的，可能大小写之分
        std::string lower = partHeaders_;
        for(auto& c : lower) c = std::tolower(c);
        size_t ctPos = lower.find("content-type:");
        if(ctPos != std::string::npos)
        {
            ctPos += 13;
            while (ctPos < partHeaders_.size() && 
                    (partHeaders_[ctPos] == ' ' || partHeaders_[ctPos] == '\t')) {
                ++ctPos;
            }
            size_t ctEnd = partHeaders_.find('\r', ctPos);
            if(ctEnd == std::string::npos) ctEnd = partHeaders_.size();
            partContentType_ = partHeaders_.substr(ctPos, ctEnd - ctPos);
        }
    }

    //解决boundry分包的问题，合并数据
    std::string buildSearchBuffer(const char* p, const char* end){
        std::string search;

        if(lookBehindLen_ > 0 )
        {
            search.assign(lookBehind_.data(), lookBehindLen_);
        }
        search.append(p, end-p);
        return search;
    }

    //扣留一些元数
    void saveLockBehind(const char* data, size_t len)
    {
        if(len > lookBehind_.size()) len = lookBehind_.size();
        lookBehindLen_ = len;
        if(len > 0)
        {
            std::memcpy(lookBehind_.data(), data, len);
        } 
    }
    
    void flushLookBehind() {
        if(lookBehindLen_ > 0 && dataCb_)
        {
            dataCb_(lookBehind_.data(), lookBehindLen_);
        }
        lookBehindLen_ = 0;
    }


    //这里的static是编译器限制这个函数值编译在这个hpp中，不暴露给别的文件
    static const char* findCRLF(const char* start, const char* end)
    {
        for(const char* p = start; p + 1 < end; p++)
        {
            if(p[0] == '\r' && p[1] == '\n')
            {
                return p;
            }
        }
        return nullptr;
    }

    // =========== 状态 ==========
    State state_;
    std::string fullBoundary_;
    std::string firstBoundary_;
    std::string finalBoundary_;
    
    std::string partHeaders_;          //解析部分的头部
    std::string partFilename_;         //解析出的文件名
    std::string partContentType_;      //解析出的Content-Type

    std::vector<char> lookBehind_;
    size_t lookBehindLen_;
    bool partDataStarted_;

    
    
    PartHeaderCallback partHeaderCb_;
    DataCallback dataCb_;
    PartEndCallback partEndCb_;
};


}

}













