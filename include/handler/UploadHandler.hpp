#pragma once
#include "Handler.hpp"
#include "Common.hpp"
#include "StreamHash.hpp"
#include "http/HttpContext.hpp"
#include "http/MultipartParser.hpp"
#include "storage/FileDataStorage.hpp"
#include "storage/FileMetaStorage.hpp"
#include "utils/ThreadPool.hpp"
#include "network/EventLoop.hpp"
#include <cstdio>
#include <memory>
#include <openssl/evp.h>
#include <string>
#include <unistd.h>
#include <vector>
#include <ctime>



namespace miniKV {
namespace handler {

using namespace network;
using namespace http;

// ===== 流式上下文 =====
struct FileItem {
    std::string tmpFile;
    std::string fileName;
    std::string mimeType;
    FILE*       fp = nullptr;
    EVP_MD_CTX* hashCTX = nullptr;
    std::string finalHash;
};
struct StreamCtx {
    std::string dirPath;
    std::vector<FileItem> files;

    ~StreamCtx() {
        for(auto& f : files)
        {
             // A. 如果连接因为异常断开，有些文件没来得及关，在这里统一关闭 [S_D0]
             if (f.fp) {
                fclose(f.fp);
                f.fp = nullptr;
            }
            if(f.hashCTX)
            {
                EVP_MD_CTX_free(f.hashCTX);
                f.hashCTX = nullptr;
            }
            if(!f.tmpFile.empty() &&
                ::access(f.tmpFile.c_str(), F_OK) == 0)
            {
                //写了零时文件,F_OK 表示仅检查文件是否存在
                ::unlink(f.tmpFile.c_str());
                //主要功能是删除一个文件
            }
        }
    }
};


class UploadHandler : public Handler {
    public:
        explicit UploadHandler( storage::FileDataStorage* dataStorage,
                                storage::FileMetaStorage* metaStorage,
                                utils::ThreadPool* threadPool,
                                network::EventLoop* loop,
                                const char* tmpDir)
                                : dataStorage(dataStorage), metaStorage(metaStorage),
                                  threadPool(threadPool), loop(loop), tmpDir(tmpDir){}
        // === 路由匹配 ===
        bool match(const HttpRequest& req) override {
            return req.path() == "/api/files" && req.method() == HttpRequest::kPost;
        }
    
        // === 判断是否需要流式（给 HttpServer::setStreamCheck 用） ===
        static bool needsStreaming(const HttpRequest& req) {
            return req.method() == HttpRequest::kPost
                && req.path() == "/api/files"
                && req.contentLength() > 65536;
        }
    
        // === 流式注册（给 HttpServer::setBodyStreamSetup 用） ===
        static void setupStreaming(HttpContext* ctx, const HttpRequest& req,
                                    const TcpConnectionPtr&) {
            std::string ct = req.contentType();
            size_t bp = ct.find("boundary=");
            if (bp == std::string::npos) return;
            std::string boundary = ct.substr(bp + 9);
            if (!boundary.empty() && boundary.front() == '"')
                boundary = boundary.substr(1, boundary.size() - 2);
    
            auto sctx = std::make_shared<StreamCtx>();
            sctx->dirPath = normalizePath(
                urlDecode(getQueryParam(req.query(), "path")));
    
            auto parser = std::make_shared<MultipartParser>();
            parser->setBoundary(boundary);
    
            parser->setPartHeaderCallback(
                [sctx](const std::string& fname, const std::string& mime) {
                    ensureDir("../data/tmp/");
                    FileItem item;
                    item.fileName = fname;
                    item.mimeType = mime;
                    item.tmpFile  = "../data/tmp/" + fname + ".upload";
                    item.fp       = fopen(item.tmpFile.c_str(), "wb");
                    
                    item.hashCTX  = EVP_MD_CTX_new();
                    EVP_DigestInit_ex(item.hashCTX, EVP_sha256(), nullptr);
                    if (item.fp && item.hashCTX){
                        sctx->files.push_back(std::move(item));
                    } else {
                        if(item.fp) fclose(item.fp);
                        if(item.hashCTX) EVP_MD_CTX_free(item.hashCTX);
                    }
                });
    
            parser->setDataCallback(
                [sctx](const char* data, size_t len) {
                    if(sctx->files.empty()) return;
                    auto& f = sctx->files.back();

                    if(f.fp) fwrite(data, 1, len, f.fp);
                    if(f.hashCTX) EVP_DigestUpdate(f.hashCTX, reinterpret_cast<const unsigned char*>(data), len);
                    // if (!sctx->files.empty() && sctx->files.back().fp)
                    //     fwrite(data, 1, len, sctx->files.back().fp);
                });
    
            parser->setPartEndCallback([sctx]() {
                // if (!sctx->files.empty() && sctx->files.back().fp) {
                //     fclose(sctx->files.back().fp);
                //     sctx->files.back().fp = nullptr;
                // }
                if (sctx->files.empty()) return;
                auto& f = sctx->files.back();
        
                // ① 先关文件
                if (f.fp) {
                    fclose(f.fp);
                    f.fp = nullptr;
                }
        
                // ② 🌟 完成 hash 计算 → hex 字符串
                if (f.hashCTX) {
                    unsigned char hash[EVP_MAX_MD_SIZE];
                    unsigned int  hashLen = 0;
                    EVP_DigestFinal_ex(f.hashCTX, hash, &hashLen);
                    EVP_MD_CTX_free(f.hashCTX);
                    f.hashCTX = nullptr;
        
                    // 转 hex 字符串
                    std::ostringstream oss;
                    for (unsigned int i = 0; i < hashLen; ++i)
                        oss << std::hex << std::setw(2)
                            << std::setfill('0') << (int)hash[i];
                    f.finalHash = oss.str();   // 🌟 存起来，handle() 直接用
                }
            });
    
            ctx->setUserData(sctx);
            ctx->setBodyCallback(req.contentLength(),
                [parser](const char* data, size_t len) {
                    parser->feed(data, len);
                });
        }
    
        // === 请求处理（流式 + 非流式） ===
        void handle(const HttpRequest& req, HttpResponse* resp,
                    const TcpConnectionPtr&) override {
            std::string dirPath = normalizePath(
                urlDecode(getQueryParam(req.query(), "path")));
    
            // --- 流式路径 ---
            auto sctx = std::static_pointer_cast<StreamCtx>(req.userData());
            if (sctx && !sctx->files.empty()) {
                for (auto& item : sctx->files) {
                    processOneFile(item, dirPath);
                }
                resp->setStatusCode(HttpResponse::k200Ok);
                resp->setContentType("application/json; charset=utf-8");
                resp->setBody("{\"status\":\"ok\",\"count\":" +
                                std::to_string(sctx->files.size()) + "}");
                return;
            }
    
            // --- 非流式路径（小文件，用 req.body()） ---
            // ... 原有逻辑：MultipartParser + ThreadPool ...
            resp->setStatusCode(HttpResponse::k200Ok);
            resp->setContentType("application/json; charset=utf-8");
            resp->setBody("{\"status\":\"ok\"}");
        }
    
    private:
        void processOneFile(const FileItem& item, const std::string& dirPath) {
            // 1. 流式 SHA256（OpenSSL 分块计算，常数内存 ~32KB）
            std::string hash = item.finalHash;//handle::computeFileHashStreaming(item.tmpFile);
            std::cout << "hash data :" << hash << std::endl;
            if (hash.empty()) return;
    
            struct stat st;
            stat(item.tmpFile.c_str(), &st);
    
            // 2. move 到正式存储
            std::string dstPath = dataStorage->filePath(hash);
            std::string dstDir  = dstPath.substr(0, dstPath.find_last_of('/'));
            ensureDir(dstDir);
    
            if (::rename(item.tmpFile.c_str(), dstPath.c_str()) < 0) {
                copyFileStreaming(item.tmpFile, dstPath);
                unlink(item.tmpFile.c_str());
            }
    
            // 3. 存元数据
            storage::MetaInfo meta;
            meta.file_name    = item.fileName;
            meta.file_hash    = hash;
            meta.file_path    = dirPath;
            meta.file_size    = st.st_size;
            meta.content_type = item.mimeType;
            time_t now = time(nullptr);
            char buf[32];
            strftime(buf, sizeof(buf), "%Y-%m-%d", localtime(&now));
            meta.file_time   = buf;
            meta.file_author = "";
            metaStorage->saveMeta(hash, meta);
        }
    private:
        storage::FileDataStorage* dataStorage;
        storage::FileMetaStorage* metaStorage;
        utils::ThreadPool* threadPool;
        network::EventLoop* loop;
        const char* tmpDir;
    };

}
}


























