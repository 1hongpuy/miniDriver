// ============================================================
// MiniDrive V1.0 — 入口文件（支持虚拟目录）
//
// 路由表：
//   GET  /                     → index.html
//   GET  /style.css             → style.css
//   GET  /app.js               → app.js
//   GET  /api/files?path=/     → 列出目录下的文件 + 子目录（JSON）
//   POST /api/files?path=/     → 上传文件到指定目录
//   POST /api/files/move       → 移动文件 {hash, new_path}
//   DELETE /api/files/{hash}   → 删除文件
//   GET  /api/files/{hash}/download → 下载文件
//
// 编译： cd build && cmake .. && make
// 运行： ./minikv
// 浏览器：http://localhost:8888
// ============================================================

#include "http/HttpContext.hpp"
#include "network/EventLoop.hpp"
#include "network/TcpConnection.hpp"
#include "network/TcpServer.hpp"
#include "http/HttpServer.hpp"
#include "http/HttpRequest.hpp"
#include "http/HttpResponse.hpp"
#include "http/MultipartParser.hpp"
#include "storage/FileDataStorage.hpp"
#include "storage/FileMetaStorage.hpp"
#include "utils/ThreadPool.hpp"

#include <iostream>
#include <fstream>
#include <sstream>
#include <sys/stat.h>
#include <unistd.h>

using namespace miniKV::network;
using namespace miniKV::http;
using namespace miniKV::storage;
using namespace miniKV::utils;

// ============================================================
// 全局对象
// ============================================================
static std::unique_ptr<FileDataStorage> g_dataStorage;
static std::unique_ptr<FileMetaStorage> g_metaStorage;
static ThreadPool*                     g_threadPool = nullptr;
static EventLoop*                      g_loop = nullptr;

static const char* g_tmpDir = "../data/tmp/";
static const char* g_wwwDir = "../www/";

// ============================================================
// 工具函数
// ============================================================

static std::string getMimeType(const std::string& filename) {
    size_t dot = filename.find_last_of('.');
    if (dot == std::string::npos) return "application/octet-stream";
    std::string ext = filename.substr(dot);
    for (auto& c : ext) c = std::tolower(c);

    if (ext == ".html" || ext == ".htm") return "text/html; charset=utf-8";
    if (ext == ".css")                   return "text/css; charset=utf-8";
    if (ext == ".js")                    return "application/javascript; charset=utf-8";
    if (ext == ".json")                  return "application/json; charset=utf-8";
    if (ext == ".jpg" || ext == ".jpeg") return "image/jpeg";
    if (ext == ".png")                   return "image/png";
    if (ext == ".gif")                   return "image/gif";
    if (ext == ".webp")                  return "image/webp";
    if (ext == ".bmp")                   return "image/bmp";
    if (ext == ".svg")                   return "image/svg+xml";
    if (ext == ".mp4")                   return "video/mp4";
    if (ext == ".mov")                   return "video/quicktime";
    if (ext == ".mkv")                   return "video/x-matroska";
    if (ext == ".avi")                   return "video/x-msvideo";
    if (ext == ".raw" || ext == ".nef" || ext == ".cr2" || ext == ".cr3"
        || ext == ".arw" || ext == ".dng" || ext == ".orf" || ext == ".rw2")
                                         return "image/x-raw";
    return "application/octet-stream";
}

static bool ensureDir(const std::string& path) {
    std::string dir = path;
    while (!dir.empty() && dir.back() == '/') dir.pop_back();
    if (dir.empty()) return true;
    struct stat st;
    if (stat(dir.c_str(), &st) == 0) return S_ISDIR(st.st_mode);
    size_t pos = dir.find_last_of('/');
    if (pos != std::string::npos) ensureDir(dir.substr(0, pos));
    if (mkdir(dir.c_str(), 0755) != 0 && errno != EEXIST) return false;
    return true;
}

static std::string readFile(const std::string& path) {
    std::ifstream f(path, std::ios::binary);
    if (!f.is_open()) return "";
    std::stringstream ss;
    ss << f.rdbuf();
    return ss.str();
}

// 从 query string 提取参数
// /api/files?path=/photos/ → 返回 "path=/photos/"
static std::string getQueryParam(const std::string& query, const std::string& key) {
    std::string search = key + "=";
    size_t pos = query.find(search);
    if (pos == std::string::npos) return "";
    pos += search.size();
    size_t end = query.find('&', pos);
    if (end == std::string::npos) end = query.size();
    return query.substr(pos, end - pos);
}

// URL decode
static std::string urlDecode(const std::string& s) {
    std::string result;
    for (size_t i = 0; i < s.size(); ++i) {
        if (s[i] == '%' && i + 2 < s.size()) {
            int c;
            sscanf(s.substr(i + 1, 2).c_str(), "%x", &c);
            result += static_cast<char>(c);
            i += 2;
        } else if (s[i] == '+') {
            result += ' ';
        } else {
            result += s[i];
        }
    }
    return result;
}

// 确保路径以 / 开头和结尾，合并多余斜杠
static std::string normalizePath(const std::string& p) {
    std::string s = p;
    if (s.empty()) return "/";
    if (s[0] != '/') s = "/" + s;        // 确保开头有 /
    while (s.size() > 1 && s.back() == '/') s.pop_back();  // 去末尾 /
    // 合并多重 ///
    size_t pos;
    while ((pos = s.find("//")) != std::string::npos)
        s.replace(pos, 2, "/");
    if (s.empty()) return "/";
    if (s == "/") return "/";              // 根目录不加双斜杠
    return s + "/";
}

// ============================================================
// 静态文件
// ============================================================
static void serveStaticFile(const std::string& filename, HttpResponse* resp) {
    std::string path = std::string(g_wwwDir) + filename;
    std::string content = readFile(path);
    if (content.empty()) {
        resp->setStatusCode(HttpResponse::k404NotFound);
        resp->setBody("File not found");
        return;
    }
    resp->setStatusCode(HttpResponse::k200Ok);
    resp->setContentType(getMimeType(filename));
    resp->setBody(content);
    resp->setCloseConnection(false);
}

// ============================================================
// JSON 转义
// ============================================================
static std::string escapeJson(const std::string& s) {
    std::string out;
    for (char c: s) {
        switch (c) {
            case '"':  out += "\\\""; break;
            case '\\': out += "\\\\"; break;
            case '\n': out += "\\n";  break;
            case '\r': out += "\\r";  break;
            case '\t': out += "\\t";  break;
            default:   out += c;
        }
    }
    return out;
}

// ============================================================
// SHA256 简易计算
// ============================================================
static std::string computeFileHash(const std::string& filepath) {
    char cmd[1024], result[128] = {0};
    snprintf(cmd, sizeof(cmd), "sha256sum %s 2>/dev/null | cut -d' ' -f1", filepath.c_str());
    FILE* fp = popen(cmd, "r");
    if (!fp) return "";
    fgets(result, sizeof(result), fp);
    pclose(fp);
    size_t len = strlen(result);
    if (len > 0 && result[len-1] == '\n') result[len-1] = '\0';
    return std::string(result);
}


//流式接收代码


// ============================================================
// setupStreamingUpload — miniDriver 里的流式上传
// ============================================================

void setupStreamingUpload(HttpContext* ctx, const HttpRequest& req,
    const TcpConnectionPtr& conn) {
    size_t contentLen = req.contentLength();

    // 1. 提取 boundary
    std::string boundary;
    size_t bp = req.contentType().find("boundary=");
    if (bp == std::string::npos) return;
    boundary = req.contentType().substr(bp + 9);

    // 2. 创建 MultipartParser（shared_ptr 跨多次 onMessage 存活）
    auto parser = std::make_shared<MultipartParser>();
    parser->setBoundary(boundary);

    auto tmpFile  = std::make_shared<std::string>();
    auto tmpFp    = std::make_shared<FILE*>(nullptr);
    auto fileName = std::make_shared<std::string>();
    auto ct       = std::make_shared<std::string>();
    auto targetDir = std::make_shared<std::string>(
    normalizePath(urlDecode(getQueryParam(req.query(), "path"))));

    parser->setPartHeaderCallback(
    [tmpFile, tmpFp, fileName, ct](const std::string& fname,
                    const std::string& mime) {
    *fileName = fname; *ct = mime;
    ensureDir("../data/tmp/");
    *tmpFile = std::string("../data/tmp/") + fname + ".upload";
    *tmpFp = fopen(tmpFile->c_str(), "wb");
    });

    parser->setDataCallback(
    [tmpFp](const char* data, size_t len) {
    if (*tmpFp) fwrite(data, 1, len, *tmpFp);  // 边收边写盘
    });

    parser->setPartEndCallback(
    [tmpFile, tmpFp, fileName, ct, targetDir, conn]() {
    if (*tmpFp) { fclose(*tmpFp); *tmpFp = nullptr; }
    if (tmpFile->empty()) return;

    g_threadPool->enqueue([tmpFile, fileName, ct, targetDir, conn]() {
    // 哈希 + 存盘 + 元数据（和原来一样）
    std::string hash = computeFileHash(*tmpFile);
    std::string fileData = readFile(*tmpFile);
    if (fileData.empty()) return;

    g_dataStorage->writeData(hash, fileData.data(), fileData.size());
    MetaInfo meta;
    meta.file_name = *fileName; meta.file_hash = hash;
    meta.file_path = *targetDir; meta.file_size = fileData.size();
    meta.content_type = *ct;
    // ... time, author, thumb status ...
    g_metaStorage->saveMeta(hash, meta);
    unlink(tmpFile->c_str());

    // 回 IO 线程发 200
    g_loop->queueInLoop([conn]() {
    HttpResponse resp;
    resp.setStatusCode(HttpResponse::k200Ok);
    resp.setContentType("application/json; charset=utf-8");
    resp.setBody("{\"status\":\"ok\"}");
    Buffer outBuf;
    resp.appendToBuffer(&outBuf);
    conn->send(std::string(outBuf.peek(), outBuf.readableBytes()));
    conn->shutdown();
    });
    });
    });

    // === 核心：注册流式回调 ===
    ctx->setBodyCallback(contentLen,
    [parser](const char* data, size_t len) {
    parser->feed(data, len);
    });
}


// ============================================================
// 业务路由
// ============================================================
static void onHttpRequest(const HttpRequest& req,
                           HttpResponse* resp,
                           const TcpConnectionPtr& conn) {
    std::string path = req.path();
    std::string query = req.query();
    std::cout << "[ROUTE] method=" << req.methodString() 
          << " path=|" << req.path() << "|"
          << " query=|" << req.query() << "|"
          << " ct=|" << req.contentType() << "|" << std::endl;
    // ---- 静态网页 ----
    if (path == "/" || path == "/index.html") {
        serveStaticFile("index.html", resp);
        return;
    }
    if (path == "/style.css") {
        serveStaticFile("style.css", resp);
        return;
    }
    if (path == "/app.js") {
        serveStaticFile("app.js", resp);
        return;
    }
    if (path == "/favicon.ico" || path == "/favicon.svg") {
        serveStaticFile("favicon.svg", resp);
        return;
    }

    // ==== API: 列出目录（文件 + 子目录） ====
    // GET /api/files?path=/photos/
    if (path == "/api/files" && req.method() == HttpRequest::kGet) {
        std::string dirPath = urlDecode(getQueryParam(query, "path"));
        dirPath = normalizePath(dirPath);

        auto files       = g_metaStorage->listByPath(dirPath);
        auto subDirs     = g_metaStorage->listSubDirs(dirPath);

        // 构造 JSON: { "path": "/", "folders": [...], "files": [...] }
        std::string json = "{\"path\":\"" + escapeJson(dirPath) + "\",";

        // 子目录数组
        json += "\"folders\":[";
        for (size_t i = 0; i < subDirs.size(); ++i) {
            if (i > 0) json += ",";
            json += "\"" + escapeJson(subDirs[i]) + "\"";
        }
        json += "],";

        // 文件数组
        json += "\"files\":[";
        for (size_t i = 0; i < files.size(); ++i) {
            if (i > 0) json += ",";
            json += "{";
            json += "\"file_name\":\""    + escapeJson(files[i].file_name)    + "\",";
            json += "\"file_hash\":\""    + escapeJson(files[i].file_hash)    + "\",";
            json += "\"file_path\":\""    + escapeJson(files[i].file_path)    + "\",";
            json += "\"file_size\":"      + std::to_string(files[i].file_size) + ",";
            json += "\"file_time\":\""    + escapeJson(files[i].file_time)    + "\",";
            json += "\"content_type\":\"" + escapeJson(files[i].content_type)  + "\"";
            json += "}";
        }
        json += "]}";

        resp->setStatusCode(HttpResponse::k200Ok);
        resp->setContentType("application/json; charset=utf-8");
        resp->setBody(json);
        resp->setCloseConnection(false);
        return;
    }

    // ==== API: 下载/预览文件 ====
    // GET /api/files/{hash}/download?path=/xxx/&name=xxx.jpg
    if (path.find("/api/files/") == 0 && path.find("/download") != std::string::npos) {
        size_t start = 11;
        size_t end   = path.find("/download", start);
        if (end == std::string::npos) {
            resp->setStatusCode(HttpResponse::k400BadRequest);
            resp->setBody("Invalid URL");
            return;
        }
        std::string hash = path.substr(start, end - start);

        // 优先从路径查 MetaInfo（有 file_name），fallback 到 hash
        MetaInfo meta;
        bool found = false;
        std::string filePath = urlDecode(getQueryParam(query, "path"));
        std::string fileName = urlDecode(getQueryParam(query, "name"));
        if (!filePath.empty() && !fileName.empty()) {
            found = g_metaStorage->getMetaByPath(filePath, fileName, meta);
        }
        if (!found) {
            found = g_metaStorage->getMeta(hash, meta);
        }
        if (!found) {
            resp->setStatusCode(HttpResponse::k404NotFound);
            resp->setBody("File not found");
            return;
        }

        std::string fileData;
        if (!g_dataStorage->readData(hash, fileData, meta.file_size)) {
            resp->setStatusCode(HttpResponse::k404NotFound);
            resp->setBody("Data not found");
            return;
        }

        std::string fname = meta.file_name.empty() ? "download" : meta.file_name;
        bool forceDownload = !getQueryParam(query, "dl").empty();
        std::string disposition = forceDownload ? "attachment" : "inline";
        
        std::string diskPath = g_dataStorage->filePath(hash);
        resp->setStatusCode(HttpResponse::k200Ok);
        resp->setContentType(getMimeType(fname));
        resp->addHeader("Content-Disposition", disposition + "; filename=\"" + fname + "\"");
        resp->addHeader("Cache-Control", "max-age=3600");
        resp->setFileBody(diskPath, meta.file_size);
        return;
    }

    // ==== API: 移动文件 ====
    // POST /api/files/move
    // Body: {"old_path":"/张三/照片/","old_name":"海.jpg",
    //        "new_path":"/李四/收藏/","new_name":"海.jpg"}
    if (path == "/api/files/move" && req.method() == HttpRequest::kPost) {
        std::string body = req.body();

        auto extract = [&body](const std::string& key) -> std::string {
            size_t p = body.find('"' + key + '"');
            if (p == std::string::npos) return "";
            p = body.find('"', p + key.size() + 3);
            if (p == std::string::npos) return "";
            size_t end = body.find('"', p + 1);
            if (end == std::string::npos) return "";
            return body.substr(p + 1, end - p - 1);
        };


        
        std::string oldPath = extract("old_path");
        std::string oldName = extract("old_name");
        std::string newPath = extract("new_path");
        std::string newName = extract("new_name");

        if (oldPath.empty() || oldName.empty() || newPath.empty()) {
            resp->setStatusCode(HttpResponse::k400BadRequest);
            resp->setBody("Missing old_path, old_name, or new_path");
            return;
        }
        if (newName.empty()) newName = oldName;  // 不换名

        if (g_metaStorage->moveByPath(oldPath, oldName, newPath, newName)) {
            resp->setStatusCode(HttpResponse::k200Ok);
            resp->setContentType("application/json");
            resp->setBody("{\"status\":\"ok\"}");
        } else {
            resp->setStatusCode(HttpResponse::k404NotFound);
            resp->setBody("File not found or move failed");
        }
        return;
    }

    // ==== API: 删除文件 ====
    // DELETE /api/files?path=/张三/照片/&name=海.jpg
    if (path == "/api/files" && req.method() == HttpRequest::kDelete) {
        std::string filePath = urlDecode(getQueryParam(query, "path"));
        std::string fileName = urlDecode(getQueryParam(query, "name"));

        if (filePath.empty() || fileName.empty()) {
            resp->setStatusCode(HttpResponse::k400BadRequest);
            resp->setBody("Missing path or name");
            return;
        }

        // 路径级删除：只删这个路径的门牌号
        // LevelDB 内部维护引用计数，ref_count==0 时才删物理文件
        if (!g_metaStorage->removeByPath(filePath, fileName)) {
            resp->setStatusCode(HttpResponse::k404NotFound);
            resp->setBody("File not found");
            return;
        }

        resp->setStatusCode(HttpResponse::k200Ok);
        resp->setContentType("application/json");
        resp->setBody("{\"status\":\"ok\"}");
        return;
    }

    // ==== API: 上传文件 ====
    // POST /api/files?path=/photos/
    if (path == "/api/files" && req.method() == HttpRequest::kPost) {
        std::string dirPath = urlDecode(getQueryParam(query, "path"));
        dirPath = normalizePath(dirPath);

        std::string contentType = req.contentType();

        // 提取 boundary
        std::string boundary;
        size_t bp = contentType.find("boundary=");
        if (bp == std::string::npos) {
            resp->setStatusCode(HttpResponse::k400BadRequest);
            resp->setBody("Missing boundary in Content-Type");
            return;
        }
        boundary = contentType.substr(bp + 9);
        if (!boundary.empty() && boundary.front() == '"') {
            boundary = boundary.substr(1, boundary.size() - 2);
        }

        auto parser = std::make_shared<MultipartParser>();
        parser->setBoundary(boundary);

        // 临时变量
        auto tmpFile  = std::make_shared<std::string>();
        auto tmpFp    = std::make_shared<FILE*>(nullptr);
        auto fileName = std::make_shared<std::string>();
        auto ct       = std::make_shared<std::string>();
        auto uploaded = std::make_shared<int>(0);
        auto targetDir = std::make_shared<std::string>(dirPath);

        parser->setPartHeaderCallback(
            [tmpFile, tmpFp, fileName, ct](const std::string& fname, const std::string& mime) {
                *fileName = fname;
                *ct       = mime;
                ensureDir(g_tmpDir);
                *tmpFile = std::string(g_tmpDir) + fname + ".upload";
                *tmpFp = fopen(tmpFile->c_str(), "wb");
            });

        parser->setDataCallback(
            [tmpFp](const char* data, size_t len) {
                if (*tmpFp) fwrite(data, 1, len, *tmpFp);
            });

        parser->setPartEndCallback(
            [tmpFile, tmpFp, fileName, ct, uploaded, targetDir, parser, conn]() {
                if (*tmpFp) { fclose(*tmpFp); *tmpFp = nullptr; }
                if (tmpFile->empty()) return;
                (*uploaded)++;

                g_threadPool->enqueue([tmpFile, fileName, ct, targetDir, parser, conn]() {
                    std::string hash = computeFileHash(*tmpFile);
                    std::string fileData = readFile(*tmpFile);
                    if (fileData.empty()) return;

                    g_dataStorage->writeData(hash, fileData.data(), fileData.size());

                    MetaInfo meta;
                    meta.file_name    = *fileName;
                    meta.file_hash    = hash;
                    meta.file_path    = *targetDir;
                    meta.file_size    = fileData.size();
                    meta.content_type = *ct;

                    time_t now = time(nullptr);
                    char timeBuf[32];
                    strftime(timeBuf, sizeof(timeBuf), "%Y-%m-%d", localtime(&now));
                    meta.file_time   = timeBuf;
                    meta.file_author = "";

                    g_metaStorage->saveMeta(hash, meta);
                    unlink(tmpFile->c_str());
                });
            });

        const std::string& body = req.body();
        if (!body.empty()) {
            parser->feed(body.data(), body.size());
            parser->finish();
        }

        resp->setStatusCode(HttpResponse::k200Ok);
        resp->setContentType("application/json; charset=utf-8");
        resp->setBody("{\"status\":\"ok\",\"message\":\"upload processing\",\"path\":\"" +
                       escapeJson(dirPath) + "\"}");
        return;
    }

    // ---- 404 ----
    resp->setStatusCode(HttpResponse::k404NotFound);
    resp->setBody("Not Found");
}

// ============================================================
// main
// ============================================================
int main() {
    std::cout << "========================================" << std::endl;
    std::cout << "  MiniDrive V1.0 启动中..." << std::endl;
    std::cout << "========================================" << std::endl;

    ensureDir("../data/tmp/");
    ensureDir("../data/files/");
    ensureDir("../data/meta/");
    ensureDir("../www/");

    g_dataStorage = std::make_unique<FileDataStorage>("../data/files/");
    g_metaStorage = std::make_unique<FileMetaStorage>("../data/meta/");
    g_dataStorage->init();
    g_metaStorage->init();

    std::cout << "[Storage] OK  data: ../data/files/   meta: ../data/meta/" << std::endl;

    ThreadPool pool(4);
    g_threadPool = &pool;
    std::cout << "[ThreadPool] 4 workers" << std::endl;

    EventLoop loop;
    g_loop = &loop;

    HttpServer server(&loop, &pool, 8080);
    server.setHttpCallback(onHttpRequest);
    server.setBodyStreamSetup([](HttpContext* ctx, const HttpRequest& req, 
                                 const TcpConnectionPtr& conn){
                                    setupStreamingUpload(ctx, req, conn);
                                 });
    
    server.setStreamCheck([](const HttpRequest&req)->bool{
        if(req.method() != HttpRequest::kPost) return false;
        if(req.path() != "/api/files") return false;
        if(req.contentLength() < 65536) return false;
        return true;
    });
    
    
    
    
    
    server.start();

    std::cout << "[HttpServer] :8080" << std::endl;
    std::cout << "  → http://localhost:8080" << std::endl;
    std::cout << "========================================" << std::endl;

    loop.loop();
    std::cout << "MiniDrive stopped." << std::endl;
    return 0;
}
