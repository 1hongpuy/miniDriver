// ============================================================
// MiniDrive V3.0 — Handler 解耦版
//
// 所有业务逻辑分散在独立 Handler 文件中
// main 只做初始化 + 路由注册，50 行以内
// ============================================================

#include "network/EventLoop.hpp"
#include "http/HttpServer.hpp"
#include "handler/Router.hpp"
#include "handler/StaticFileHandler.hpp"
#include "handler/FileListHandler.hpp"
#include "handler/DownloadHandler.hpp"
#include "handler/UploadHandler.hpp"
#include "handler/MoveHandler.hpp"
#include "handler/DeleteHandler.hpp"
#include "handler/Common.hpp"
#include "storage/FileDataStorage.hpp"
#include "storage/FileMetaStorage.hpp"
#include "utils/ThreadPool.hpp"
#include <iostream>

using namespace miniKV;
using namespace miniKV::handler;
using namespace miniKV::network;
using namespace miniKV::http;
using namespace miniKV::storage;
using namespace miniKV::utils;

// ===== 全局对象（Handler 通过 extern 引用） =====
std::unique_ptr<FileDataStorage> g_dataStorage;
std::unique_ptr<FileMetaStorage> g_metaStorage;
ThreadPool* g_threadPool = nullptr;
EventLoop*  g_loop       = nullptr;
const char* g_tmpDir     = "../data/tmp/";
const char* g_wwwDir     = "../www/";

static Router g_router;

// ===== 统一入口（就一行） =====
void onHttpRequest(const HttpRequest& req, HttpResponse* resp,
                    const TcpConnectionPtr& conn) {
    if (!g_router.route(req, resp, conn)) {
        resp->setStatusCode(HttpResponse::k404NotFound);
        resp->setBody("Not Found");
    }
}

// ===== main =====
int main() {
    std::cout << "==== MiniDrive V3.0 ====" << std::endl;

    // 1. 目录 + 存储
    ensureDir("../data/tmp/");
    ensureDir("../data/files/");
    ensureDir("../data/meta/");
    ensureDir("../www/");

    g_dataStorage = std::make_unique<FileDataStorage>("../data/files/");
    g_metaStorage = std::make_unique<FileMetaStorage>("../data/meta/", g_dataStorage.get());
    g_dataStorage->init();
    g_metaStorage->init();
    std::cout << "[Storage] OK" << std::endl;

    // 2. 线程池
    ThreadPool pool(4);
    g_threadPool = &pool;

  

    // 4. HttpServer
    EventLoop loop;
    g_loop = &loop;


    // 3. 注册所有 Handler
    g_router.add(std::make_unique<StaticFileHandler>("../www/"));
    g_router.add(std::make_unique<FileListHandler>(g_metaStorage.get()));
    g_router.add(std::make_unique<DownloadHandler>(g_metaStorage.get(), g_dataStorage.get()));
    g_router.add(std::make_unique<UploadHandler>(g_dataStorage.get(),
                                                 g_metaStorage.get(),
                                                 g_threadPool,
                                                 &loop,
                                                "../data/tmp/"));
    g_router.add(std::make_unique<MoveHandler>( g_metaStorage.get()));
    g_router.add(std::make_unique<DeleteHandler>( g_metaStorage.get()));
    std::cout << "[Router] " << g_router.handlers().size()
            << " handlers registered" << std::endl;
    HttpServer server(&loop, &pool, 8080);
    server.setHttpCallback(onHttpRequest);
    server.setStreamCheck(UploadHandler::needsStreaming);
    server.setBodyStreamSetup([](HttpContext* ctx, const HttpRequest& req,
                                   const TcpConnectionPtr& conn) {
        UploadHandler::setupStreaming(ctx, req, conn);
    });
    server.start();

    std::cout << "[HttpServer] :8080" << std::endl;
    std::cout << "  → http://localhost:8080" << std::endl;
    std::cout << "============================" << std::endl;

    loop.loop();
    return 0;
}
