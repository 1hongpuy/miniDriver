#pragma once


#include <algorithm>
#include <memory>
#include <vector>
#include "Handler.hpp"
#include "http/HttpRequest.hpp"
#include "http/HttpResponse.hpp"
#include "network/TcpConnection.hpp"

namespace miniKV {
namespace handler{

class Router {
public:
    void add(std::unique_ptr<Handler> h) {
        handlers_.push_back(std::move(h));
    }
    void addHandler(std::unique_ptr<handler::Handler> h){
    //由于 std::unique_ptr 的拷贝构造函数是已经被禁用了的,这里不能拷贝
        handlers_.push_back(std::move(h));
    }

    bool route(const http::HttpRequest& req,
               http::HttpResponse* resq,
               const network::TcpConnectionPtr& conn) {
        for(auto& h : handlers_)
        {
            if(h->match(req))
            {
                h->handle(req, resq, conn);
                return true;
            }
        }
        return false;
    }

    const std::vector<std::unique_ptr<handler::Handler>>& handlers() const {
        return handlers_;
    }


private:
    std::vector<std::unique_ptr<handler::Handler>> handlers_;

};

}
}



































