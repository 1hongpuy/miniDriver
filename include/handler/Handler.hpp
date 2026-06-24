#pragma once

#include "http/HttpRequest.hpp"
#include "http/HttpResponse.hpp"
#include "network/TcpConnection.hpp"

namespace miniKV {
namespace handler {

class Handler{
public:
    virtual ~Handler() = default;
    virtual bool match(const http::HttpRequest& req) = 0;
    virtual void handle(const http::HttpRequest& req, 
                        http::HttpResponse* resp, 
                        const network::TcpConnectionPtr& conn) = 0;
};

}


}































