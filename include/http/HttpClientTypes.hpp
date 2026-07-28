#pragma once


#include <map>
#include <string>




namespace miniKV{
namespace http {

struct HttpClientResponse
{
    int status = 0;
    std::map<std::string, std::string> headers;
    std::string body;
};


}


}























