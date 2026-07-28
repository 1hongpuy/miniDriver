#pragma once

#include <string>

namespace miniKV::http {
class HttpResponse;

class CorsPolicy {
public:
    explicit CorsPolicy(std::string allowedOrigin);

    bool enabled() const;
    bool allows(const std::string& origin) const;
    void appendHeaders(http::HttpResponse& response, const std::string& origin) const;

private:
    std::string allowedOrigin_;
};

}  // namespace miniKV::v2
