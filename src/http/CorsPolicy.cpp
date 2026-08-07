#include "http/CorsPolicy.hpp"

#include "http/HttpResponse.hpp"

#include <utility>

namespace miniKV {
namespace http {

constexpr char kAllowedMethods[] = "GET, HEAD, PUT, OPTIONS";
constexpr char kAllowedHeaders[] =
    "Content-Type, X-Session-Id, X-Chunk-Index, X-Commit-Owner, "
    "X-Gateway-Address, X-Gateway-Port, X-Replica-Chain, "
    "X-Replica-Position, X-Upload-Token, X-Client-Instance-Id";

CorsPolicy::CorsPolicy(std::string allowedOrigin)
    : allowedOrigin_(std::move(allowedOrigin)) {}

bool CorsPolicy::enabled() const
{
    return !allowedOrigin_.empty();
}

bool CorsPolicy::allows(const std::string& origin) const
{
    return origin.empty() || (enabled() && origin == allowedOrigin_);
}

void CorsPolicy::appendHeaders(http::HttpResponse& response, const std::string& origin) const
{
    if(origin.empty() || !allows(origin)) return;
    response.addHeader("Access-Control-Allow-Origin", allowedOrigin_);
    response.addHeader("Access-Control-Allow-Methods", kAllowedMethods);
    response.addHeader("Access-Control-Allow-Headers", kAllowedHeaders);
    response.addHeader("Access-Control-Expose-Headers", "Content-Length, X-Chunk-Hash, Retry-After");
    response.addHeader("Access-Control-Max-Age", "600");
    response.addHeader("Vary", "Origin");
}

}  // namespace miniKV::v2
}  // namespace
