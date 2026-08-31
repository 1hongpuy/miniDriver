#pragma once

#include "client/HttpTransport.hpp"

namespace miniKV::benchmark {

// Compatibility aliases. Benchmark no longer owns a separate HTTP client:
// it exercises the same transport used by minidriver-client-core.
using Endpoint = miniKV::client::Endpoint;
using HttpResponse = miniKV::client::HttpResponse;
using StreamingRequest = miniKV::client::StreamingRequest;
using miniKV::client::httpRequest;

}  // namespace miniKV::benchmark
