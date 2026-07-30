# V2 Local Data Plane Benchmark Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `minikv_v2_bench`, a reproducible command-line benchmark that drives the complete V2 upload and download protocol against local Gateway and DataNode processes and writes machine-readable results.

**Architecture:** Add a small synchronous benchmark-only HTTP client that owns no server state and does not alter the epoll data plane. `BenchmarkRunner` generates deterministic files, calls the existing V2 HTTP endpoints, uploads chunks in 64 KiB writes, downloads through manifests, validates hashes, and emits one record per run plus per-size aggregates.

**Tech Stack:** C++17, POSIX sockets/poll, OpenSSL EVP SHA-256, existing `miniKV::util` JSON helpers, CMake, CSV and JSON.

## Global Constraints

- Keep V2 Gateway/DataNode endpoints, Token headers, Chunk format, and object metadata unchanged.
- Benchmark data must be under the supplied `--work-dir`; never use a DataNode `dataDir`.
- Default sizes are exactly `4MiB,32MiB,256MiB,1GiB`; default runs is `3`.
- Use a fixed 64 KiB I/O buffer for hash, PUT, and download file writes.
- A failed run is written to `runs.csv`, excluded from summary timing statistics, and makes the process exit non-zero after all requested runs finish.
- Do not add Python, gnuplot, or other runtime dependencies in this phase.

---

## File Structure

```text
include/benchmark/
  BenchmarkTypes.hpp          CLI options, per-run record, aggregate types
  BenchmarkHttpClient.hpp     synchronous HTTP/1.1 client used only by benchmark
  BenchmarkRunner.hpp         complete V2 upload/download orchestration

src/benchmark/
  BenchmarkTypes.cpp          size parsing, throughput/statistics, CSV/JSON serialization
  BenchmarkHttpClient.cpp     connect, request, streaming PUT, response parsing
  BenchmarkRunner.cpp         deterministic input, V2 protocol, validation, report writes
  benchmark_main.cpp          CLI parsing and process exit policy

test/
  test_benchmark_types.cpp    pure parsing/statistics/report tests
  test_benchmark_http.cpp     loopback response parsing and Content-Length test
```

`minikv_benchmark` is a static library shared by the test targets and linked into the final executable. It is deliberately not linked into Gateway or DataNode binaries.

---

### Task 1: Benchmark value types and report math

**Files:**
- Create: `include/benchmark/BenchmarkTypes.hpp`
- Create: `src/benchmark/BenchmarkTypes.cpp`
- Create: `test/test_benchmark_types.cpp`
- Modify: `CMakeLists.txt`

**Consumes:** C++17 standard library only.

**Produces:**

```cpp
namespace miniKV::benchmark {
struct Endpoint { std::string host; uint16_t port = 0; };
struct BenchmarkOptions {
    Endpoint gateway;
    std::filesystem::path workDir;
    std::vector<uint64_t> sizes;
    uint32_t runs = 3;
    std::string remoteDir = "/benchmark";
};
struct RunRecord {
    std::string runId; uint64_t sizeBytes = 0; uint32_t chunkCount = 0;
    double uploadMs = 0; double downloadMs = 0;
    bool uploadOk = false; bool downloadOk = false; std::string error;
};
struct SizeSummary {
    uint64_t sizeBytes = 0; uint32_t requestedRuns = 0; uint32_t successCount = 0;
    double uploadMinMs = 0, uploadMedianMs = 0, uploadMeanMs = 0;
    double downloadMinMs = 0, downloadMedianMs = 0, downloadMeanMs = 0;
};
bool parseSize(std::string_view text, uint64_t& bytes);
bool parseEndpoint(std::string_view text, Endpoint& endpoint);
double mibPerSecond(uint64_t bytes, double elapsedMs);
SizeSummary summarize(uint64_t sizeBytes, uint32_t requestedRuns,
                      const std::vector<RunRecord>& records);
}
```

- [ ] **Step 1: Write the failing value-type test**

```cpp
int main() {
    using namespace miniKV::benchmark;
    uint64_t bytes = 0;
    assert(parseSize("4MiB", bytes) && bytes == 4ULL * 1024 * 1024);
    assert(parseSize("1GiB", bytes) && bytes == 1024ULL * 1024 * 1024);
    assert(!parseSize("4MB", bytes));
    const std::vector<RunRecord> records{
        {"a", 4, 1, 10, 20, true, true, ""},
        {"b", 4, 1, 30, 40, true, true, ""},
        {"c", 4, 1, 999, 999, false, false, "HTTP 500"},
    };
    const SizeSummary summary = summarize(4, 3, records);
    assert(summary.successCount == 2);
    assert(summary.uploadMedianMs == 20.0);
}
```

- [ ] **Step 2: Compile and run the test to verify RED**

Run: `cmake -S . -B build && cmake --build build --target test_benchmark_types -j2 && ./build/bin/test_benchmark_types`

Expected: compilation fails because `benchmark/BenchmarkTypes.hpp` does not exist.

- [ ] **Step 3: Implement minimal parsing and summary logic**

`parseSize` accepts an unsigned decimal value followed by exactly `MiB` or `GiB`, checks multiplication overflow, and rejects zero. `summarize` includes only `uploadOk && downloadOk && error.empty()` records; it sorts upload and download samples separately, chooses the lower middle element for an even count, and returns zero metrics when no run succeeds.

- [ ] **Step 4: Add CMake targets and verify GREEN**

```cmake
file(GLOB BENCHMARK_SRC src/benchmark/*.cpp)
list(REMOVE_ITEM BENCHMARK_SRC "${CMAKE_CURRENT_SOURCE_DIR}/src/benchmark/benchmark_main.cpp")
add_library(minikv_benchmark STATIC ${BENCHMARK_SRC})
target_include_directories(minikv_benchmark PUBLIC include)
target_link_libraries(minikv_benchmark PUBLIC minikv_utils OpenSSL::Crypto pthread)

add_executable(test_benchmark_types test/test_benchmark_types.cpp)
target_link_libraries(test_benchmark_types PRIVATE minikv_benchmark)
```

Run: `cmake --build build --target test_benchmark_types -j2 && ./build/bin/test_benchmark_types`

Expected: `PASS: benchmark size parsing and summary statistics`.

- [ ] **Step 5: Commit the self-contained task**

```bash
git add CMakeLists.txt include/benchmark/BenchmarkTypes.hpp src/benchmark/BenchmarkTypes.cpp test/test_benchmark_types.cpp
git commit -m "feat: add V2 benchmark report primitives"
```

### Task 2: Synchronous benchmark HTTP client

**Files:**
- Create: `include/benchmark/BenchmarkHttpClient.hpp`
- Create: `src/benchmark/BenchmarkHttpClient.cpp`
- Create: `test/test_benchmark_http.cpp`
- Modify: `CMakeLists.txt`

**Consumes:** `benchmark::Endpoint` from Task 1.

**Produces:**

```cpp
struct HttpResponse {
    int status = 0;
    std::map<std::string, std::string> headers;
    std::string body;
};
class StreamingRequest {
public:
    bool open(const Endpoint&, std::string_view method, std::string_view path,
              const std::map<std::string, std::string>& headers,
              uint64_t contentLength, int timeoutMs, std::string& error);
    bool write(const char* bytes, size_t size, std::string& error);
    bool finish(HttpResponse& response, std::string& error);
    void cancel();
};
bool httpRequest(const Endpoint&, std::string_view method, std::string_view path,
                 const std::map<std::string, std::string>& headers,
                 std::string_view body, int timeoutMs,
                 HttpResponse& response, std::string& error);
```

- [ ] **Step 1: Write a failing loopback HTTP test**

The test starts a loopback socket server that sends two recv fragments:

```text
HTTP/1.1 200 OK\r\nContent-Length: 3\r\nX-Test: yes\r\n\r\nabc
```

The test calls `httpRequest`, then asserts `status == 200`, `body == "abc"`, and `headers.at("Content-Length") == "3"`.

- [ ] **Step 2: Run RED**

Run: `cmake --build build --target test_benchmark_http -j2 && ./build/bin/test_benchmark_http`

Expected: compilation fails because `BenchmarkHttpClient` does not exist.

- [ ] **Step 3: Implement the minimal HTTP client**

Implement nonblocking `connect`, wait with `poll`, `SO_ERROR` verification, `MSG_NOSIGNAL` writes, and response parsing. The response reader must parse headers first, obtain case-insensitive `Content-Length`, and keep receiving until exactly that many body bytes arrive. It must not rely on peer close because V2 HTTP responses may be keep-alive. Return a descriptive error on timeout, malformed headers, short body, or socket failure.

- [ ] **Step 4: Verify GREEN**

Run: `cmake --build build --target test_benchmark_http -j2 && ./build/bin/test_benchmark_http`

Expected: `PASS: benchmark HTTP client reads Content-Length response`.

- [ ] **Step 5: Commit the self-contained task**

```bash
git add CMakeLists.txt include/benchmark/BenchmarkHttpClient.hpp src/benchmark/BenchmarkHttpClient.cpp test/test_benchmark_http.cpp
git commit -m "feat: add benchmark HTTP client"
```

### Task 3: Full V2 benchmark runner

**Files:**
- Create: `include/benchmark/BenchmarkRunner.hpp`
- Create: `src/benchmark/BenchmarkRunner.cpp`
- Modify: `src/benchmark/BenchmarkTypes.cpp`

**Consumes:** `BenchmarkOptions`, `RunRecord`, `StreamingRequest`, `httpRequest`, `miniKV::util::{jsonString,jsonUint,jsonObjectArray,jsonEscape,join,sha256Hex}`.

**Produces:**

```cpp
class BenchmarkRunner {
public:
    explicit BenchmarkRunner(BenchmarkOptions options);
    int run(std::ostream& console);
};
```

- [ ] **Step 1: Write failing runner output tests**

Add a test-only `writeReports(workDir, records, summaries)` declaration in `BenchmarkTypes.hpp`. Test creates a temporary directory, writes one successful and one failed `RunRecord`, then asserts:

```text
runs.csv has header and two data rows
summary.csv success_count is 1
summary.json contains "requestedRuns":2 and the configured Gateway host
```

- [ ] **Step 2: Run RED**

Run: `cmake --build build --target test_benchmark_types -j2 && ./build/bin/test_benchmark_types`

Expected: fail because report writing is absent.

- [ ] **Step 3: Implement deterministic files and protocol sequence**

For each `(size, runIndex)`:

1. Create `${workDir}/input-${size}-run-${runIndex}.bin` by repeatedly writing a 64 KiB buffer where byte `i` is `(i + runIndex) & 0xff`.
2. Compute the input whole-file SHA-256 with OpenSSL EVP using 64 KiB reads.
3. `POST /api/v2/upload/sessions` with generated filename, configured remote directory and exact file size.
4. `GET /api/v2/upload/sessions/{sessionId}` to read `chunkSize` and `totalChunks`.
5. For every index, hash exactly one range, request one route, build `X-Replica-Chain` from route `chain`, and stream the range to `primaryAddress:primaryPort` with all V2 upload headers.
6. `POST /api/v2/upload/sessions/{sessionId}/commit`; parse `fileHash`.
7. `GET /api/v2/files/{fileHash}/manifest`; for each ordered chunk, GET replicas in listed order until a 200 response hashes to the manifest hash, then append it to `${workDir}/download-${size}-run-${runIndex}.bin`.
8. Compare the input and output whole-file SHA-256. Mark both success flags only after that comparison passes.
9. Capture upload timing around steps 3-6 and download timing around steps 7-8 using `std::chrono::steady_clock`.

Use 30-second Gateway timeouts and 60-second DataNode PUT/GET timeouts. On every early return set `RunRecord::error` to the endpoint and protocol operation that failed.

- [ ] **Step 4: Implement CSV/JSON output and verify GREEN**

`runs.csv` must use the exact header from the spec. Escape CSV error quotes by doubling `"`; write `summary.csv` with only computed aggregates; write one JSON object with `gateway`, `sizes`, `runs`, and summaries. Re-run `test_benchmark_types`.

- [ ] **Step 5: Commit the self-contained task**

```bash
git add include/benchmark/BenchmarkRunner.hpp src/benchmark/BenchmarkRunner.cpp src/benchmark/BenchmarkTypes.cpp test/test_benchmark_types.cpp
git commit -m "feat: add complete V2 benchmark runner"
```

### Task 4: CLI executable and local smoke test

**Files:**
- Create: `src/benchmark/benchmark_main.cpp`
- Modify: `CMakeLists.txt`
- Create: `docs/V2_LOCAL_BENCHMARK.md`

**Consumes:** `BenchmarkRunner(BenchmarkOptions)` from Task 3.

**Produces:** `build/bin/minikv_v2_bench`.

- [ ] **Step 1: Write a failing CLI parser test**

Expose a testable function:

```cpp
bool parseBenchmarkOptions(const std::vector<std::string>& args,
                           BenchmarkOptions& options, std::string& error);
```

Test valid arguments and assert the parsed `127.0.0.1:18081`, `4MiB,1GiB`, `runs=3`, and `/benchmark`. Test missing `--work-dir` and invalid endpoint `127.0.0.1` and assert `false` with a nonempty error.

- [ ] **Step 2: Run RED**

Run: `cmake --build build --target test_benchmark_types -j2 && ./build/bin/test_benchmark_types`

Expected: fail because CLI parsing is absent.

- [ ] **Step 3: Implement CLI and CMake target**

Accept only:

```text
minikv_v2_bench local --gateway HOST:PORT --work-dir PATH
  [--sizes LIST] [--runs COUNT] [--remote-dir PATH]
```

Reject unknown flags, missing values, zero runs, non-absolute/empty work directories, invalid sizes, and malformed endpoints. Create the supplied work directory before running. Add:

```cmake
add_executable(minikv_v2_bench src/benchmark/benchmark_main.cpp)
target_link_libraries(minikv_v2_bench PRIVATE minikv_benchmark minikv_utils OpenSSL::Crypto pthread)
```

- [ ] **Step 4: Verify tests, build, and run a local smoke benchmark**

Run:

```bash
cmake -S . -B build
cmake --build build -j2
./build/bin/test_benchmark_types
./build/bin/test_benchmark_http
./build/bin/minikv_v2_bench local \
  --gateway 127.0.0.1:18081 \
  --work-dir /tmp/minikv-v2-bench-smoke \
  --sizes 4MiB --runs 1 --remote-dir /benchmark
```

Expected: one successful `runs.csv` row, one successful `summary.csv` row, matching input/output SHA-256, and zero exit status. If Gateway/DataNode processes are not running, the command must exit non-zero while still producing `runs.csv` with its error.

- [ ] **Step 5: Document real baseline execution**

`docs/V2_LOCAL_BENCHMARK.md` documents process startup prerequisites, the four-size command, output fields, failure interpretation, and that `/tmp/minikv-v2-bench` may consume approximately 2 GiB plus uploaded DataNode storage during the 1 GiB run.

- [ ] **Step 6: Commit the self-contained task**

```bash
git add CMakeLists.txt src/benchmark/benchmark_main.cpp docs/V2_LOCAL_BENCHMARK.md test/test_benchmark_types.cpp
git commit -m "feat: add local V2 data plane benchmark"
```

## Plan Self-Review

- Spec coverage: local full protocol, fixed matrix/defaults, 64 KiB I/O, correctness checks, CSV/JSON, and no chart dependency are covered by Tasks 1-4.
- Deliberate exclusion: remote/Tailscale measurements, service resource sampling, concurrent clients, and graph rendering remain outside this plan.
- Type consistency: `BenchmarkOptions`, `RunRecord`, `SizeSummary`, `Endpoint`, `HttpResponse`, `StreamingRequest`, and `BenchmarkRunner` are defined before they are consumed.
- No placeholders: all endpoint paths, result fields, timeout values, command form, tests, and failure behavior are explicit.
