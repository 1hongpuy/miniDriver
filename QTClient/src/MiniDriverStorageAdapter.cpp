#include "qtclient/MiniDriverStorageAdapter.hpp"

#include "utils/Util.hpp"

#include <QByteArray>

#include <algorithm>
#include <atomic>
#include <array>
#include <filesystem>
#include <fstream>
#include <memory>
#include <map>
#include <utility>

#include <openssl/evp.h>

namespace miniKV::qtclient {

namespace {

std::string queryEscape(const QString& value) {
    static constexpr char digits[] = "0123456789ABCDEF";
    const QByteArray utf8 = value.toUtf8();
    std::string result;
    result.reserve(static_cast<size_t>(utf8.size()) * 3U);
    for (const unsigned char byte : utf8) {
        const bool safe = (byte >= 'a' && byte <= 'z') ||
                          (byte >= 'A' && byte <= 'Z') ||
                          (byte >= '0' && byte <= '9') ||
                          byte == '-' || byte == '_' || byte == '.' || byte == '~';
        if (safe) {
            result.push_back(static_cast<char>(byte));
        } else {
            result.push_back('%');
            result.push_back(digits[(byte >> 4U) & 0x0fU]);
            result.push_back(digits[byte & 0x0fU]);
        }
    }
    return result;
}

}  // namespace

MiniDriverStorageAdapter::MiniDriverStorageAdapter(miniKV::client::ClientConfig config)
    : config_(config), client_(std::move(config)) {}

bool MiniDriverStorageAdapter::sha256File(const std::filesystem::path& path,
                                          std::string& digest,
                                          std::string& error) {
    std::ifstream input(path, std::ios::binary);
    if (!input) {
        error = "cannot open file for SHA-256: " + path.string();
        return false;
    }
    using Context = std::unique_ptr<EVP_MD_CTX, decltype(&EVP_MD_CTX_free)>;
    Context context(EVP_MD_CTX_new(), &EVP_MD_CTX_free);
    if (!context || EVP_DigestInit_ex(context.get(), EVP_sha256(), nullptr) != 1) {
        error = "cannot initialize SHA-256";
        return false;
    }
    std::array<char, 1024 * 1024> buffer{};
    while (input) {
        input.read(buffer.data(), static_cast<std::streamsize>(buffer.size()));
        const std::streamsize count = input.gcount();
        if (count > 0 && EVP_DigestUpdate(context.get(), buffer.data(),
                                          static_cast<size_t>(count)) != 1) {
            error = "SHA-256 update failed";
            return false;
        }
    }
    if (!input.eof()) {
        error = "read failed while computing SHA-256: " + path.string();
        return false;
    }
    unsigned char output[EVP_MAX_MD_SIZE]{};
    unsigned int outputSize = 0;
    if (EVP_DigestFinal_ex(context.get(), output, &outputSize) != 1) {
        error = "SHA-256 finalize failed";
        return false;
    }
    static constexpr char digits[] = "0123456789abcdef";
    digest.clear();
    digest.reserve(outputSize * 2);
    for (unsigned int index = 0; index < outputSize; ++index) {
        digest.push_back(digits[(output[index] >> 4) & 0x0fU]);
        digest.push_back(digits[output[index] & 0x0fU]);
    }
    return true;
}

bool MiniDriverStorageAdapter::upload(const std::filesystem::path& input,
                                      const ProgressCallback& progress,
                                      miniKV::client::ObjectRef& object,
                                      std::string& error,
                                      const std::string& commandId) {
    std::error_code ec;
    const uint64_t total = std::filesystem::file_size(input, ec);
    if (ec || total == 0) {
        error = "cannot determine a non-empty input file";
        return false;
    }

    miniKV::client::UploadOptions options;
    options.commandId = commandId;
    auto reportedBytes = std::make_shared<std::atomic<uint64_t>>(0);
    options.onProgress = [progress, reportedBytes](const miniKV::client::UploadProgress& value) {
        if (!progress) return;
        uint64_t observed = reportedBytes->load(std::memory_order_relaxed);
        while (observed < value.logicalBytesCompleted &&
               !reportedBytes->compare_exchange_weak(
                   observed, value.logicalBytesCompleted, std::memory_order_relaxed)) {
        }
        progress(std::max(observed, value.logicalBytesCompleted), value.totalBytes);
    };
    miniKV::client::UploadResult result;
    const std::string name = input.filename().string();
    if (!client_.uploadFile(input, name, "/", options, result, error)) return false;
    object = result.object;
    if (progress) progress(total, total);
    return true;
}

bool MiniDriverStorageAdapter::download(const miniKV::client::ObjectRef& object,
                                        const std::filesystem::path& output,
                                        const ProgressCallback& progress,
                                        std::string& error) {
    miniKV::client::ObjectReadPlan plan;
    if (!client_.getReadPlan(object, plan, error)) return false;

    if (progress) progress(0, plan.fileSize);

    const std::filesystem::path partial = output.string() + ".part";
    std::ofstream stream(partial, std::ios::binary | std::ios::trunc);
    if (!stream) {
        error = "cannot open download temporary file";
        return false;
    }
    uint64_t completed = 0;
    miniKV::client::ReadOptions options;
    miniKV::client::TransferStats stats;
    const bool ok = client_.getObject(
        object, options,
        [&stream, &completed, &progress, total = plan.fileSize](const char* data,
                                                                  size_t size,
                                                                  std::string& sinkError) {
            stream.write(data, static_cast<std::streamsize>(size));
            if (!stream) {
                sinkError = "cannot write download temporary file";
                return false;
            }
            completed += size;
            if (progress) progress(completed, total);
            return true;
        }, stats, error);
    stream.close();
    if (!ok) {
        std::error_code ignored;
        std::filesystem::remove(partial, ignored);
        return false;
    }
    std::error_code renameError;
    std::filesystem::rename(partial, output, renameError);
    if (renameError) {
        error = "cannot publish downloaded file: " + renameError.message();
        std::error_code ignored;
        std::filesystem::remove(partial, ignored);
        return false;
    }
    return true;
}

bool MiniDriverStorageAdapter::listCatalog(const QString& path,
                                           CatalogSnapshot& out,
                                           std::string& error) const {
    out = {};
    const QString normalizedPath = path.trimmed().isEmpty() ? QStringLiteral("/") : path.trimmed();
    if (!normalizedPath.startsWith(QLatin1Char('/'))) {
        error = "catalog path must start with '/'";
        return false;
    }
    std::map<std::string, std::string> headers;
    if (!config_.clusterInternalToken.empty()) {
        headers.emplace("X-Cluster-Internal-Token", config_.clusterInternalToken);
    }
    if (!config_.servicePrincipal.empty()) {
        headers.emplace("X-Service-Principal", config_.servicePrincipal);
    }
    miniKV::client::HttpResponse response;
    const std::string requestPath = "/api/v2/catalog?path=" + queryEscape(normalizedPath);
    if (!miniKV::client::httpRequest(config_.gateway, "GET", requestPath, headers, "",
                                     config_.gatewayTimeoutMs, response, error)) {
        return false;
    }
    if (response.status != 200) {
        const std::string message = miniKV::util::jsonString(response.body, "error");
        error = "catalog HTTP " + std::to_string(response.status) +
                (message.empty() ? std::string{} : ": " + message);
        return false;
    }
    out.path = QString::fromStdString(miniKV::util::jsonString(response.body, "path"));
    if (out.path.isEmpty()) {
        error = "catalog response has no path";
        return false;
    }
    for (const std::string& item : miniKV::util::jsonObjectArray(response.body, "directories")) {
        CatalogDirectory directory;
        directory.path = QString::fromStdString(miniKV::util::jsonString(item, "path"));
        directory.createdAt = static_cast<qint64>(miniKV::util::jsonUint(item, "createdAt"));
        if (!directory.path.isEmpty()) out.directories.push_back(std::move(directory));
    }
    for (const std::string& item : miniKV::util::jsonObjectArray(response.body, "files")) {
        CatalogFile file;
        file.objectId = QString::fromStdString(miniKV::util::jsonString(item, "objectId"));
        file.objectVersion = miniKV::util::jsonUint(item, "objectVersion");
        file.metadataVersion = miniKV::util::jsonUint(item, "metadataVersion");
        file.name = QString::fromStdString(miniKV::util::jsonString(item, "name"));
        file.parentPath = QString::fromStdString(miniKV::util::jsonString(item, "parentPath"));
        file.state = QString::fromStdString(miniKV::util::jsonString(item, "state"));
        file.fileSize = miniKV::util::jsonUint(item, "fileSize");
        file.createdAt = static_cast<qint64>(miniKV::util::jsonUint(item, "createdAt"));
        if (!file.objectId.isEmpty() && file.objectVersion > 0) out.files.push_back(std::move(file));
    }
    return true;
}

}  // namespace miniKV::qtclient
