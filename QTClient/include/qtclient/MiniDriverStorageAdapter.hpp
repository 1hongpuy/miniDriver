#pragma once

#include "qtclient/TransferTypes.hpp"
#include "qtclient/CatalogTypes.hpp"

#include <functional>
#include <filesystem>
#include <string>

namespace miniKV::qtclient {

class MiniDriverStorageAdapter {
public:
    using ProgressCallback = std::function<void(uint64_t completed, uint64_t total)>;

    explicit MiniDriverStorageAdapter(miniKV::client::ClientConfig config);

    bool upload(const std::filesystem::path& input,
                const ProgressCallback& progress,
                miniKV::client::ObjectRef& object,
                std::string& error,
                const std::string& commandId = {});

    bool download(const miniKV::client::ObjectRef& object,
                  const std::filesystem::path& output,
                  const ProgressCallback& progress,
                  std::string& error);

    // Adapts the existing Gateway /api/v2/catalog endpoint. The endpoint is
    // currently a trusted-cluster catalog surface, not a complete per-user
    // ACL API; Qt must not present it as one.
    bool listCatalog(const QString& path, CatalogSnapshot& out, std::string& error) const;

    // Streaming SHA-256 used only by the Qt round-trip verification workflow.
    // Storage checksum verification remains owned by MiniDriverClient.
    static bool sha256File(const std::filesystem::path& path,
                           std::string& digest,
                           std::string& error);

private:
    miniKV::client::ClientConfig config_;
    miniKV::client::MiniDriverClient client_;
};

}  // namespace miniKV::qtclient
