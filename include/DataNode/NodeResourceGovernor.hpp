#pragma once

#include <algorithm>
#include <cstdint>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>

namespace miniKV::datanode {

class NodeResourceGovernor {
public:
    struct Config {
        uint32_t maxActiveUploads = 2;
        uint32_t maxActiveDownloads = 8;
        uint32_t maxUploadsPerClient = 2;
        uint32_t maxDownloadsPerClient = 4;
    };

    struct Snapshot {
        uint32_t activeUploads = 0;
        uint32_t activeDownloads = 0;
        uint64_t uploadAcquireAttempts = 0;
        uint64_t uploadAcquireSuccesses = 0;
        uint64_t uploadRejects = 0;
        uint64_t uploadRejectsGlobal = 0;
        uint64_t uploadRejectsPerClient = 0;
        uint64_t uploadReleases = 0;
    };

private:
    enum class ResourceKind {
        Upload,
        Download
    };

public:
    class Lease {
    public:
        Lease() = default;
        Lease(const Lease&) = delete;
        Lease& operator=(const Lease&) = delete;

        Lease(Lease&& other) noexcept
            : owner_(other.owner_), kind_(other.kind_), clientId_(std::move(other.clientId_))
        {
            other.owner_ = nullptr;
        }

        Lease& operator=(Lease&& other) noexcept
        {
            if(this == &other) return *this;
            reset();
            owner_ = other.owner_;
            kind_ = other.kind_;
            clientId_ = std::move(other.clientId_);
            other.owner_ = nullptr;
            return *this;
        }

        ~Lease() { reset(); }

        void reset()
        {
            if(owner_ == nullptr) return;
            owner_->release(kind_, clientId_);
            owner_ = nullptr;
            clientId_.clear();
        }

        explicit operator bool() const { return owner_ != nullptr; }

    private:
        friend class NodeResourceGovernor;

        Lease(NodeResourceGovernor* owner, ResourceKind kind, std::string clientId)
            : owner_(owner), kind_(kind), clientId_(std::move(clientId)) {}

        NodeResourceGovernor* owner_ = nullptr;
        ResourceKind kind_ = ResourceKind::Upload;
        std::string clientId_;
    };

    using UploadLease = Lease;
    using DownloadLease = Lease;

    explicit NodeResourceGovernor(Config config)
        : config_(normalize(config)) {}

    std::optional<UploadLease> tryAcquireUpload(std::string clientId)
    {
        return tryAcquireUpload(std::move(clientId), nullptr);
    }

    std::optional<UploadLease> tryAcquireUpload(std::string clientId, std::string* rejectReason)
    {
        return tryAcquire(ResourceKind::Upload, normalizeClientId(std::move(clientId)), rejectReason);
    }

    std::optional<DownloadLease> tryAcquireDownload(std::string clientId)
    {
        return tryAcquire(ResourceKind::Download, normalizeClientId(std::move(clientId)));
    }

    Snapshot snapshot() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        Snapshot result;
        result.activeUploads = activeUploads_;
        result.activeDownloads = activeDownloads_;
        result.uploadAcquireAttempts = uploadAcquireAttempts_;
        result.uploadAcquireSuccesses = uploadAcquireSuccesses_;
        result.uploadRejects = uploadRejects_;
        result.uploadRejectsGlobal = uploadRejectsGlobal_;
        result.uploadRejectsPerClient = uploadRejectsPerClient_;
        result.uploadReleases = uploadReleases_;
        return result;
    }

    const Config& config() const { return config_; }

private:
    static Config normalize(Config config)
    {
        config.maxActiveUploads = std::max<uint32_t>(1, config.maxActiveUploads);
        config.maxActiveDownloads = std::max<uint32_t>(1, config.maxActiveDownloads);
        config.maxUploadsPerClient = std::max<uint32_t>(1, config.maxUploadsPerClient);
        config.maxDownloadsPerClient = std::max<uint32_t>(1, config.maxDownloadsPerClient);
        config.maxUploadsPerClient = std::min(config.maxUploadsPerClient,
                                              config.maxActiveUploads);
        config.maxDownloadsPerClient = std::min(config.maxDownloadsPerClient,
                                                config.maxActiveDownloads);
        return config;
    }

    static std::string normalizeClientId(std::string clientId)
    {
        constexpr size_t kMaxClientIdBytes = 128;
        if(clientId.empty()) return "anonymous";
        if(clientId.size() > kMaxClientIdBytes) clientId.resize(kMaxClientIdBytes);
        return clientId;
    }

    std::optional<Lease> tryAcquire(ResourceKind kind, std::string clientId,
                                    std::string* rejectReason = nullptr)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if(kind == ResourceKind::Upload) ++uploadAcquireAttempts_;
        uint32_t& active = kind == ResourceKind::Upload ? activeUploads_ : activeDownloads_;
        const uint32_t globalLimit = kind == ResourceKind::Upload
            ? config_.maxActiveUploads : config_.maxActiveDownloads;
        const uint32_t clientLimit = kind == ResourceKind::Upload
            ? config_.maxUploadsPerClient : config_.maxDownloadsPerClient;
        auto& clients = kind == ResourceKind::Upload ? uploadClients_ : downloadClients_;
        uint32_t& clientActive = clients[clientId];
        const bool globalRejected = active >= globalLimit;
        const bool clientRejected = clientActive >= clientLimit;
        if(globalRejected || clientRejected) {
            if(kind == ResourceKind::Upload) {
                ++uploadRejects_;
                if(globalRejected) ++uploadRejectsGlobal_;
                if(clientRejected) ++uploadRejectsPerClient_;
                if(rejectReason) *rejectReason = globalRejected ? "global_limit" : "per_client_limit";
            }
            if(clientActive == 0) clients.erase(clientId);
            return std::nullopt;
        }
        ++active;
        ++clientActive;
        if(kind == ResourceKind::Upload) ++uploadAcquireSuccesses_;
        return Lease(this, kind, std::move(clientId));
    }

    void release(ResourceKind kind, const std::string& clientId)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        uint32_t& active = kind == ResourceKind::Upload ? activeUploads_ : activeDownloads_;
        auto& clients = kind == ResourceKind::Upload ? uploadClients_ : downloadClients_;
        const auto it = clients.find(clientId);
        if(it != clients.end()) {
            if(it->second > 1) --it->second;
            else clients.erase(it);
        }
        if(active > 0) --active;
        if(kind == ResourceKind::Upload) ++uploadReleases_;
    }

    const Config config_;
    mutable std::mutex mutex_;
    uint32_t activeUploads_ = 0;
    uint32_t activeDownloads_ = 0;
    std::unordered_map<std::string, uint32_t> uploadClients_;
    std::unordered_map<std::string, uint32_t> downloadClients_;
    uint64_t uploadAcquireAttempts_ = 0;
    uint64_t uploadAcquireSuccesses_ = 0;
    uint64_t uploadRejects_ = 0;
    uint64_t uploadRejectsGlobal_ = 0;
    uint64_t uploadRejectsPerClient_ = 0;
    uint64_t uploadReleases_ = 0;
};

}  // namespace miniKV::datanode
