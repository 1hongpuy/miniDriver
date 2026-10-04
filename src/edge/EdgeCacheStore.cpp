#include "edge/EdgeCacheStore.hpp"

#include "utils/Util.hpp"

#include <openssl/evp.h>

#include <array>
#include <cerrno>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <list>
#include <limits>
#include <mutex>
#include <sstream>
#include <unordered_map>
#include <unordered_set>
#include <fcntl.h>
#include <sys/file.h>
#include <unistd.h>

namespace miniKV::edge {
namespace {

uint32_t crc32cUpdate(uint32_t crc, const unsigned char* bytes, size_t size) {
    static std::array<uint32_t, 256> table = [] {
        std::array<uint32_t, 256> output{};
        for (uint32_t index = 0; index < output.size(); ++index) {
            uint32_t value = index;
            for (int bit = 0; bit < 8; ++bit) {
                value = (value >> 1U) ^ ((value & 1U) ? 0x82f63b78U : 0U);
            }
            output[index] = value;
        }
        return output;
    }();
    for (size_t index = 0; index < size; ++index) {
        crc = table[(crc ^ bytes[index]) & 0xffU] ^ (crc >> 8U);
    }
    return crc;
}

std::string hex32(uint32_t value) {
    static constexpr char digits[] = "0123456789abcdef";
    std::string out(8, '0');
    for (int shift = 28, index = 0; shift >= 0; shift -= 4, ++index) {
        out[static_cast<size_t>(index)] = digits[(value >> shift) & 0xfU];
    }
    return out;
}

bool verifyFile(const std::filesystem::path& path, const EdgeCacheKey& key, std::string& error) {
    std::error_code sizeError;
    if (std::filesystem::file_size(path, sizeError) != key.size || sizeError) {
        error = "cache file length mismatch";
        return false;
    }
    std::ifstream input(path, std::ios::binary);
    if (!input) {
        error = "cannot read cache file";
        return false;
    }
    std::array<char, 256 * 1024> buffer{};
    if (key.checksumType == "crc32c") {
        uint32_t crc = 0xffffffffU;
        while (input) {
            input.read(buffer.data(), static_cast<std::streamsize>(buffer.size()));
            const std::streamsize count = input.gcount();
            if (count > 0) crc = crc32cUpdate(crc, reinterpret_cast<const unsigned char*>(buffer.data()),
                                              static_cast<size_t>(count));
        }
        if (!input.eof()) { error = "cannot read cache checksum"; return false; }
        if (hex32(crc ^ 0xffffffffU) != key.checksumDigest) {
            error = "cache CRC32C mismatch";
            return false;
        }
        return true;
    }
    if (key.checksumType == "sha256") {
        EVP_MD_CTX* context = EVP_MD_CTX_new();
        if (!context || EVP_DigestInit_ex(context, EVP_sha256(), nullptr) != 1) {
            if (context) EVP_MD_CTX_free(context);
            error = "cannot initialize cache SHA-256";
            return false;
        }
        while (input) {
            input.read(buffer.data(), static_cast<std::streamsize>(buffer.size()));
            const std::streamsize count = input.gcount();
            if (count > 0 && EVP_DigestUpdate(context, buffer.data(), static_cast<size_t>(count)) != 1) {
                EVP_MD_CTX_free(context); error = "cannot update cache SHA-256"; return false;
            }
        }
        if (!input.eof()) { EVP_MD_CTX_free(context); error = "cannot read cache checksum"; return false; }
        unsigned char digest[EVP_MAX_MD_SIZE];
        unsigned int digestSize = 0;
        if (EVP_DigestFinal_ex(context, digest, &digestSize) != 1) {
            EVP_MD_CTX_free(context); error = "cannot finish cache SHA-256"; return false;
        }
        EVP_MD_CTX_free(context);
        static constexpr char digits[] = "0123456789abcdef";
        std::string actual;
        actual.reserve(digestSize * 2);
        for (unsigned int index = 0; index < digestSize; ++index) {
            actual += digits[(digest[index] >> 4) & 0xfU];
            actual += digits[digest[index] & 0xfU];
        }
        if (actual != key.checksumDigest) { error = "cache SHA-256 mismatch"; return false; }
        return true;
    }
    error = "unsupported cache checksum type " + key.checksumType;
    return false;
}

}  // namespace

struct CacheLease::State {
    struct Entry {
        EdgeCacheKey key;
        std::filesystem::path body;
        std::filesystem::path meta;
        uint64_t pins = 0;
    };

    explicit State(EdgeCacheStore::Config initial) : config(std::move(initial)) {}
    ~State() {
        if (lockFd >= 0) ::close(lockFd);
    }

    EdgeCacheStore::Config config;
    mutable std::mutex mutex;
    std::unordered_map<std::string, Entry> entries;
    std::unordered_set<std::string> filling;
    uint64_t readyBytes = 0;
    uint64_t reservedBytes = 0;
    uint64_t hits = 0;
    uint64_t misses = 0;
    uint64_t rejectedReservations = 0;
    uint64_t recoveredEntries = 0;
    int lockFd = -1;

    void releaseLease(const std::string& id) noexcept {
        std::lock_guard<std::mutex> lock(mutex);
        const auto found = entries.find(id);
        if (found != entries.end() && found->second.pins > 0) --found->second.pins;
    }
    void releaseReservation(const std::string& id, uint64_t bytes) noexcept {
        std::lock_guard<std::mutex> lock(mutex);
        reservedBytes = bytes > reservedBytes ? 0 : reservedBytes - bytes;
        filling.erase(id);
    }
};

EdgeCacheKey EdgeCacheKey::fromChunk(const client::ObjectRef& object,
                                     const client::ChunkReadPlan& chunk) {
    const std::string canonical = object.objectId + "\n" + std::to_string(object.objectVersion) + "\n" +
        chunk.storageIdentity + "\n" + std::to_string(chunk.size) + "\n" +
        chunk.checksumType + "\n" + chunk.checksumDigest;
    EdgeCacheKey key;
    key.id = miniKV::util::sha256Hex(canonical.data(), canonical.size());
    key.size = chunk.size;
    key.checksumType = chunk.checksumType;
    key.checksumDigest = chunk.checksumDigest;
    return key;
}

CacheLease::CacheLease(std::shared_ptr<State> state, std::string id,
                       std::filesystem::path path, uint64_t size)
    : state_(std::move(state)), id_(std::move(id)), path_(std::move(path)), size_(size) {}
CacheLease::CacheLease(CacheLease&& other) noexcept { *this = std::move(other); }
CacheLease& CacheLease::operator=(CacheLease&& other) noexcept {
    if (this != &other) {
        reset();
        state_ = std::move(other.state_);
        id_ = std::move(other.id_);
        path_ = std::move(other.path_);
        size_ = other.size_;
        other.size_ = 0;
    }
    return *this;
}
CacheLease::~CacheLease() { reset(); }
void CacheLease::reset() noexcept {
    if (state_) state_->releaseLease(id_);
    state_.reset(); id_.clear(); path_.clear(); size_ = 0;
}

CacheReservation::CacheReservation(std::shared_ptr<CacheLease::State> state, std::string id, uint64_t bytes)
    : state_(std::move(state)), id_(std::move(id)), bytes_(bytes) {}
CacheReservation::CacheReservation(CacheReservation&& other) noexcept { *this = std::move(other); }
CacheReservation& CacheReservation::operator=(CacheReservation&& other) noexcept {
    if (this != &other) {
        reset();
        state_ = std::move(other.state_);
        id_ = std::move(other.id_);
        bytes_ = other.bytes_;
        other.bytes_ = 0;
    }
    return *this;
}
CacheReservation::~CacheReservation() { reset(); }
void CacheReservation::reset() noexcept {
    if (state_) state_->releaseReservation(id_, bytes_);
    state_.reset(); id_.clear(); bytes_ = 0;
}

EdgeCacheStore::EdgeCacheStore(Config config)
    : state_(std::make_shared<CacheLease::State>(std::move(config))) {}

bool EdgeCacheStore::initialize(std::string& error) {
    std::error_code filesystemError;
    std::filesystem::create_directories(state_->config.root, filesystemError);
    if (filesystemError) { error = "cannot create edge cache root: " + filesystemError.message(); return false; }
    const auto lockPath = state_->config.root / ".edge-cache.lock";
    state_->lockFd = ::open(lockPath.c_str(), O_CREAT | O_RDWR | O_CLOEXEC, 0600);
    if (state_->lockFd < 0) {
        error = std::string("cannot open edge cache instance lock: ") + std::strerror(errno);
        return false;
    }
    if (::flock(state_->lockFd, LOCK_EX | LOCK_NB) != 0) {
        error = "another EdgeCache process already owns this cache root";
        ::close(state_->lockFd);
        state_->lockFd = -1;
        return false;
    }
    for (const auto& entry : std::filesystem::directory_iterator(state_->config.root, filesystemError)) {
        if (filesystemError) { error = "cannot scan edge cache root: " + filesystemError.message(); return false; }
        const auto name = entry.path().filename().string();
        if (name.size() > 4 && name.substr(name.size() - 4) == ".tmp") {
            std::error_code ignored;
            std::filesystem::remove(entry.path(), ignored);
        }
    }
    for (const auto& entry : std::filesystem::directory_iterator(state_->config.root, filesystemError)) {
        if (filesystemError) { error = "cannot recover edge cache root: " + filesystemError.message(); return false; }
        if (!entry.is_regular_file()) continue;
        const auto name = entry.path().filename().string();
        constexpr std::string_view suffix = ".meta";
        if (name.size() <= suffix.size() || name.compare(name.size() - suffix.size(), suffix.size(), suffix) != 0) continue;
        const std::string id = name.substr(0, name.size() - suffix.size());
        const auto body = state_->config.root / (id + ".ready");
        std::ifstream meta(entry.path());
        std::string sizeLine, type, digest, extra;
        uint64_t size = 0;
        bool valid = static_cast<bool>(meta) && static_cast<bool>(std::getline(meta, sizeLine)) &&
                     static_cast<bool>(std::getline(meta, type)) && static_cast<bool>(std::getline(meta, digest)) &&
                     !std::getline(meta, extra);
        try {
            if (valid && !sizeLine.empty()) size = std::stoull(sizeLine);
            else valid = false;
        } catch (...) { valid = false; }
        EdgeCacheKey key{id, size, type, digest};
        std::string verifyError;
        if (valid) valid = key.size != 0 && verifyFile(body, key, verifyError);
        if (valid && key.size <= state_->config.capacityBytes -
                                 std::min(state_->config.capacityBytes, state_->readyBytes)) {
            state_->entries.emplace(id, CacheLease::State::Entry{key, body, entry.path(), 0});
            state_->readyBytes += key.size;
            ++state_->recoveredEntries;
        } else {
            std::error_code ignored;
            std::filesystem::remove(body, ignored);
            std::filesystem::remove(entry.path(), ignored);
        }
    }
    return true;
}

bool EdgeCacheStore::acquire(const EdgeCacheKey& key, CacheLease& out, std::string& error) {
    out.reset();
    std::filesystem::path body;
    {
        std::lock_guard<std::mutex> lock(state_->mutex);
        const auto found = state_->entries.find(key.id);
        if (found == state_->entries.end() || found->second.key.size != key.size ||
            found->second.key.checksumType != key.checksumType ||
            found->second.key.checksumDigest != key.checksumDigest) {
            ++state_->misses;
            return false;
        }
        ++found->second.pins;
        body = found->second.body;
    }
    if (state_->config.verifyHitChecksum && !verifyFile(body, key, error)) {
        std::lock_guard<std::mutex> lock(state_->mutex);
        const auto found = state_->entries.find(key.id);
        if (found != state_->entries.end()) {
            if (found->second.pins > 0) --found->second.pins;
            if (found->second.pins == 0) {
                state_->readyBytes -= found->second.key.size;
                std::error_code ignored;
                std::filesystem::remove(found->second.body, ignored);
                std::filesystem::remove(found->second.meta, ignored);
                state_->entries.erase(found);
            }
        }
        ++state_->misses;
        return false;
    }
    {
        std::lock_guard<std::mutex> lock(state_->mutex);
        ++state_->hits;
    }
    out = CacheLease(state_, key.id, std::move(body), key.size);
    return true;
}

bool EdgeCacheStore::reserve(const EdgeCacheKey& key, CacheReservation& out, std::string& error) {
    out.reset();
    std::lock_guard<std::mutex> lock(state_->mutex);
    if (key.size == 0 || state_->config.capacityBytes == 0 || key.size > state_->config.capacityBytes -
        std::min(state_->config.capacityBytes, state_->readyBytes + state_->reservedBytes)) {
        ++state_->rejectedReservations;
        error = "edge cache capacity unavailable";
        return false;
    }
    if (state_->entries.find(key.id) != state_->entries.end() || !state_->filling.insert(key.id).second) {
        ++state_->rejectedReservations;
        error = "edge cache fill already active";
        return false;
    }
    state_->reservedBytes += key.size;
    out = CacheReservation(state_, key.id, key.size);
    return true;
}

bool EdgeCacheStore::publish(const EdgeCacheKey& key, const std::string& bytes,
                             CacheReservation& reservation, CacheLease& out, std::string& error) {
    out.reset();
    if (!reservation.valid() || reservation.bytes() != key.size || bytes.size() != key.size) {
        error = "invalid edge cache publication reservation";
        return false;
    }
    if (key.size > static_cast<uint64_t>(std::numeric_limits<size_t>::max())) {
        error = "edge cache chunk exceeds platform file API size";
        return false;
    }
    const auto body = state_->config.root / (key.id + ".ready");
    const auto meta = state_->config.root / (key.id + ".meta");
    const auto unique = key.id + "." + std::to_string(::getpid()) + "." +
        std::to_string(reinterpret_cast<uintptr_t>(&reservation));
    const auto temporaryBody = state_->config.root / (unique + ".body.tmp");
    const auto temporaryMeta = state_->config.root / (unique + ".meta.tmp");
    {
        std::ofstream output(temporaryBody, std::ios::binary | std::ios::trunc);
        if (!output) { error = "cannot create edge cache temporary body"; return false; }
        output.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
        output.close();
        if (!output) { std::error_code ignored; std::filesystem::remove(temporaryBody, ignored); error = "cannot write edge cache body"; return false; }
    }
    {
        std::ofstream output(temporaryMeta, std::ios::trunc);
        if (!output) { std::error_code ignored; std::filesystem::remove(temporaryBody, ignored); error = "cannot create edge cache metadata"; return false; }
        output << key.size << '\n' << key.checksumType << '\n' << key.checksumDigest << '\n';
        output.close();
        if (!output) { std::error_code ignored; std::filesystem::remove(temporaryBody, ignored); std::filesystem::remove(temporaryMeta, ignored); error = "cannot write edge cache metadata"; return false; }
    }
    std::error_code renameError;
    std::filesystem::rename(temporaryBody, body, renameError);
    if (renameError) { std::filesystem::remove(temporaryBody, renameError); std::filesystem::remove(temporaryMeta, renameError); error = "cannot publish edge cache body: " + renameError.message(); return false; }
    std::filesystem::rename(temporaryMeta, meta, renameError);
    if (renameError) { std::error_code ignored; std::filesystem::remove(body, ignored); std::filesystem::remove(temporaryMeta, ignored); error = "cannot publish edge cache metadata: " + renameError.message(); return false; }
    {
        std::lock_guard<std::mutex> lock(state_->mutex);
        auto [found, inserted] = state_->entries.emplace(key.id, CacheLease::State::Entry{key, body, meta, 0});
        if (!inserted) {
            // A concurrent writer finished first. Its immutable READY file wins.
            std::error_code ignored;
            std::filesystem::remove(body, ignored);
            std::filesystem::remove(meta, ignored);
            if (found->second.pins == 0) {
                ++found->second.pins;
                out = CacheLease(state_, key.id, found->second.body, found->second.key.size);
            }
        } else {
            state_->reservedBytes -= reservation.bytes();
            state_->filling.erase(key.id);
            reservation = {};
            state_->readyBytes += key.size;
            found->second.pins = 1;
            out = CacheLease(state_, key.id, body, key.size);
            return true;
        }
    }
    reservation.reset();
    if (out.valid()) return true;
    error = "concurrent cache publication cannot acquire winner";
    return false;
}

EdgeCacheStore::Stats EdgeCacheStore::stats() const {
    std::lock_guard<std::mutex> lock(state_->mutex);
    Stats result;
    result.readyBytes = state_->readyBytes;
    result.reservedBytes = state_->reservedBytes;
    result.entries = state_->entries.size();
    for (const auto& item : state_->entries) if (item.second.pins != 0) ++result.pinnedEntries;
    result.hits = state_->hits;
    result.misses = state_->misses;
    result.rejectedReservations = state_->rejectedReservations;
    result.recoveredEntries = state_->recoveredEntries;
    return result;
}

}  // namespace miniKV::edge
