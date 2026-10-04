#include "metadata/SnapshotStore.hpp"

#include <openssl/sha.h>

#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <limits>
#include <sstream>
#include <sys/stat.h>
#include <unistd.h>

namespace miniKV::metadata {
namespace {

constexpr char kMagic[] = "MKVMETA2";
constexpr size_t kMagicSize = sizeof(kMagic) - 1;
constexpr uint64_t kMaxSnapshotBytes = 4ULL * 1024ULL * 1024ULL * 1024ULL;

void appendU32(std::string& out, uint32_t value)
{ for(int shift = 0; shift < 32; shift += 8) out.push_back(static_cast<char>(value >> shift)); }
void appendU64(std::string& out, uint64_t value)
{ for(int shift = 0; shift < 64; shift += 8) out.push_back(static_cast<char>(value >> shift)); }

bool readU32(const std::string& bytes, size_t& pos, uint32_t& value)
{
    if(bytes.size() - pos < 4) return false;
    value = 0; for(int shift = 0; shift < 32; shift += 8) value |= static_cast<uint32_t>(static_cast<uint8_t>(bytes[pos++])) << shift;
    return true;
}
bool readU64(const std::string& bytes, size_t& pos, uint64_t& value)
{
    if(bytes.size() - pos < 8) return false;
    value = 0; for(int shift = 0; shift < 64; shift += 8) value |= static_cast<uint64_t>(static_cast<uint8_t>(bytes[pos++])) << shift;
    return true;
}

std::string sha256(const std::string& bytes)
{
    unsigned char digest[SHA256_DIGEST_LENGTH];
    SHA256(reinterpret_cast<const unsigned char*>(bytes.data()), bytes.size(), digest);
    return std::string(reinterpret_cast<const char*>(digest), sizeof(digest));
}

void setError(std::string* output, const std::string& message)
{ if(output) *output = message; }

bool writeAll(int fd, const std::string& bytes)
{
    size_t offset = 0;
    while(offset < bytes.size()) {
        const ssize_t written = ::write(fd, bytes.data() + offset, bytes.size() - offset);
        if(written < 0 && errno == EINTR) continue;
        if(written <= 0) return false;
        offset += static_cast<size_t>(written);
    }
    return true;
}

} // namespace

SnapshotStore::SnapshotStore(std::string directory) : directory_(std::move(directory)) {}

bool SnapshotStore::publish(const MetadataSnapshot& snapshot, std::string* error) const
{
    if(snapshot.schemaVersion != kMetadataSchemaVersion || snapshot.bytes.size() > kMaxSnapshotBytes) {
        setError(error, "invalid snapshot schema or size"); return false;
    }
    std::error_code ec;
    std::filesystem::create_directories(directory_, ec);
    if(ec) { setError(error, "cannot create snapshot directory: " + ec.message()); return false; }

    std::string encoded(kMagic, kMagicSize);
    appendU32(encoded, snapshot.schemaVersion); appendU64(encoded, snapshot.lastAppliedIndex);
    appendU64(encoded, snapshot.lastAppliedTerm); appendU64(encoded, snapshot.bytes.size());
    encoded.append(sha256(snapshot.bytes)); encoded.append(snapshot.bytes);

    const std::string temporary = directory_ + "/snapshot.tmp";
    const std::string published = directory_ + "/snapshot.bin";
    const int fd = ::open(temporary.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0600);
    if(fd < 0) { setError(error, "cannot open snapshot temp file: " + std::string(std::strerror(errno))); return false; }
    bool ok = writeAll(fd, encoded);
    if(ok) ok = ::fsync(fd) == 0;
    const int closeResult = ::close(fd);
    if(closeResult != 0) ok = false;
    if(!ok) { setError(error, "cannot durably write snapshot temp file"); return false; }
    if(::rename(temporary.c_str(), published.c_str()) != 0) {
        setError(error, "cannot atomically publish snapshot: " + std::string(std::strerror(errno))); return false;
    }
    const int directoryFd = ::open(directory_.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if(directoryFd < 0) { setError(error, "cannot open snapshot parent directory"); return false; }
    ok = ::fsync(directoryFd) == 0;
    ::close(directoryFd);
    if(!ok) { setError(error, "cannot fsync snapshot parent directory"); return false; }
    return true;
}

std::optional<MetadataSnapshot> SnapshotStore::load(std::string* error) const
{
    const std::string path = directory_ + "/snapshot.bin";
    const int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
    if(fd < 0) { setError(error, "cannot open snapshot: " + std::string(std::strerror(errno))); return std::nullopt; }
    struct stat info {};
    if(::fstat(fd, &info) != 0 || info.st_size < 0 || static_cast<uint64_t>(info.st_size) > kMaxSnapshotBytes + 128) {
        ::close(fd); setError(error, "invalid snapshot file size"); return std::nullopt;
    }
    std::string encoded(static_cast<size_t>(info.st_size), '\0');
    size_t offset = 0;
    while(offset < encoded.size()) {
        const ssize_t count = ::read(fd, encoded.data() + offset, encoded.size() - offset);
        if(count < 0 && errno == EINTR) continue;
        if(count <= 0) { ::close(fd); setError(error, "short snapshot read"); return std::nullopt; }
        offset += static_cast<size_t>(count);
    }
    ::close(fd);

    size_t pos = 0;
    if(encoded.size() < kMagicSize || encoded.compare(0, kMagicSize, kMagic) != 0) {
        setError(error, "snapshot magic mismatch"); return std::nullopt;
    }
    pos += kMagicSize;
    MetadataSnapshot snapshot; uint64_t payloadSize = 0;
    if(!readU32(encoded, pos, snapshot.schemaVersion) || !readU64(encoded, pos, snapshot.lastAppliedIndex)
       || !readU64(encoded, pos, snapshot.lastAppliedTerm) || !readU64(encoded, pos, payloadSize)
       || snapshot.schemaVersion != kMetadataSchemaVersion || payloadSize > kMaxSnapshotBytes
       || encoded.size() - pos < SHA256_DIGEST_LENGTH || encoded.size() - pos - SHA256_DIGEST_LENGTH != payloadSize) {
        setError(error, "snapshot header is invalid"); return std::nullopt;
    }
    const std::string expected = encoded.substr(pos, SHA256_DIGEST_LENGTH); pos += SHA256_DIGEST_LENGTH;
    snapshot.bytes = encoded.substr(pos);
    if(sha256(snapshot.bytes) != expected) { setError(error, "snapshot checksum mismatch"); return std::nullopt; }
    return snapshot;
}

} // namespace miniKV::metadata
