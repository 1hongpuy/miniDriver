#include "DataNode/ChecksumProvider.hpp"

#include <array>
#include <openssl/evp.h>

namespace miniKV::datanode {
namespace {

char hexDigit(unsigned int value)
{
    return "0123456789abcdef"[value & 0x0fU];
}

bool isLowerHex(const std::string& value, size_t expectedLength)
{
    if(value.size() != expectedLength) return false;
    for(const unsigned char byte : value) {
        if(!((byte >= '0' && byte <= '9') || (byte >= 'a' && byte <= 'f'))) return false;
    }
    return true;
}

class Sha256Context final : public ChecksumContext {
public:
    Sha256Context()
    {
        context_ = EVP_MD_CTX_new();
        valid_ = context_ != nullptr &&
            EVP_DigestInit_ex(context_, EVP_sha256(), nullptr) == 1;
    }

    ~Sha256Context() override
    {
        if(context_ != nullptr) EVP_MD_CTX_free(context_);
    }

    bool update(const char* bytes, size_t size) override
    {
        return valid_ && bytes != nullptr && size > 0 &&
            EVP_DigestUpdate(context_, bytes, size) == 1;
    }

    bool finalizeHex(std::string& digest) override
    {
        if(!valid_ || finalized_) return false;
        unsigned char bytes[EVP_MAX_MD_SIZE];
        unsigned int size = 0;
        if(EVP_DigestFinal_ex(context_, bytes, &size) != 1) return false;
        digest.clear();
        digest.reserve(size * 2);
        for(unsigned int i = 0; i < size; ++i) {
            digest.push_back(hexDigit(bytes[i] >> 4));
            digest.push_back(hexDigit(bytes[i]));
        }
        finalized_ = true;
        return true;
    }

private:
    EVP_MD_CTX* context_ = nullptr;
    bool valid_ = false;
    bool finalized_ = false;
};

constexpr uint32_t kCrc32cPolynomial = 0x82f63b78U;

std::array<uint32_t, 256> makeCrc32cTable()
{
    std::array<uint32_t, 256> table{};
    for(uint32_t index = 0; index < table.size(); ++index) {
        uint32_t value = index;
        for(unsigned int bit = 0; bit < 8; ++bit) {
            value = (value >> 1) ^ ((value & 1U) ? kCrc32cPolynomial : 0U);
        }
        table[index] = value;
    }
    return table;
}

const std::array<uint32_t, 256>& crc32cTable()
{
    static const std::array<uint32_t, 256> table = makeCrc32cTable();
    return table;
}

class Crc32cContext final : public ChecksumContext {
public:
    bool update(const char* bytes, size_t size) override
    {
        if(finalized_ || bytes == nullptr || size == 0) return false;
        const auto& table = crc32cTable();
        for(size_t i = 0; i < size; ++i) {
            const uint8_t byte = static_cast<uint8_t>(bytes[i]);
            value_ = table[(value_ ^ byte) & 0xffU] ^ (value_ >> 8);
        }
        return true;
    }

    bool finalizeHex(std::string& digest) override
    {
        if(finalized_) return false;
        const uint32_t value = value_ ^ 0xffffffffU;
        digest.resize(8);
        for(size_t i = 0; i < 8; ++i) {
            const unsigned int shift = static_cast<unsigned int>((7 - i) * 4);
            digest[i] = hexDigit(value >> shift);
        }
        finalized_ = true;
        return true;
    }

private:
    uint32_t value_ = 0xffffffffU;
    bool finalized_ = false;
};

class Sha256Provider final : public ChecksumProvider {
public:
    ChunkChecksumType type() const override { return ChunkChecksumType::kSha256; }
    std::unique_ptr<ChecksumContext> create() const override
    {
        return std::make_unique<Sha256Context>();
    }
};

class Crc32cProvider final : public ChecksumProvider {
public:
    ChunkChecksumType type() const override { return ChunkChecksumType::kCrc32c; }
    std::unique_ptr<ChecksumContext> create() const override
    {
        return std::make_unique<Crc32cContext>();
    }
};

class UnsupportedProvider final : public ChecksumProvider {
public:
    ChunkChecksumType type() const override { return ChunkChecksumType::kBlake3; }
    std::unique_ptr<ChecksumContext> create() const override { return nullptr; }
};

}  // namespace

const ChecksumProvider& checksumProvider(ChunkChecksumType type)
{
    static const Sha256Provider sha256;
    static const Crc32cProvider crc32c;
    static const UnsupportedProvider unsupported;
    switch(type) {
    case ChunkChecksumType::kSha256: return sha256;
    case ChunkChecksumType::kCrc32c: return crc32c;
    case ChunkChecksumType::kBlake3: return unsupported;
    }
    return unsupported;
}

bool isSupportedChecksumType(ChunkChecksumType type)
{
    return type == ChunkChecksumType::kSha256 || type == ChunkChecksumType::kCrc32c;
}

bool validateChecksumDigest(ChunkChecksumType type, const std::string& digest)
{
    switch(type) {
    case ChunkChecksumType::kSha256: return isLowerHex(digest, 64);
    case ChunkChecksumType::kCrc32c: return isLowerHex(digest, 8);
    case ChunkChecksumType::kBlake3: return false;
    }
    return false;
}

std::string crc32cHex(const char* bytes, size_t size)
{
    auto context = checksumProvider(ChunkChecksumType::kCrc32c).create();
    std::string digest;
    if(context == nullptr || bytes == nullptr || size == 0 ||
       !context->update(bytes, size) || !context->finalizeHex(digest)) return {};
    return digest;
}

}  // namespace miniKV::datanode
