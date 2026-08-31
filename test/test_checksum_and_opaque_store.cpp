#include "DataNode/ChecksumProvider.hpp"
#include "DataNode/FastDataStore.hpp"
#include "TestCheck.hpp"
#include "utils/Util.hpp"

#include <filesystem>
#include <iostream>

int main()
{
    using namespace miniKV::datanode;
    MINIKV_CHECK(crc32cHex("123456789", 9) == "e3069283");
    MINIKV_CHECK(validateChecksumDigest(ChunkChecksumType::kSha256,
                                       std::string(64, 'a')));
    MINIKV_CHECK(validateChecksumDigest(ChunkChecksumType::kCrc32c,
                                       "e3069283"));
    MINIKV_CHECK(!validateChecksumDigest(ChunkChecksumType::kCrc32c,
                                        "E3069283"));
    MINIKV_CHECK(!isSupportedChecksumType(ChunkChecksumType::kBlake3));

    const std::filesystem::path directory =
        std::filesystem::temp_directory_path() / "minikv_checksum_opaque_store_test";
    std::error_code error;
    std::filesystem::remove_all(directory, error);

    const std::string legacyBytes = "legacy-cas-bytes";
    const std::string legacyHash = miniKV::util::sha256Hex(
        legacyBytes.data(), legacyBytes.size());
    const std::string opaqueBytes = "123456789";
    FastDataStore::PutOptions opaque;
    opaque.storageKey = "chk-object-1-v1-0";
    opaque.identityScheme = ChunkIdentityScheme::kOpaqueChunkId;
    opaque.checksum.type = ChunkChecksumType::kCrc32c;
    opaque.checksum.wholeDigest = "e3069283";

    {
        FastDataStore store(directory.string());
        MINIKV_CHECK(store.open());
        bool alreadyExists = false;
        MINIKV_CHECK(store.put(legacyHash, legacyBytes, alreadyExists));
        MINIKV_CHECK(!alreadyExists);
        MINIKV_CHECK(store.put(opaque, opaqueBytes, alreadyExists));
        MINIKV_CHECK(!alreadyExists);

        std::string read;
        MINIKV_CHECK(store.get(legacyHash, read) && read == legacyBytes);
        MINIKV_CHECK(store.get(opaque, read) && read == opaqueBytes);
        FileRegion region;
        MINIKV_CHECK(store.getRegion(legacyHash, region));
        MINIKV_CHECK(store.getRegion(opaque.storageKey, region));

        FastDataStore::PutOptions wrong = opaque;
        wrong.checksum.wholeDigest = "00000000";
        MINIKV_CHECK(!store.get(wrong, read));
        MINIKV_CHECK(!store.put(wrong, opaqueBytes, alreadyExists));
    }

    // Reopen exercises orphan/free-space reconstruction across both e: and o:
    // physical index namespaces.
    {
        FastDataStore reopened(directory.string());
        MINIKV_CHECK(reopened.open());
        std::string read;
        MINIKV_CHECK(reopened.get(legacyHash, read) && read == legacyBytes);
        MINIKV_CHECK(reopened.get(opaque, read) && read == opaqueBytes);
        bool removed = false;
        MINIKV_CHECK(reopened.remove(opaque.storageKey, removed) && removed);
        MINIKV_CHECK(!reopened.get(opaque, read));
        MINIKV_CHECK(reopened.get(legacyHash, read) && read == legacyBytes);
    }

    std::filesystem::remove_all(directory, error);
    std::cout << "PASS: SHA-256/CRC32C providers and CAS/opaque physical keys\n";
    return 0;
}
