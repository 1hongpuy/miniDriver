#include "control/ObjectReadTypes.hpp"
#include "utils/Util.hpp"
#include "TestCheck.hpp"

#include <iostream>
#include <string>

int main()
{
    const std::string secret = "read-capability-test-secret";
    miniKV::util::ReadCapability capability;
    capability.capabilityId = "read-cap-1";
    capability.principalId = "service:lenscompute";
    capability.objectId = "object-123";
    capability.objectVersion = 7;
    capability.storageIdentity = "chunk-opaque-9";
    capability.expiresAt = miniKV::util::unixSeconds() + 60;

    const std::string token = miniKV::util::issueReadCapability(capability, secret);
    MINIKV_CHECK(!token.empty());
    miniKV::util::ReadCapability decoded;
    MINIKV_CHECK(miniKV::util::verifyReadCapability(token, secret, decoded));
    MINIKV_CHECK(decoded.capabilityId == capability.capabilityId);
    MINIKV_CHECK(decoded.principalId == capability.principalId);
    MINIKV_CHECK(decoded.scope == "object:read");
    MINIKV_CHECK(decoded.objectId == capability.objectId);
    MINIKV_CHECK(decoded.objectVersion == capability.objectVersion);
    MINIKV_CHECK(decoded.storageIdentity == capability.storageIdentity);

    std::string tampered = token;
    tampered.back() = tampered.back() == '0' ? '1' : '0';
    MINIKV_CHECK(!miniKV::util::verifyReadCapability(tampered, secret, decoded));
    MINIKV_CHECK(!miniKV::util::verifyReadCapability(token, "wrong-secret", decoded));

    auto expired = capability;
    expired.capabilityId = "expired-cap";
    expired.expiresAt = miniKV::util::unixSeconds() - 1;
    const std::string expiredToken = miniKV::util::issueReadCapability(expired, secret);
    MINIKV_CHECK(!expiredToken.empty());
    MINIKV_CHECK(!miniKV::util::verifyReadCapability(expiredToken, secret, decoded));

    auto invalid = capability;
    invalid.scope = "object:write";
    MINIKV_CHECK(miniKV::util::issueReadCapability(invalid, secret).empty());
    invalid = capability;
    invalid.storageIdentity.clear();
    MINIKV_CHECK(miniKV::util::issueReadCapability(invalid, secret).empty());

    miniKV::control::ObjectReadDescriptor descriptor;
    descriptor.objectId = capability.objectId;
    descriptor.objectVersion = capability.objectVersion;
    MINIKV_CHECK(descriptor.objectId == "object-123");
    MINIKV_CHECK(descriptor.objectVersion == 7);

    std::cout << "PASS: signed per-Chunk read capability contract\n";
    return 0;
}
