#pragma once

#include <cstdint>
#include <string>

namespace miniKV {
namespace media {

// A small, durable outbox record. File bytes never enter Redis: consumers use
// objectId to read a committed object through MiniDrive's manifest contract.
struct AiIndexEvent {
    std::string eventId;
    std::string objectId;
    std::string objectKey;
    std::string fileHash;
    uint64_t fileSize = 0;
    uint64_t objectVersion = 1;
    uint64_t metadataVersion = 1;
    int64_t occurredAt = 0;
    int64_t publishedAt = 0;
};

std::string serializeAiIndexEvent(const AiIndexEvent& event);
bool parseAiIndexEvent(const std::string& value, AiIndexEvent& out);

}  // namespace media
}  // namespace miniKV
