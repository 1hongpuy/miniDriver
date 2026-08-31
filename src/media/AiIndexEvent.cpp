#include "media/AiIndexEvent.hpp"

#include "utils/Util.hpp"

#include <vector>

namespace miniKV {
namespace media {

std::string serializeAiIndexEvent(const AiIndexEvent& event)
{
    using miniKV::util::hexEncode;
    return hexEncode(event.eventId) + "|" + hexEncode(event.objectId) + "|" +
           hexEncode(event.objectKey) + "|" + hexEncode(event.fileHash) + "|" +
           std::to_string(event.fileSize) + "|" + std::to_string(event.objectVersion) + "|" +
           std::to_string(event.metadataVersion) + "|" + std::to_string(event.occurredAt) + "|" +
           std::to_string(event.publishedAt);
}

bool parseAiIndexEvent(const std::string& value, AiIndexEvent& out)
{
    using miniKV::util::hexDecode;
    using miniKV::util::split;
    const std::vector<std::string> fields = split(value, '|');
    if(fields.size() != 9) return false;
    try {
        AiIndexEvent parsed;
        if(!hexDecode(fields[0], parsed.eventId) || !hexDecode(fields[1], parsed.objectId) ||
           !hexDecode(fields[2], parsed.objectKey) || !hexDecode(fields[3], parsed.fileHash)) {
            return false;
        }
        parsed.fileSize = std::stoull(fields[4]);
        parsed.objectVersion = std::stoull(fields[5]);
        parsed.metadataVersion = std::stoull(fields[6]);
        parsed.occurredAt = std::stoll(fields[7]);
        parsed.publishedAt = std::stoll(fields[8]);
        if(parsed.eventId.empty() || parsed.objectId.empty() || parsed.fileHash.empty() ||
           parsed.fileSize == 0 || parsed.objectVersion == 0 || parsed.metadataVersion == 0 ||
           parsed.occurredAt <= 0) {
            return false;
        }
        out = std::move(parsed);
        return true;
    } catch(...) {
        return false;
    }
}

}  // namespace media
}  // namespace miniKV
