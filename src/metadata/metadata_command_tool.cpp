#include "metadata/MetadataCommand.hpp"

#include <fstream>
#include <iostream>

using namespace miniKV::metadata;

int main(int argc, char** argv)
{
    if(argc != 10 || std::string(argv[1]) != "register-node") {
        std::cerr << "usage: metadata_command_tool register-node OUTPUT COMMAND_ID NODE_ID BOOT_ID ADDRESS PORT CAPACITY CAPABILITIES_CSV\n";
        return 2;
    }
    MetadataCommand command; command.commandId = argv[3]; command.type = MetadataCommandType::kRegisterNode;
    command.actorType = "admin"; command.actorId = "admin";
    RegisterNodePayload payload; payload.nodeId = argv[4]; payload.bootId = argv[5]; payload.address = argv[6];
    try { payload.dataPort = static_cast<uint16_t>(std::stoul(argv[7])); payload.registeredCapacityBytes = std::stoull(argv[8]); }
    catch(...) { std::cerr << "invalid port or capacity\n"; return 2; }
    std::string capabilities = argv[9]; size_t start = 0;
    while(start <= capabilities.size()) {
        const size_t comma = capabilities.find(',', start); const std::string value = capabilities.substr(start, comma - start);
        if(!value.empty()) payload.capabilities.push_back(value); if(comma == std::string::npos) break; start = comma + 1;
    }
    command.payload = std::move(payload); const auto bytes = encodeMetadataCommand(command);
    if(!bytes) { std::cerr << "cannot encode command\n"; return 1; }
    std::ofstream output(argv[2], std::ios::binary | std::ios::trunc); output.write(bytes->data(), bytes->size());
    if(!output) { std::cerr << "cannot write command file\n"; return 1; }
    return 0;
}
