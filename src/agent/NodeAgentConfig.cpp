#include "agent/NodeAgentConfig.hpp"

#include <yaml-cpp/yaml.h>

#include <fstream>
#include <filesystem>
#include <set>
#include <sstream>
#include <stdexcept>

namespace miniKV {
namespace agent {

[[noreturn]] void invalid(const std::string& message)
{
    throw std::runtime_error("invalid node agent configuration: " + message);
}

template <typename T>
T required(const YAML::Node& parent, const char* key)
{
    const YAML::Node value = parent[key];
    if(!value || value.IsNull()) invalid(std::string("missing ") + key);
    try {
        return value.as<T>();
    } catch(const YAML::Exception&) {
        invalid(std::string("invalid ") + key);
    }
}

uint16_t port(const YAML::Node& parent, const char* key)
{
    const uint64_t value = required<uint64_t>(parent, key);
    if(value == 0 || value > UINT16_MAX) invalid(std::string("invalid ") + key);
    return static_cast<uint16_t>(value);
}

ServiceType parseType(const std::string& value)
{
    if(value == "gateway") return ServiceType::kGateway;
    if(value == "datanode") return ServiceType::kDataNode;
    invalid("unknown service type " + value);
}

RestartPolicy parseRestart(const YAML::Node& node)
{
    RestartPolicy policy;
    if(!node) return policy;
    if(node["policy"]) {
        const std::string value = node["policy"].as<std::string>();
        if(value == "on-failure") policy.onFailure = true;
        else if(value == "never") policy.onFailure = false;
        else invalid("invalid restart.policy");
    }
    if(node["initialBackoffSeconds"]) policy.initialBackoffSeconds = node["initialBackoffSeconds"].as<uint32_t>();
    if(node["maxBackoffSeconds"]) policy.maxBackoffSeconds = node["maxBackoffSeconds"].as<uint32_t>();
    if(policy.initialBackoffSeconds == 0 || policy.maxBackoffSeconds < policy.initialBackoffSeconds) {
        invalid("invalid restart backoff");
    }
    return policy;
}

bool isLogLevel(const std::string& level)
{
    return level == "trace" || level == "debug" || level == "info" ||
           level == "warn" || level == "error" || level == "critical" ||
           level == "off";
}

ServiceLoggingConfig parseLogging(const YAML::Node& node,
                                  const std::string& dataDir,
                                  const std::string& serviceId)
{
    ServiceLoggingConfig config;
    config.filePath = (std::filesystem::path(dataDir) / "logs" /
                       (serviceId + ".log")).string();
    if(!node) return config;
    if(!node.IsMap()) invalid("logging must be a map");

    if(node["file"]) config.filePath = node["file"].as<std::string>();
    if(node["level"]) config.level = node["level"].as<std::string>();
    if(node["queueSize"]) config.queueSize = node["queueSize"].as<uint32_t>();
    if(node["rotateBytes"]) config.rotateBytes = node["rotateBytes"].as<uint64_t>();
    if(node["rotateFiles"]) config.rotateFiles = node["rotateFiles"].as<uint32_t>();

    if(config.filePath.empty() || !isLogLevel(config.level) || config.queueSize == 0 ||
       config.rotateBytes == 0 || config.rotateFiles == 0) {
        invalid("invalid logging configuration");
    }
    return config;
}

NodeAgentConfig parse(const YAML::Node& root)
{
    if(!root || !root.IsMap()) invalid("root must be a map");
    const YAML::Node node = root["node"];
    const YAML::Node cluster = root["cluster"];
    const YAML::Node web = root["web"];
    const YAML::Node services = root["services"];
    if(!node || !node.IsMap()) invalid("missing node");
    if(!cluster || !cluster.IsMap()) invalid("missing cluster");
    if(!services || !services.IsSequence()) invalid("missing services");

    NodeAgentConfig config;
    config.nodeId = required<std::string>(node, "nodeId");
    config.advertiseAddress = required<std::string>(node, "advertiseAddress");
    config.secretFile = required<std::string>(cluster, "secretFile");
    if(config.nodeId.empty() || config.advertiseAddress.empty() || config.secretFile.empty()) invalid("empty required field");
    if(web) {
        if(!web.IsMap()) invalid("web must be a map");
        if(web["allowedOrigin"]) config.webAllowedOrigin = web["allowedOrigin"].as<std::string>();
    }
    
    bool hasEnabledDataNode = false;
    std::set<std::string> ids;
    std::set<uint16_t> ports;
    for(const YAML::Node& serviceNode : services) {
        if(!serviceNode.IsMap()) invalid("service must be a map");
        ManagedServiceConfig service;
        service.id = required<std::string>(serviceNode, "id");
        service.type = parseType(required<std::string>(serviceNode, "type"));
        if(serviceNode["enabled"]) service.enabled = serviceNode["enabled"].as<bool>();
        service.listenPort = port(serviceNode, "listenPort");
        service.dataDir = required<std::string>(serviceNode, "dataDir");
        service.restart = parseRestart(serviceNode["restart"]);
        if(serviceNode["logs"]) {
            const YAML::Node logs = serviceNode["logs"];
            if(!logs.IsMap()) invalid("logs must be a map");
            if(logs["stdout"]) service.logs.stdoutPath = logs["stdout"].as<std::string>();
            if(logs["stderr"]) service.logs.stderrPath = logs["stderr"].as<std::string>();
        }
        if(service.id.empty() || service.dataDir.empty()) invalid("empty service id or dataDir");
        service.logging = parseLogging(serviceNode["logging"], service.dataDir, service.id);
        if(!ids.insert(service.id).second) invalid("duplicate service id " + service.id);
        if(!ports.insert(service.listenPort).second) invalid("duplicate listenPort");
        hasEnabledDataNode = hasEnabledDataNode || (service.enabled && service.type == ServiceType::kDataNode);
        config.services.push_back(std::move(service));
    }
    if(config.services.empty()) invalid("services cannot be empty");
    if(hasEnabledDataNode) {
        config.gatewayAddress = required<std::string>(cluster, "gatewayAddress");
        config.gatewayPort = port(cluster, "gatewayPort");
        if(config.gatewayAddress.empty()) invalid("empty gatewayAddress");
    } else if(cluster["gatewayAddress"] || cluster["gatewayPort"]) {
        config.gatewayAddress = cluster["gatewayAddress"] ? cluster["gatewayAddress"].as<std::string>() : "";
        config.gatewayPort = cluster["gatewayPort"] ? port(cluster, "gatewayPort") : 0;
    }
    return config;
}



NodeAgentConfig parseNodeAgentConfigText(const std::string& yamlText)
{
    //直接解析文字来得到node
    try {
        return parse(YAML::Load(yamlText));
    } catch(const YAML::ParserException&) {
        invalid("YAML parse error");
    } catch(const YAML::BadConversion&) {
        invalid("YAML value type error");
    }
}

NodeAgentConfig loadNodeAgentConfigFile(const std::string& path)
{
    //解析指定路径来得到node
    try {
        return parse(YAML::LoadFile(path));
    } catch(const YAML::BadFile&) {
        throw std::runtime_error("cannot read node agent configuration file: " + path);
    } catch(const YAML::ParserException&) {
        invalid("YAML parse error");
    } catch(const YAML::BadConversion&) {
        invalid("YAML value type error");
    }
}

std::string readClusterSecret(const std::string& path)
{
    std::ifstream file(path);
    if(!file) throw std::runtime_error("cannot read cluster secret file");
    std::ostringstream value;
    value << file.rdbuf();
    std::string secret = value.str();
    if(secret.size() >= 2 && secret.compare(secret.size() - 2, 2, "\r\n") == 0) secret.resize(secret.size() - 2);
    else if(!secret.empty() && secret.back() == '\n') secret.pop_back();
    if(secret.empty()) throw std::runtime_error("cluster secret file is empty");
    return secret;
}

}  // namespace miniKV::v2
}  // namespace
