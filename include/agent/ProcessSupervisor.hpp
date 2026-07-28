#pragma once

#include "NodeAgentConfig.hpp"

#include <chrono>
#include <cstdint>
#include <map>
#include <string>
#include <vector>

namespace miniKV {
namespace agent {

struct ChildSpec {
    std::string id;
    std::string executable;
    std::vector<std::string> argv;
    std::string stdoutPath;
    std::string stderrPath;
    RestartPolicy restart;
};

class ProcessSupervisor {
public:
    explicit ProcessSupervisor(std::string clusterSecret);
    ~ProcessSupervisor();

    ProcessSupervisor(const ProcessSupervisor&) = delete;
    ProcessSupervisor& operator=(const ProcessSupervisor&) = delete;

    void start(const ChildSpec& spec);
    void startDueRestarts(std::chrono::steady_clock::time_point now);
    void reapExitedChildren(std::chrono::steady_clock::time_point now);
    void beginShutdown();

    bool hasRunningChildren() const;
    bool hasPendingRestarts() const;
    bool restartsEnabled() const;
    uint32_t restartCount(const std::string& id) const;
    std::chrono::steady_clock::time_point nextRestartAt(const std::string& id) const;

private:
    struct ChildState {
        ChildSpec spec;
        int pid = -1;
        uint32_t restarts = 0;
        bool restartPending = false;
        std::chrono::steady_clock::time_point restartAt{};
    };

    void spawn(ChildState& state);
    std::chrono::seconds restartDelay(const ChildState& state) const;

    std::string clusterSecret_; //集群密匙
    bool restartsEnabled_ = true; //是否允许重启子进程
    std::map<std::string, ChildState> children_;
};


}


}
