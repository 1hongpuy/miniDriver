#include "agent/ProcessSupervisor.hpp"
#include "utils/AsyncLogger.hpp"

#include <algorithm>
#include <cerrno>
#include <climits>
#include <cstring>
#include <cstdlib>
#include <fcntl.h>
#include <spawn.h>
#include <stdexcept>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

extern char** environ;

namespace miniKV {
namespace agent {

std::vector<std::string> childEnvironment(const ChildSpec& spec, const std::string& clusterSecret)
{
    std::map<std::string, std::string> overrides = spec.environment;
    overrides["MINIKV_V2_CLUSTER_SECRET"] = clusterSecret;

    std::vector<std::string> values;
    for(char** entry = environ; entry != nullptr && *entry != nullptr; ++entry) {
        const std::string value(*entry);
        const size_t equals = value.find('=');
        const std::string name = value.substr(0, equals);
        if(overrides.erase(name) == 0) values.push_back(value);
    }
    for(const auto& [name, value] : overrides) values.push_back(name + "=" + value);
    return values;
}



ProcessSupervisor::ProcessSupervisor(std::string clusterSecret)
    : clusterSecret_(std::move(clusterSecret))
{
    if(clusterSecret_.empty()) throw std::runtime_error("cluster secret is required for process supervision");
}

ProcessSupervisor::~ProcessSupervisor()
{
    beginShutdown();
}

void ProcessSupervisor::start(const ChildSpec& spec)
{
    if(spec.id.empty() || spec.executable.empty() || spec.argv.empty()) {
        throw std::runtime_error("invalid child process specification");
    }
    auto [it, inserted] = children_.try_emplace(spec.id);
    ChildState& state = it->second;
    if(!inserted && state.pid > 0) throw std::runtime_error("child process is already running: " + spec.id);
    state.spec = spec;
    state.restartPending = false;
    spawn(state);
}

void ProcessSupervisor::spawn(ChildState& state)
{
    posix_spawn_file_actions_t actions;
    posix_spawnattr_t attributes;
    if(::posix_spawn_file_actions_init(&actions) != 0) {
        throw std::runtime_error("cannot initialize posix_spawn state");
    }
    if(::posix_spawnattr_init(&attributes) != 0) {
        ::posix_spawn_file_actions_destroy(&actions);
        throw std::runtime_error("cannot initialize posix_spawn state");
    }

    const char* stdoutPath = state.spec.stdoutPath.empty() ? "/dev/null" : state.spec.stdoutPath.c_str();
    const char* stderrPath = state.spec.stderrPath.empty() ? "/dev/null" : state.spec.stderrPath.c_str();
    int result = ::posix_spawn_file_actions_addopen(&actions, STDOUT_FILENO, stdoutPath,
                                                     O_CREAT | O_APPEND | O_WRONLY, 0644);
    if(result == 0) result = ::posix_spawn_file_actions_addopen(&actions, STDERR_FILENO, stderrPath,
                                                                  O_CREAT | O_APPEND | O_WRONLY, 0644);

    sigset_t unblockedSignals;
    ::sigemptyset(&unblockedSignals);
    if(result == 0) result = ::posix_spawnattr_setsigmask(&attributes, &unblockedSignals);
    if(result == 0) result = ::posix_spawnattr_setflags(&attributes, POSIX_SPAWN_SETSIGMASK);

    std::vector<std::string> argvStorage = state.spec.argv;
    std::vector<char*> argv;
    argv.reserve(argvStorage.size() + 1);
    for(std::string& value : argvStorage) argv.push_back(value.data());
    argv.push_back(nullptr);

    std::vector<std::string> environmentStorage = childEnvironment(state.spec, clusterSecret_);
    std::vector<char*> environment;
    environment.reserve(environmentStorage.size() + 1);
    for(std::string& value : environmentStorage) environment.push_back(value.data());
    environment.push_back(nullptr);

    pid_t pid = -1;
    if(result == 0) {
        result = ::posix_spawn(&pid, state.spec.executable.c_str(), &actions, &attributes,
                               argv.data(), environment.data());
    }
    ::posix_spawn_file_actions_destroy(&actions);
    ::posix_spawnattr_destroy(&attributes);
    if(result != 0) throw std::runtime_error("posix_spawn failed: " + std::string(std::strerror(result)));

    state.pid = static_cast<int>(pid);
    miniKV::utils::logInfo("event=service_spawn service=" + state.spec.id +
                           " pid=" + std::to_string(state.pid));
}

std::chrono::seconds ProcessSupervisor::restartDelay(const ChildState& state) const
{
    const uint32_t base = state.spec.restart.initialBackoffSeconds;
    const uint32_t maximum = state.spec.restart.maxBackoffSeconds;
    uint64_t delay = base;
    for(uint32_t step = 1; step < state.restarts && delay < maximum; ++step) {
        delay = std::min<uint64_t>(maximum, delay * 2U);
    }
    return std::chrono::seconds(delay);
}

void ProcessSupervisor::reapExitedChildren(std::chrono::steady_clock::time_point now)
{
    int status = 0;
    while(true) {
        const pid_t pid = ::waitpid(-1, &status, WNOHANG);
        if(pid == 0) return;
        if(pid < 0) {
            if(errno == EINTR) continue;
            //如果这个时候读取一些系统信号，会阻塞
            return;
        }
        auto found = std::find_if(children_.begin(), children_.end(), [pid](const auto& item) {
            return item.second.pid == pid;
        });
        if(found == children_.end()) continue;
        ChildState& state = found->second;
        state.pid = -1;
        const bool failed = !WIFEXITED(status) || WEXITSTATUS(status) != 0;
        miniKV::utils::logWarn("event=service_exit service=" + state.spec.id +
                               " pid=" + std::to_string(pid) + " status=" +
                               std::to_string(status) + " failed=" + (failed ? "true" : "false"));
        //WIFEXITED true就是正常退出，而WEXITSTATUS表示是退出的返回值，是0就是正常的
        if(failed && restartsEnabled_ && state.spec.restart.onFailure) {
            ++state.restarts;
            state.restartAt = now + restartDelay(state);
            state.restartPending = true;
            miniKV::utils::logInfo("event=service_restart_scheduled service=" + state.spec.id +
                                   " restart_count=" + std::to_string(state.restarts));
        }
    }
}

void ProcessSupervisor::startDueRestarts(std::chrono::steady_clock::time_point now)
{
    if(!restartsEnabled_) return;
    for(auto& [id, state] : children_) {
        if(state.restartPending && state.pid < 0 && state.restartAt <= now) {
            state.restartPending = false;
            miniKV::utils::logInfo("event=service_restart service=" + state.spec.id);
            spawn(state);
        }
    }
}

void ProcessSupervisor::beginShutdown()
{
    if(!restartsEnabled_) return;
    restartsEnabled_ = false;
    miniKV::utils::logInfo("event=agent_shutdown");
    for(auto& [id, state] : children_) {
        state.restartPending = false;
        if(state.pid > 0) ::kill(static_cast<pid_t>(state.pid), SIGTERM);
    }
}

bool ProcessSupervisor::hasRunningChildren() const
{
    return std::any_of(children_.begin(), children_.end(), [](const auto& item) {
        return item.second.pid > 0;
    });
}

bool ProcessSupervisor::hasPendingRestarts() const
{
    return std::any_of(children_.begin(), children_.end(), [](const auto& item) {
        return item.second.restartPending;
    });
}

bool ProcessSupervisor::restartsEnabled() const
{
    return restartsEnabled_;
}

uint32_t ProcessSupervisor::restartCount(const std::string& id) const
{
    const auto found = children_.find(id);
    return found == children_.end() ? 0 : found->second.restarts;
}

std::chrono::steady_clock::time_point ProcessSupervisor::nextRestartAt(const std::string& id) const
{
    const auto found = children_.find(id);
    return found == children_.end() ? std::chrono::steady_clock::time_point{} : found->second.restartAt;
}

}  // namespace miniKV::v2
}  // namespace
