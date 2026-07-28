#include "agent/ProcessSupervisor.hpp"

#include <algorithm>
#include <cerrno>
#include <climits>
#include <cstdlib>
#include <fcntl.h>
#include <stdexcept>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

extern char** environ;

namespace miniKV {
namespace agent {

int openLog(const std::string& path)
{
    const char* target = path.empty() ? "/dev/null" : path.c_str();
    return ::open(target, O_CREAT | O_APPEND | O_WRONLY, 0644);
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
    const pid_t pid = ::fork();
    if(pid < 0) throw std::runtime_error("fork failed");
    if(pid == 0) {
        //子进程
        sigset_t unblockedSignals;
        ::sigemptyset(&unblockedSignals);
        ::sigprocmask(SIG_SETMASK, &unblockedSignals, nullptr);
        //用了个空信号集来修改之前空的信号就行
        const int stdoutFd = openLog(state.spec.stdoutPath);
        const int stderrFd = openLog(state.spec.stderrPath);
        if(stdoutFd < 0 || stderrFd < 0 ||
           ::dup2(stdoutFd, STDOUT_FILENO) < 0 || ::dup2(stderrFd, STDERR_FILENO) < 0 ||
           ::setenv("MINIKV_V2_CLUSTER_SECRET", clusterSecret_.c_str(), 1) != 0) 
            //设置环境变量
        {
            _exit(127);
        }
        if(stdoutFd != STDOUT_FILENO) ::close(stdoutFd);
        if(stderrFd != STDERR_FILENO) ::close(stderrFd);

        std::vector<char*> argv;
        argv.reserve(state.spec.argv.size() + 1);
        for(std::string& value : state.spec.argv) argv.push_back(value.data());
        argv.push_back(nullptr);
        ::execve(state.spec.executable.c_str(), argv.data(), environ);
        //只有这个函数运行失败才会执行
        _exit(127);
    }
    state.pid = static_cast<int>(pid);
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
        //WIFEXITED true就是正常退出，而WEXITSTATUS表示是退出的返回值，是0就是正常的
        if(failed && restartsEnabled_ && state.spec.restart.onFailure) {
            ++state.restarts;
            state.restartAt = now + restartDelay(state);
            state.restartPending = true;
        }
    }
}

void ProcessSupervisor::startDueRestarts(std::chrono::steady_clock::time_point now)
{
    if(!restartsEnabled_) return;
    for(auto& [id, state] : children_) {
        if(state.restartPending && state.pid < 0 && state.restartAt <= now) {
            state.restartPending = false;
            spawn(state);
        }
    }
}

void ProcessSupervisor::beginShutdown()
{
    if(!restartsEnabled_) return;
    restartsEnabled_ = false;
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