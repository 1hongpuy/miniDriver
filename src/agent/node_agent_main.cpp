#include "agent/NodeAgentConfig.hpp"
#include "agent/ProcessSupervisor.hpp"

#include <cerrno>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <poll.h>
#include <signal.h>
#include <stdexcept>
#include <string>
#include <sys/signalfd.h>
#include <unistd.h>

namespace {

using miniKV::agent::ChildSpec;
using miniKV::agent::ManagedServiceConfig;
using miniKV::agent::NodeAgentConfig;
using miniKV::agent::ServiceType;
using namespace miniKV::agent;
void usage()
{
    std::cerr << "usage: minikv_v2_node_agent --config <path> [--bin-dir <directory>]\n";
}

std::string executableDirectory(const char* argv0)
{
    const std::filesystem::path path(argv0 == nullptr ? "" : argv0);
    return path.has_parent_path() ? path.parent_path().string() : ".";
}

void createParentDirectory(const std::string& path)
{
    if(path.empty()) return;
    const std::filesystem::path parent = std::filesystem::path(path).parent_path();
    if(!parent.empty()) std::filesystem::create_directories(parent);
}

ChildSpec childSpec(const ManagedServiceConfig& service, const NodeAgentConfig& config,
                    const std::string& binaryDirectory)
{
    const std::filesystem::path executable = std::filesystem::path(binaryDirectory) /
        (service.type == ServiceType::kGateway ? "minikv_v2_gateway" : "minikv_v2_datanode");
    ChildSpec spec;
    spec.id = service.id;
    spec.executable = executable.string();
    spec.stdoutPath = service.logs.stdoutPath;
    spec.stderrPath = service.logs.stderrPath;
    spec.restart = service.restart;
    if(service.type == ServiceType::kGateway) {
        spec.argv = {spec.executable, std::to_string(service.listenPort), service.dataDir};
    } else {
        spec.argv = {spec.executable, config.nodeId, config.advertiseAddress,
                     std::to_string(service.listenPort), service.dataDir,
                     config.gatewayAddress, std::to_string(config.gatewayPort)};
    }
    return spec;
}

}  // namespace

int main(int argc, char** argv)
{
    std::string configPath;
    std::string binaryDirectory;
    for(int index = 1; index < argc; ++index) {
        const std::string option = argv[index];
        if((option == "--config" || option == "--bin-dir") && index + 1 < argc) {
            std::string& destination = option == "--config" ? configPath : binaryDirectory;
            destination = argv[++index];
            continue;
        }
        usage();
        return 2;
    }
    if(configPath.empty()) {
        usage();
        return 2;
    }
    if(binaryDirectory.empty()) binaryDirectory = executableDirectory(argv[0]);

    try {
    //检查部分
    /*
        主循环就是 while 里的那几步，它的运行逻辑像极了人类的“照顾婴儿”流程：

检测是否有活着的婴儿或待喂奶的婴儿（hasRunningChildren 或 hasPendingRestarts）。如果没有，管家下班（进程退出）。

竖起耳朵听哭声（poll 监听 signalfd）：

听到孩子死了（SIGCHLD）→ 立刻去收尸（reap）。

听到管理员喊停（SIGTERM）→ 开启关机流程，不再重启孩子。

哪怕没哭声，每隔 200ms 也要扫一眼（超时醒来）：

再去收一次尸（防止信号丢失导致漏掉僵尸）。

看看有没有“正在等待复活时间”的孩子（restartPending），如果当前时间到了约定时间（now >= restartAt），立即把它重新生出来（调用 spawn）。
    */
        const NodeAgentConfig config = miniKV::agent::loadNodeAgentConfigFile(configPath);
        const std::string secret = miniKV::agent::readClusterSecret(config.secretFile);
        miniKV::agent::ProcessSupervisor supervisor(secret);

        sigset_t signals;
        ::sigemptyset(&signals);
        ::sigaddset(&signals, SIGCHLD); //子进程停止
        ::sigaddset(&signals, SIGINT);  //交互式注意力信号  Ctrl +  C
        ::sigaddset(&signals, SIGTERM); //终止请求         kill
        if(::sigprocmask(SIG_BLOCK, &signals, nullptr) != 0) throw std::runtime_error("cannot block agent signals");
        //把这三个信号屏蔽，内核只会记录这个信号发生，不会打算代码运行
        const int signalFd = ::signalfd(-1, &signals, SFD_CLOEXEC | SFD_NONBLOCK);
        if(signalFd < 0) throw std::runtime_error("cannot create signalfd");
        //登记成一个文件描述符 id

        for(const ManagedServiceConfig& service : config.services) {
            if(!service.enabled) continue;
            std::filesystem::create_directories(service.dataDir);
            createParentDirectory(service.logs.stdoutPath);
            createParentDirectory(service.logs.stderrPath);
            supervisor.start(childSpec(service, config, binaryDirectory));
        }

        bool stopping = false;
        while(supervisor.hasRunningChildren() || supervisor.hasPendingRestarts()) {
            pollfd descriptor{signalFd, POLLIN, 0};
            const int ready = ::poll(&descriptor, 1, 200);
            //监控那个fd，只监控一个
            const auto now = std::chrono::steady_clock::now();
            if(ready > 0 && (descriptor.revents & POLLIN)) {
                signalfd_siginfo signalInfo{};
                while(::read(signalFd, &signalInfo, sizeof(signalInfo)) == sizeof(signalInfo)) {
                    if(signalInfo.ssi_signo == SIGCHLD) //孩子死亡了，收尸 
                        supervisor.reapExitedChildren(now);
                    if(signalInfo.ssi_signo == SIGINT || signalInfo.ssi_signo == SIGTERM) {
                        stopping = true;
                        supervisor.beginShutdown();
                    }
                }
            } else if(ready < 0 && errno != EINTR) {
                ::close(signalFd);
                throw std::runtime_error("agent poll failed");
            }
            supervisor.reapExitedChildren(now);
            if(!stopping) supervisor.startDueRestarts(now);
        }
        ::close(signalFd);
        return 0;
    } catch(const std::exception& error) {
        std::cerr << "node agent failed: " << error.what() << '\n';
        return 1;
    }
}
