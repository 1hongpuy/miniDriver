#include "TestCheck.hpp"
#include "agent/ProcessSupervisor.hpp"

#include <chrono>
#include <filesystem>
#include <fstream>
#include <thread>

int main()
{
    const auto root = std::filesystem::temp_directory_path() / "minikv-process-supervisor-test";
    std::error_code error;
    std::filesystem::remove_all(root, error);
    std::filesystem::create_directories(root, error);

    miniKV::agent::ChildSpec spec;
    spec.id = "child";
    spec.executable = "/bin/sh";
    spec.argv = {"/bin/sh", "-c", "printf '%s' \"$MINIKV_V2_CLUSTER_SECRET:$MINIKV_TEST_ENV\""};
    spec.environment["MINIKV_TEST_ENV"] = "present";
    spec.stdoutPath = (root / "child.out").string();
    spec.stderrPath = (root / "child.err").string();
    spec.restart.onFailure = false;

    miniKV::agent::ProcessSupervisor supervisor("cluster-secret");
    supervisor.start(spec);
    for(int attempts = 0; attempts < 100 && supervisor.hasRunningChildren(); ++attempts) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        supervisor.reapExitedChildren(std::chrono::steady_clock::now());
    }
    MINIKV_CHECK(!supervisor.hasRunningChildren());

    std::ifstream output(spec.stdoutPath);
    const std::string content((std::istreambuf_iterator<char>(output)),
                              std::istreambuf_iterator<char>());
    MINIKV_CHECK(content == "cluster-secret:present");

    std::filesystem::remove_all(root, error);
    return 0;
}
