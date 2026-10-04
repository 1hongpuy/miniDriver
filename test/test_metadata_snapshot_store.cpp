#include "TestCheck.hpp"
#include "metadata/MetadataStateMachine.hpp"
#include "metadata/SnapshotStore.hpp"

#include <filesystem>
#include <fstream>
#include <unistd.h>

using namespace miniKV::metadata;

int main()
{
    const std::string directory = "/tmp/minikv-metadata-snapshot-" + std::to_string(::getpid());
    std::filesystem::remove_all(directory);
    SnapshotStore store(directory);
    MetadataStateMachine state;
    MetadataCommand barrier; barrier.commandId = "barrier"; barrier.type = MetadataCommandType::kReadBarrier;
    barrier.payload = ReadBarrierPayload{42};
    MINIKV_CHECK(state.apply(barrier, 1, 9).status == ApplyStatus::kOk);
    std::string error;
    MINIKV_CHECK(store.publish(state.snapshot(), &error));
    const auto loaded = store.load(&error); MINIKV_CHECK(loaded);
    MetadataStateMachine restored; MINIKV_CHECK(restored.restore(*loaded));
    MINIKV_CHECK(restored.stateDigest() == state.stateDigest());

    const std::string published = directory + "/snapshot.bin";
    {
        std::fstream file(published, std::ios::in | std::ios::out | std::ios::binary);
        MINIKV_CHECK(file.good()); file.seekp(-1, std::ios::end); file.put('x');
    }
    MINIKV_CHECK(!store.load(&error));
    std::filesystem::remove_all(directory);
    return 0;
}
