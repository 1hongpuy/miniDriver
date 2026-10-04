#pragma once

#include "metadata/MetadataTypes.hpp"

#include <optional>
#include <string>

namespace miniKV::metadata {

// Persists snapshots using temp-file + fsync + rename + parent-directory fsync.
// A failed publication never removes the previous valid snapshot.
class SnapshotStore {
public:
    explicit SnapshotStore(std::string directory);

    bool publish(const MetadataSnapshot& snapshot, std::string* error = nullptr) const;
    std::optional<MetadataSnapshot> load(std::string* error = nullptr) const;
    const std::string& directory() const { return directory_; }

private:
    std::string directory_;
};

} // namespace miniKV::metadata
