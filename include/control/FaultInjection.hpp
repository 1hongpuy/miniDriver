#pragma once

#include <chrono>
#include <cstdint>

namespace miniKV {
namespace control {

enum class FaultPoint {
    kMetadataBeforePropose,
    kMetadataAfterCommit,
    kHeartbeatSend,
    kRepairBeforeFinalize,
};

struct FaultDecision {
    bool fail = false;
    std::chrono::milliseconds delay{0};
};

class Clock {
public:
    virtual ~Clock() = default;
    virtual std::chrono::steady_clock::time_point now() const = 0;
};

class SystemClock final : public Clock {
public:
    std::chrono::steady_clock::time_point now() const override
    {
        return std::chrono::steady_clock::now();
    }
};

class RandomSource {
public:
    virtual ~RandomSource() = default;
    virtual uint64_t next() = 0;
};

class FaultInjector {
public:
    virtual ~FaultInjector() = default;
    virtual FaultDecision before(FaultPoint point) = 0;
};

class NoFaultInjector final : public FaultInjector {
public:
    FaultDecision before(FaultPoint) override { return {}; }
};

}  // namespace control
}  // namespace miniKV
