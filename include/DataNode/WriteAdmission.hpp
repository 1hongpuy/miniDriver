#pragma once

#include <atomic>
#include <cstdint>

namespace miniKV::datanode {

// The local admission counter is authoritative for this process. Gateway
// heartbeats observe it, but cannot replace it because they are delayed.
class WriteAdmission {
public:
    explicit WriteAdmission(uint32_t limit) : limit_(limit == 0 ? 1 : limit) {}

    bool tryAcquire() {
        uint32_t current = active_.load(std::memory_order_relaxed);
        while (current < limit_) {
            if (active_.compare_exchange_weak(current, current + 1,
                                              std::memory_order_acq_rel,
                                              std::memory_order_relaxed)) {
                return true;
            }
        }
        return false;
    }

    void release() {
        uint32_t current = active_.load(std::memory_order_relaxed);
        while (current != 0) {
            if (active_.compare_exchange_weak(current, current - 1,
                                              std::memory_order_acq_rel,
                                              std::memory_order_relaxed)) {
                return;
            }
        }
    }

    uint32_t active() const { return active_.load(std::memory_order_relaxed); }
    uint32_t limit() const { return limit_; }

private:
    const uint32_t limit_;
    std::atomic<uint32_t> active_{0};
};

}  // namespace miniKV::datanode
