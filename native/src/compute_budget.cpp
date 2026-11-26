#include "xuntrade/kernels.hpp"

#include <mutex>
#include <stdexcept>

namespace xuntrade {
namespace {
std::mutex budget_mutex;
ComputeCapacity used{};
}

ComputeLease::ComputeLease(std::size_t bytes, unsigned workers)
    : bytes_(bytes), workers_(workers) {
    if (bytes > maximum_batch_bytes)
        throw std::invalid_argument("batch memory budget exceeds 64 MiB");
    if (workers == 0 || workers > maximum_workers)
        throw std::invalid_argument("workers must be in [1, 32]");
    std::lock_guard lock(budget_mutex);
    if (used.calls >= maximum_compute_calls || bytes > maximum_compute_bytes - used.bytes ||
        workers > maximum_workers - used.workers)
        throw std::runtime_error("compute capacity exhausted; no computation was admitted");
    used.bytes += bytes;
    used.calls += 1;
    used.workers += workers;
}

ComputeLease::~ComputeLease() {
    std::lock_guard lock(budget_mutex);
    used.bytes -= bytes_;
    used.calls -= 1;
    used.workers -= workers_;
}

ComputeCapacity compute_capacity() {
    std::lock_guard lock(budget_mutex);
    return used;
}
}
