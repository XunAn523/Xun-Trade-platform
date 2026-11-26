#include "xuntrade/kernels.hpp"

#include <mutex>
#include <stdexcept>
#if defined(_WIN32)
#include <process.h>
#else
#include <unistd.h>
#endif

namespace xuntrade {
std::uint64_t compute_process_id() noexcept {
#if defined(_WIN32)
    return static_cast<std::uint64_t>(_getpid());
#else
    return static_cast<std::uint64_t>(getpid());
#endif
}
namespace {
std::mutex budget_mutex;
ComputeCapacity used{};
const auto owner_pid = compute_process_id();
}

bool compute_process_is_owner() noexcept { return owner_pid == compute_process_id(); }

ComputeLease::ComputeLease(std::size_t bytes, unsigned workers)
    : bytes_(bytes), workers_(workers) {
    if (!compute_process_is_owner())
        throw std::runtime_error("native compute cannot be inherited through fork; use spawn");
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
    if (!compute_process_is_owner()) return;
    std::lock_guard lock(budget_mutex);
    used.bytes -= bytes_;
    used.calls -= 1;
    used.workers -= workers_;
}

ComputeCapacity compute_capacity() {
    if (!compute_process_is_owner())
        throw std::runtime_error("native compute cannot be inherited through fork; use spawn");
    std::lock_guard lock(budget_mutex);
    return used;
}
}
