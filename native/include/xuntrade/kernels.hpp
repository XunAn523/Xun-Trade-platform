#pragma once

#if defined(_MSVC_LANG)
#if _MSVC_LANG < 202002L
#error "XunTradeOMSEMS C++ sources require ISO C++20 or newer."
#endif
#elif __cplusplus < 202002L
#error "XunTradeOMSEMS C++ sources require ISO C++20 or newer."
#endif

#include <cstddef>
#include <cstdint>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace xuntrade {

// Stable wire algorithms. Changing these changes persisted checksums/routes.
std::uint32_t crc32c(std::span<const std::uint8_t> data) noexcept;
std::uint32_t crc32c_portable(std::span<const std::uint8_t> data) noexcept;
const char* crc32c_implementation() noexcept;
std::uint32_t session_shard(std::string_view execution_id, std::uint32_t shards);
std::vector<std::uint32_t> session_shards(
    const std::vector<std::string>& execution_ids, std::uint32_t shards, unsigned workers);

enum class AssessmentCode : std::uint8_t {
    accepted = 0, invalid = 1, overflow = 2, order_limit = 3, cash_limit = 4
};

// Values use one caller-selected currency minor unit. No binary floating point.
struct OrderInput {
    std::int64_t quantity;
    std::int64_t price_minor;
    std::int64_t available_minor;
    std::int64_t order_limit_minor;
};

struct Assessment {
    std::int64_t notional_minor;
    AssessmentCode code;
    bool operator==(const Assessment&) const = default;
};

Assessment assess_order(const OrderInput& input) noexcept;
// Independent snapshots only: this is not a shared cash reservation ledger.
std::vector<Assessment> assess_orders(std::span<const OrderInput> inputs, unsigned workers);

inline constexpr std::size_t maximum_batch_size = 1'000'000;
inline constexpr unsigned maximum_workers = 32;
inline constexpr std::size_t maximum_batch_bytes = 64 * 1024 * 1024;
inline constexpr std::size_t maximum_compute_bytes = 256 * 1024 * 1024;
inline constexpr unsigned maximum_compute_calls = 4;

struct ComputeCapacity {
    std::size_t bytes;
    unsigned calls;
    unsigned workers;
};
ComputeCapacity compute_capacity();
std::uint64_t compute_process_id() noexcept;
bool compute_process_is_owner() noexcept;
unsigned worker_pool_thread_count() noexcept;

// Binding-owned lease spans validation, copied inputs, calculation and outputs.
class ComputeLease {
    std::size_t bytes_;
    unsigned workers_;
public:
    ComputeLease(std::size_t bytes, unsigned workers);
    ~ComputeLease();
    ComputeLease(const ComputeLease&) = delete;
    ComputeLease& operator=(const ComputeLease&) = delete;
};

}  // namespace xuntrade
