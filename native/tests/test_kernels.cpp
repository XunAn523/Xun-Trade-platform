#include "xuntrade/kernels.hpp"

#include <array>
#include <iostream>
#include <limits>
#include <stdexcept>

// Exercise the effective compiler mode rather than trusting a CMake label.
static_assert(__cplusplus == 202002L, "Native targets must compile in ISO C++20 mode");

void require(bool condition, const char* message) {
    if (!condition) throw std::runtime_error(message);
}

int main() {
    using namespace xuntrade;
    {
        ComputeLease lease(1024, maximum_workers);
        require(compute_capacity().workers == maximum_workers, "worker reservation mismatch");
        bool rejected = false;
        try { ComputeLease excess(1, 1); }
        catch (const std::runtime_error&) { rejected = true; }
        require(rejected, "overcommitted worker budget");
    }
    require(compute_capacity().calls == 0, "compute lease leaked on failure");
    try { ComputeLease excess(maximum_batch_bytes + 1, 1); require(false, "oversized memory"); }
    catch (const std::invalid_argument&) {}
    {
        ComputeLease a(1, 1), b(1, 1), c(1, 1), d(1, 1);
        bool rejected = false;
        try { ComputeLease excess(1, 1); }
        catch (const std::runtime_error&) { rejected = true; }
        require(rejected, "overcommitted call budget");
    }
    require(compute_capacity().bytes == 0, "compute memory reservation leaked");
    const std::array<std::uint8_t, 9> reference{'1','2','3','4','5','6','7','8','9'};
    require(crc32c(reference) == 0xe3069283U, "CRC32C reference mismatch");
    require(crc32c_portable(reference) == 0xe3069283U, "portable CRC mismatch");
    require(crc32c({}) == 0, "empty CRC mismatch");
    std::vector<std::uint8_t> bytes(65537);
    for (std::size_t i = 0; i < bytes.size(); ++i) bytes[i] = static_cast<std::uint8_t>(i * 37);
    for (std::size_t offset = 0; offset < 16; ++offset) {
        auto data = std::span(bytes).subspan(offset);
        require(crc32c(data) == crc32c_portable(data), "unaligned CRC mismatch");
    }
    require(session_shard("execution-1", 31) == 14, "BLAKE2s route vector mismatch");
    require(session_shard(std::string(64, 'a'), 31) == 6, "full-block route mismatch");
    require(session_shard(std::string(65, 'a'), 31) == 4, "multi-block route mismatch");
    const auto maximum = std::numeric_limits<std::int64_t>::max();
    require(assess_order({100, 12, 1200, 1200}) == Assessment{1200, AssessmentCode::accepted},
            "exact available boundary mismatch");
    require(assess_order({maximum, 2, maximum, maximum}).code == AssessmentCode::overflow,
            "overflow must reject before multiplication");
    require(assess_order({-1, 2, 10, 10}).code == AssessmentCode::invalid, "negative quantity");
    require(assess_order({100, 12, 1199, 1200}).code == AssessmentCode::cash_limit, "cash limit");
    require(assess_order({100, 12, 1200, 1199}).code == AssessmentCode::order_limit, "order limit");
    std::vector<std::string> ids(20000, "execution-1");
    require(session_shards(ids, 31, 1) == session_shards(ids, 31, 8), "parallel route mismatch");
    std::vector<OrderInput> rows(20000, {100, 12, 1200, 1200});
    require(assess_orders(rows, 1) == assess_orders(rows, 8), "parallel assessment mismatch");
    try { assess_orders({}, 0); require(false, "invalid workers accepted"); }
    catch (const std::invalid_argument&) {}
    std::cout << "C++ kernels passed; crc=" << crc32c_implementation() << '\n';
}
