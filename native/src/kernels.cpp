#include "xuntrade/kernels.hpp"

#include <algorithm>
#include <array>
#include <bit>
#include <atomic>
#include <condition_variable>
#include <cstring>
#include <deque>
#include <exception>
#include <functional>
#include <limits>
#include <mutex>
#include <stdexcept>
#include <thread>

#if defined(_M_X64) || defined(__x86_64__)
#include <nmmintrin.h>
#if defined(_MSC_VER)
#include <intrin.h>
#endif
#endif

namespace xuntrade {
namespace {
constexpr auto crc_table = [] {
    std::array<std::uint32_t, 256> table{};
    for (std::uint32_t i = 0; i < 256; ++i) {
        auto value = i;
        for (unsigned j = 0; j < 8; ++j)
            value = (value >> 1) ^ ((value & 1) ? 0x82f63b78U : 0U);
        table[i] = value;
    }
    return table;
}();

std::atomic<unsigned> pool_threads{0};

class WorkerPool {
    std::mutex mutex_;
    std::condition_variable ready_;
    std::deque<std::function<void()>> jobs_;
    std::vector<std::jthread> threads_;
    bool closing_ = false;

    void run() {
        while (true) {
            std::function<void()> job;
            {
                std::unique_lock lock(mutex_);
                ready_.wait(lock, [&] { return closing_ || !jobs_.empty(); });
                if (jobs_.empty()) return;
                job = std::move(jobs_.front());
                jobs_.pop_front();
            }
            job();
        }
    }
public:
    WorkerPool() { threads_.reserve(maximum_workers - 1); }
    ~WorkerPool() {
        {
            std::lock_guard lock(mutex_);
            closing_ = true;
        }
        ready_.notify_all();
        threads_.clear();  // Join before destroying the queue or condition variable.
    }

    void submit(std::vector<std::function<void()>>& jobs, unsigned needed) {
        std::unique_lock lock(mutex_);
        if (closing_ || jobs_.size() + jobs.size() > 128)
            throw std::runtime_error("native worker queue capacity exhausted");
        while (threads_.size() < needed) {
            threads_.emplace_back([this] { run(); });
            pool_threads.store(static_cast<unsigned>(threads_.size()));
        }
        // Publish the entire group atomically. On allocation failure remove every
        // newly queued closure before the workers can see its caller-owned state.
        std::size_t added = 0;
        try {
            for (auto& job : jobs) { jobs_.push_back(std::move(job)); ++added; }
        } catch (...) {
            while (added > 0) { jobs_.pop_back(); --added; }
            throw;
        }
        lock.unlock();
        ready_.notify_all();
    }
};

struct PoolOwner {
    const std::uint64_t pid = compute_process_id();
    WorkerPool* pool = new WorkerPool;
    ~PoolOwner() {
        // An inherited child has no copies of the parent's OS threads. Never
        // lock/join their inherited C++ handles while the child is exiting.
        if (pid == compute_process_id()) delete pool;
    }
};

WorkerPool& worker_pool() {
    if (!compute_process_is_owner())
        throw std::runtime_error("native compute cannot be inherited through fork; use spawn");
    static PoolOwner owner;
    return *owner.pool;
}

#if defined(_M_X64) || defined(__x86_64__)
bool has_sse42() noexcept {
#if defined(_MSC_VER)
    int info[4]{};
    __cpuid(info, 1);
    return (info[2] & (1 << 20)) != 0;
#elif defined(__GNUC__) || defined(__clang__)
    return __builtin_cpu_supports("sse4.2");
#else
    return false;
#endif
}

#if defined(__GNUC__) || defined(__clang__)
__attribute__((target("sse4.2")))
#endif
std::uint32_t crc_hardware(std::span<const std::uint8_t> data) noexcept {
    std::uint64_t crc = 0xffffffffU;
    auto* cursor = data.data();
    auto size = data.size();
    while (size >= sizeof(std::uint64_t)) {
        std::uint64_t word;
        std::memcpy(&word, cursor, sizeof(word));
        crc = _mm_crc32_u64(crc, word);
        cursor += sizeof(word);
        size -= sizeof(word);
    }
    while (size--) crc = _mm_crc32_u8(static_cast<std::uint32_t>(crc), *cursor++);
    return static_cast<std::uint32_t>(crc) ^ 0xffffffffU;
}
#endif

using CrcFunction = std::uint32_t (*)(std::span<const std::uint8_t>) noexcept;
const CrcFunction selected_crc = []() -> CrcFunction {
#if defined(_M_X64) || defined(__x86_64__)
    if (has_sse42()) return crc_hardware;
#endif
    return crc32c_portable;
}();

// BLAKE2s-128, personalization "tcshard1", matching the existing Python plan.
constexpr std::array<std::uint32_t, 8> iv{
    0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a,
    0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19};
constexpr std::uint8_t sigma[10][16]{
    {0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15},
    {14,10,4,8,9,15,13,6,1,12,0,2,11,7,5,3},
    {11,8,12,0,5,2,15,13,10,14,3,6,7,1,9,4},
    {7,9,3,1,13,12,11,14,2,6,5,10,4,0,15,8},
    {9,0,5,7,2,4,10,15,14,1,11,12,6,8,3,13},
    {2,12,6,10,0,11,8,3,4,13,7,5,15,14,1,9},
    {12,5,1,15,14,13,4,10,0,7,6,3,9,2,8,11},
    {13,11,7,14,12,1,3,9,5,0,15,4,8,6,2,10},
    {6,15,14,9,11,3,0,8,12,2,13,7,1,4,10,5},
    {10,2,8,4,7,6,1,5,15,11,9,14,3,12,13,0}};

std::uint32_t load_le32(const std::uint8_t* p) noexcept {
    return std::uint32_t(p[0]) | (std::uint32_t(p[1]) << 8) |
           (std::uint32_t(p[2]) << 16) | (std::uint32_t(p[3]) << 24);
}

void compress(std::array<std::uint32_t, 8>& h, const std::uint8_t* block,
              std::uint64_t count, bool last) noexcept {
    std::array<std::uint32_t, 16> m{}, v{};
    for (unsigned i = 0; i < 16; ++i) m[i] = load_le32(block + 4 * i);
    std::copy(h.begin(), h.end(), v.begin());
    std::copy(iv.begin(), iv.end(), v.begin() + 8);
    v[12] ^= static_cast<std::uint32_t>(count);
    v[13] ^= static_cast<std::uint32_t>(count >> 32);
    if (last) v[14] = ~v[14];
    auto g = [&](unsigned a, unsigned b, unsigned c, unsigned d,
                 std::uint32_t x, std::uint32_t y) {
        v[a] += v[b] + x; v[d] = std::rotr(v[d] ^ v[a], 16);
        v[c] += v[d];     v[b] = std::rotr(v[b] ^ v[c], 12);
        v[a] += v[b] + y; v[d] = std::rotr(v[d] ^ v[a], 8);
        v[c] += v[d];     v[b] = std::rotr(v[b] ^ v[c], 7);
    };
    for (const auto& s : sigma) {
        g(0,4,8,12,m[s[0]],m[s[1]]); g(1,5,9,13,m[s[2]],m[s[3]]);
        g(2,6,10,14,m[s[4]],m[s[5]]); g(3,7,11,15,m[s[6]],m[s[7]]);
        g(0,5,10,15,m[s[8]],m[s[9]]); g(1,6,11,12,m[s[10]],m[s[11]]);
        g(2,7,8,13,m[s[12]],m[s[13]]); g(3,4,9,14,m[s[14]],m[s[15]]);
    }
    for (unsigned i = 0; i < 8; ++i) h[i] ^= v[i] ^ v[i + 8];
}

template <typename Function>
void parallel_for(std::size_t count, unsigned workers, Function function) {
    if (workers == 0 || workers > maximum_workers)
        throw std::invalid_argument("workers must be in [1, 32]");
    if (count > maximum_batch_size) throw std::invalid_argument("batch exceeds 1000000 rows");
    // Small batches stay on the caller. Larger requests reuse process-owned workers.
    workers = static_cast<unsigned>(std::min<std::size_t>(workers, count / 2048 + 1));
    if (workers <= 1) {
        for (std::size_t i = 0; i < count; ++i) function(i);
        return;
    }
    std::mutex state_mutex;
    std::condition_variable complete;
    unsigned remaining = workers - 1;
    std::exception_ptr error;
    std::vector<std::function<void()>> jobs;
    jobs.reserve(workers - 1);
    for (unsigned worker = 1; worker < workers; ++worker) {
        jobs.emplace_back([&, worker] {
            try {
                for (auto i = count * worker / workers; i < count * (worker + 1) / workers; ++i)
                    function(i);
            } catch (...) {
                std::lock_guard lock(state_mutex);
                if (!error) error = std::current_exception();
            }
            std::lock_guard lock(state_mutex);
            --remaining;
            complete.notify_one();
        });
    }
    worker_pool().submit(jobs, workers - 1);
    try {
        for (std::size_t i = 0; i < count / workers; ++i) function(i);
    } catch (...) {
        std::lock_guard lock(state_mutex);
        if (!error) error = std::current_exception();
    }
    std::unique_lock lock(state_mutex);
    complete.wait(lock, [&] { return remaining == 0; });
    if (error) std::rethrow_exception(error);
}
}  // namespace

std::uint32_t crc32c_portable(std::span<const std::uint8_t> data) noexcept {
    auto crc = 0xffffffffU;
    for (auto byte : data) crc = crc_table[(crc ^ byte) & 0xff] ^ (crc >> 8);
    return crc ^ 0xffffffffU;
}

std::uint32_t crc32c(std::span<const std::uint8_t> data) noexcept { return selected_crc(data); }
unsigned worker_pool_thread_count() noexcept { return pool_threads.load(); }
const char* crc32c_implementation() noexcept {
    return selected_crc == crc32c_portable ? "portable-table" : "x86-sse4.2";
}

std::uint32_t session_shard(std::string_view id, std::uint32_t shards) {
    if (shards == 0 || shards > 32) throw std::invalid_argument("shards must be in [1, 32]");
    auto h = iv;
    h[0] ^= 0x01010010U;  // fanout=1, depth=1, digest_size=16
    h[6] ^= 0x68736374U;  // "tcsh"
    h[7] ^= 0x31647261U;  // "ard1"
    auto* p = reinterpret_cast<const std::uint8_t*>(id.data());
    auto remaining = id.size();
    std::uint64_t consumed = 0;
    while (remaining > 64) {
        consumed += 64;
        compress(h, p, consumed, false);
        p += 64;
        remaining -= 64;
    }
    std::array<std::uint8_t, 64> last{};
    if (remaining) std::memcpy(last.data(), p, remaining);
    compress(h, last.data(), consumed + remaining, true);
    // int.from_bytes(digest, "big") % shards, without 128-bit platform types.
    std::uint32_t result = 0;
    for (unsigned i = 0; i < 16; ++i)
        result = (result * 256 + ((h[i / 4] >> (8 * (i % 4))) & 0xff)) % shards;
    return result;
}

std::vector<std::uint32_t> session_shards(
    const std::vector<std::string>& ids, std::uint32_t shards, unsigned workers) {
    if (shards == 0 || shards > 32) throw std::invalid_argument("shards must be in [1, 32]");
    if (ids.size() > maximum_batch_size) throw std::invalid_argument("batch exceeds 1000000 rows");
    std::vector<std::uint32_t> results(ids.size());
    parallel_for(ids.size(), workers, [&](auto i) { results[i] = session_shard(ids[i], shards); });
    return results;
}

Assessment assess_order(const OrderInput& row) noexcept {
    if (row.quantity <= 0 || row.price_minor <= 0 || row.available_minor < 0 ||
        row.order_limit_minor < 0) return {0, AssessmentCode::invalid};
    if (row.quantity > std::numeric_limits<std::int64_t>::max() / row.price_minor)
        return {0, AssessmentCode::overflow};
    const auto notional = row.quantity * row.price_minor;
    if (notional > row.order_limit_minor) return {notional, AssessmentCode::order_limit};
    if (notional > row.available_minor) return {notional, AssessmentCode::cash_limit};
    return {notional, AssessmentCode::accepted};
}

std::vector<Assessment> assess_orders(std::span<const OrderInput> rows, unsigned workers) {
    if (rows.size() > maximum_batch_size) throw std::invalid_argument("batch exceeds 1000000 rows");
    std::vector<Assessment> results(rows.size());
    parallel_for(rows.size(), workers, [&](auto i) { results[i] = assess_order(rows[i]); });
    return results;
}
}  // namespace xuntrade
