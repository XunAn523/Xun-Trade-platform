/* Minimal audited atomic ABI for the TradeCore QMT local SPSC rings. */

#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#include <stdint.h>

#ifdef _WIN64
#define TCQMT_EXPORT __declspec(dllexport)
#else
#error TradeCore QMT atomics require a 64-bit Windows build.
#endif

TCQMT_EXPORT int64_t __cdecl tradecore_qmt_load_u64(volatile int64_t *address) {
    return InterlockedCompareExchange64((volatile LONG64 *)address, 0, 0);
}

TCQMT_EXPORT int64_t __cdecl tradecore_qmt_store_u64(volatile int64_t *address, int64_t value) {
    return InterlockedExchange64((volatile LONG64 *)address, (LONG64)value);
}

TCQMT_EXPORT int __cdecl tradecore_qmt_is_aligned_u64(const volatile int64_t *address) {
    return ((uintptr_t)address & 7U) == 0U;
}
