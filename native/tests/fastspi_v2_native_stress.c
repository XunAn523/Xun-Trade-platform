/* Linux-only sanitizer stress harness for the fastspi v2 ring primitive.
 *
 * This deliberately calls ``ring_try_push_record`` from pthreads rather than
 * via Python extension methods, so the test exercises the native MPSC memory
 * ordering without the CPython GIL serializing producers.  It is compiled in
 * an isolated Linux temporary directory; it is not a runtime component.
 */

#include "../fastspi_v2_ring.c"

#include <pthread.h>
#include <stdio.h>

#define STRESS_PRODUCERS 4
#define STRESS_RECORDS_PER_PRODUCER 4000
#define STRESS_CAPACITY (1 << 15)

typedef struct {
    NativeFastSpiV2Ring *ring;
    uint32_t producer_id;
    int failed;
} ProducerArgument;

static int initialize_ring(NativeFastSpiV2Ring *ring) {
    size_t index;
    ring->wakeup_fd = -1;
    ring->slots = PyMem_Calloc(STRESS_CAPACITY, sizeof(FastSpiV2Slot));
    if (ring->slots == NULL) return -1;
    ring->capacity = STRESS_CAPACITY;
    ring->mask = STRESS_CAPACITY - 1;
    for (index = 0; index < STRESS_CAPACITY; index++) {
        atomic_init(&ring->slots[index].sequence, index);
    }
    atomic_init(&ring->enqueue_position, 0);
    atomic_init(&ring->dequeue_position, 0);
    atomic_init(&ring->depth, 0);
    atomic_init(&ring->wakeup_pending, 0);
    atomic_init(&ring->high_watermark, 0);
    atomic_init(&ring->pushed_count, 0);
    atomic_init(&ring->popped_count, 0);
    atomic_init(&ring->overflow_count, 0);
    atomic_init(&ring->wakeup_count, 0);
    atomic_init(&ring->wakeup_failures, 0);
    ring->wakeup_fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    return ring->wakeup_fd < 0 ? -1 : 0;
}

static void *produce(void *opaque) {
    ProducerArgument *argument = opaque;
    uint64_t sequence;
    for (sequence = 1; sequence <= STRESS_RECORDS_PER_PRODUCER; sequence++) {
        FastSpiV2Record record = {
            .callback_kind = 1,
            .producer_id = argument->producer_id,
            .callback_seq = sequence,
            .observed_monotonic_ns = ((uint64_t) argument->producer_id << 32) | sequence,
            .order_emt_id = sequence,
        };
        if (!ring_try_push_record(argument->ring, &record)) {
            argument->failed = 1;
            return NULL;
        }
    }
    return NULL;
}

int main(void) {
    NativeFastSpiV2Ring ring = {0};
    ProducerArgument arguments[STRESS_PRODUCERS];
    pthread_t producers[STRESS_PRODUCERS];
    uint64_t last_sequences[STRESS_PRODUCERS] = {0};
    size_t index;
    size_t created = 0;
    size_t consumed = 0;
    int producer_failure = 0;
    int exit_code = 1;

    Py_Initialize();
    if (initialize_ring(&ring) != 0) {
        fprintf(stderr, "failed to initialize ring\n");
        goto cleanup_python;
    }
    for (index = 0; index < STRESS_PRODUCERS; index++) {
        arguments[index] = (ProducerArgument){
            .ring = &ring,
            .producer_id = (uint32_t) (index + 1),
            .failed = 0,
        };
        if (pthread_create(&producers[index], NULL, produce, &arguments[index]) != 0) {
            fprintf(stderr, "failed to create producer\n");
            producer_failure = 1;
            break;
        }
        created++;
    }
    for (index = 0; index < created; index++) {
        if (pthread_join(producers[index], NULL) != 0 || arguments[index].failed) producer_failure = 1;
    }
    if (created != STRESS_PRODUCERS || producer_failure) {
        fprintf(stderr, "producer failed or ring unexpectedly overflowed\n");
        goto cleanup_ring;
    }
    if (atomic_load_explicit(&ring.depth, memory_order_acquire)
        != STRESS_PRODUCERS * STRESS_RECORDS_PER_PRODUCER) {
        fprintf(stderr, "ring depth is inconsistent after MPSC producers\n");
        goto cleanup_ring;
    }
    while (consumed < STRESS_PRODUCERS * STRESS_RECORDS_PER_PRODUCER) {
        PyObject *raw = NativeFastSpiV2Ring_pop(&ring, NULL);
        unsigned long producer_id;
        unsigned long long sequence;
        if (raw == NULL || !PyTuple_Check(raw)) {
            Py_XDECREF(raw);
            fprintf(stderr, "consumer did not receive a scalar record\n");
            goto cleanup_ring;
        }
        producer_id = PyLong_AsUnsignedLong(PyTuple_GET_ITEM(raw, 1));
        sequence = PyLong_AsUnsignedLongLong(PyTuple_GET_ITEM(raw, 2));
        Py_DECREF(raw);
        if (
            PyErr_Occurred()
            || producer_id < 1
            || producer_id > STRESS_PRODUCERS
            || sequence != ++last_sequences[producer_id - 1]
        ) {
            PyErr_Clear();
            fprintf(stderr, "per-producer FIFO order was violated\n");
            goto cleanup_ring;
        }
        consumed++;
    }
    if (
        atomic_load_explicit(&ring.depth, memory_order_acquire) != 0
        || atomic_load_explicit(&ring.overflow_count, memory_order_acquire) != 0
        || atomic_load_explicit(&ring.pushed_count, memory_order_acquire) != consumed
        || atomic_load_explicit(&ring.popped_count, memory_order_acquire) != consumed
    ) {
        fprintf(stderr, "ring counters are inconsistent after consumer drain\n");
        goto cleanup_ring;
    }
    for (index = 0; index < STRESS_PRODUCERS; index++) {
        if (last_sequences[index] != STRESS_RECORDS_PER_PRODUCER) {
            fprintf(stderr, "a producer record was lost\n");
            goto cleanup_ring;
        }
    }
    puts("fastspi-v2-native-mpsc-stress: PASS");
    exit_code = 0;

cleanup_ring:
    if (ring.wakeup_fd >= 0) close(ring.wakeup_fd);
    PyMem_Free(ring.slots);
cleanup_python:
    Py_Finalize();
    return exit_code;
}
