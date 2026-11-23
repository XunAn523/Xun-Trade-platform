#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include <errno.h>
#include <math.h>
#include <stdint.h>
#include <stdatomic.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <poll.h>
#include <sys/eventfd.h>
#include <unistd.h>

#define FASTSPI_V2_MAX_ERROR_BYTES 192
#define FASTSPI_V2_MAX_EXECUTION_ID_BYTES 64
#define FASTSPI_V2_FLAG_ERROR_TRUNCATED (UINT32_C(1) << 4)

typedef struct {
    uint32_t callback_kind;
    uint32_t producer_id;
    uint64_t callback_seq;
    uint64_t observed_monotonic_ns;
    uint64_t client_id;
    uint64_t order_client_id;
    uint64_t order_emt_id;
    uint64_t order_cancel_emt_id;
    int32_t status;
    int64_t quantity;
    uint64_t trade_id_hash;
    uint32_t flags;
    uint16_t error_length;
    unsigned char error_bytes[FASTSPI_V2_MAX_ERROR_BYTES];
    uint64_t order_update_time;
    uint64_t qty_traded;
    uint64_t qty_left;
    int32_t trade_market;
    double trade_price;
    uint64_t trade_report_index;
    uint16_t trade_execution_id_length;
    unsigned char trade_execution_id[FASTSPI_V2_MAX_EXECUTION_ID_BYTES];
    uint64_t query_reference;
} FastSpiV2Record;

typedef struct {
    _Atomic size_t sequence;
    FastSpiV2Record record;
} FastSpiV2Slot;

typedef struct {
    PyObject_HEAD
    FastSpiV2Slot *slots;
    size_t capacity;
    size_t mask;
    _Alignas(64) _Atomic size_t enqueue_position;
    _Alignas(64) _Atomic size_t dequeue_position;
    _Atomic size_t depth;
    _Atomic int wakeup_pending;
    _Atomic size_t high_watermark;
    _Atomic unsigned long long pushed_count;
    _Atomic unsigned long long popped_count;
    _Atomic unsigned long long overflow_count;
    _Atomic unsigned long long wakeup_count;
    _Atomic unsigned long long wakeup_failures;
    int wakeup_fd;
} NativeFastSpiV2Ring;

static int is_power_of_two(size_t value) {
    return value >= 2 && (value & (value - 1)) == 0;
}

static int parse_unsigned(PyObject *value, unsigned long long maximum, unsigned long long *result, const char *name) {
    unsigned long long parsed;
    if (!PyLong_Check(value)) {
        PyErr_Format(PyExc_TypeError, "%s must be an integer", name);
        return -1;
    }
    parsed = PyLong_AsUnsignedLongLong(value);
    if (PyErr_Occurred()) return -1;
    if (parsed > maximum) {
        PyErr_Format(PyExc_ValueError, "%s is out of range", name);
        return -1;
    }
    *result = parsed;
    return 0;
}

static int parse_signed(PyObject *value, long long minimum, long long maximum, long long *result, const char *name) {
    long long parsed;
    if (!PyLong_Check(value)) {
        PyErr_Format(PyExc_TypeError, "%s must be an integer", name);
        return -1;
    }
    parsed = PyLong_AsLongLong(value);
    if (PyErr_Occurred()) return -1;
    if (parsed < minimum || parsed > maximum) {
        PyErr_Format(PyExc_ValueError, "%s is out of range", name);
        return -1;
    }
    *result = parsed;
    return 0;
}

static int parse_finite_price(PyObject *value, double *result, const char *name) {
    double parsed;
    if (PyBool_Check(value) || (!PyFloat_Check(value) && !PyLong_Check(value))) {
        PyErr_Format(PyExc_TypeError, "%s must be a finite numeric scalar", name);
        return -1;
    }
    parsed = PyFloat_AsDouble(value);
    if (PyErr_Occurred()) return -1;
    if (!isfinite(parsed)) {
        PyErr_Format(PyExc_ValueError, "%s must be finite", name);
        return -1;
    }
    *result = parsed;
    return 0;
}

static int utf8_codepoint_width(const unsigned char *value, size_t length, size_t *width) {
    unsigned char first;
    unsigned char second;
    if (length == 0) return -1;
    first = value[0];
    if (first <= 0x7f) {
        *width = 1;
        return 0;
    }
    if (first >= 0xc2 && first <= 0xdf) {
        if (length < 2 || (value[1] & 0xc0) != 0x80) return -1;
        *width = 2;
        return 0;
    }
    if (first >= 0xe0 && first <= 0xef) {
        if (length < 3 || (value[1] & 0xc0) != 0x80 || (value[2] & 0xc0) != 0x80) return -1;
        second = value[1];
        if ((first == 0xe0 && second < 0xa0) || (first == 0xed && second >= 0xa0)) return -1;
        *width = 3;
        return 0;
    }
    if (first >= 0xf0 && first <= 0xf4) {
        if (
            length < 4 || (value[1] & 0xc0) != 0x80 || (value[2] & 0xc0) != 0x80
            || (value[3] & 0xc0) != 0x80
        ) return -1;
        second = value[1];
        if ((first == 0xf0 && second < 0x90) || (first == 0xf4 && second >= 0x90)) return -1;
        *width = 4;
        return 0;
    }
    return -1;
}

static int copy_utf8_text(
    PyObject *value,
    unsigned char *target,
    uint16_t *target_length,
    size_t maximum_length,
    int truncate_to_capacity,
    int *truncated,
    const char *name
) {
    const char *error_bytes;
    char *raw_error_bytes;
    Py_ssize_t error_length;
    size_t source_length;
    size_t copy_length;
    size_t offset;
    size_t width;
    if (truncated != NULL) *truncated = 0;
    if (PyBytes_Check(value)) {
        if (PyBytes_AsStringAndSize(value, &raw_error_bytes, &error_length) < 0) return -1;
        error_bytes = raw_error_bytes;
    } else if (PyUnicode_Check(value)) {
        error_bytes = PyUnicode_AsUTF8AndSize(value, &error_length);
        if (error_bytes == NULL) return -1;
    } else {
        PyErr_Format(PyExc_TypeError, "%s must be UTF-8 bytes or text", name);
        return -1;
    }
    if (error_length < 0) {
        PyErr_Format(PyExc_ValueError, "%s has an invalid length", name);
        return -1;
    }
    source_length = (size_t) error_length;
    offset = 0;
    while (offset < source_length) {
        if (utf8_codepoint_width((const unsigned char *) error_bytes + offset, source_length - offset, &width) < 0) {
            PyErr_Format(PyExc_ValueError, "%s must be valid UTF-8", name);
            return -1;
        }
        offset += width;
    }
    if (source_length > maximum_length && !truncate_to_capacity) {
        PyErr_Format(PyExc_ValueError, "%s exceed fastspi v2 capacity", name);
        return -1;
    }
    copy_length = source_length;
    if (copy_length > maximum_length) {
        if (truncated != NULL) *truncated = 1;
        copy_length = 0;
        offset = 0;
        while (offset < source_length) {
            (void) utf8_codepoint_width(
                (const unsigned char *) error_bytes + offset,
                source_length - offset,
                &width
            );
            if (offset + width > maximum_length) break;
            offset += width;
            copy_length = offset;
        }
    }
    *target_length = (uint16_t) copy_length;
    memset(target, 0, maximum_length);
    if (copy_length > 0) memcpy(target, error_bytes, copy_length);
    return 0;
}

static int parse_record(PyObject *args, FastSpiV2Record *record) {
    PyObject *item;
    unsigned long long parsed_unsigned;
    long long parsed_signed;
    if (!PyTuple_Check(args) || PyTuple_GET_SIZE(args) != 21) {
        PyErr_SetString(PyExc_TypeError, "fastspi v2 try_push requires exactly 21 scalar fields");
        return -1;
    }
    item = PyTuple_GET_ITEM(args, 0);
    if (parse_unsigned(item, 0xffffULL, &parsed_unsigned, "callback_kind") < 0 || parsed_unsigned == 0) return -1;
    record->callback_kind = (uint32_t) parsed_unsigned;
    if (parse_unsigned(PyTuple_GET_ITEM(args, 1), 0xffffffffULL, &parsed_unsigned, "producer_id") < 0) return -1;
    record->producer_id = (uint32_t) parsed_unsigned;
    if (parse_unsigned(PyTuple_GET_ITEM(args, 2), UINT64_MAX, &parsed_unsigned, "callback_seq") < 0 || parsed_unsigned == 0) return -1;
    record->callback_seq = (uint64_t) parsed_unsigned;
    if (parse_unsigned(PyTuple_GET_ITEM(args, 3), UINT64_MAX, &parsed_unsigned, "observed_monotonic_ns") < 0) return -1;
    record->observed_monotonic_ns = (uint64_t) parsed_unsigned;
    if (parse_unsigned(PyTuple_GET_ITEM(args, 4), UINT64_MAX, &parsed_unsigned, "client_id") < 0) return -1;
    record->client_id = (uint64_t) parsed_unsigned;
    if (parse_unsigned(PyTuple_GET_ITEM(args, 5), UINT64_MAX, &parsed_unsigned, "order_client_id") < 0) return -1;
    record->order_client_id = (uint64_t) parsed_unsigned;
    if (parse_unsigned(PyTuple_GET_ITEM(args, 6), UINT64_MAX, &parsed_unsigned, "order_emt_id") < 0) return -1;
    record->order_emt_id = (uint64_t) parsed_unsigned;
    if (parse_unsigned(PyTuple_GET_ITEM(args, 7), UINT64_MAX, &parsed_unsigned, "order_cancel_emt_id") < 0) return -1;
    record->order_cancel_emt_id = (uint64_t) parsed_unsigned;
    if (parse_signed(PyTuple_GET_ITEM(args, 8), INT32_MIN, INT32_MAX, &parsed_signed, "status") < 0) return -1;
    record->status = (int32_t) parsed_signed;
    if (parse_signed(PyTuple_GET_ITEM(args, 9), INT64_MIN, INT64_MAX, &parsed_signed, "quantity") < 0) return -1;
    record->quantity = (int64_t) parsed_signed;
    if (parse_unsigned(PyTuple_GET_ITEM(args, 10), UINT64_MAX, &parsed_unsigned, "trade_id_hash") < 0) return -1;
    record->trade_id_hash = (uint64_t) parsed_unsigned;
    if (parse_unsigned(PyTuple_GET_ITEM(args, 11), 0xffffffffULL, &parsed_unsigned, "flags") < 0) return -1;
    record->flags = (uint32_t) parsed_unsigned;
    if (copy_utf8_text(
        PyTuple_GET_ITEM(args, 12), record->error_bytes, &record->error_length,
        FASTSPI_V2_MAX_ERROR_BYTES, 0, NULL, "error_bytes"
    ) < 0) return -1;
    if (parse_unsigned(PyTuple_GET_ITEM(args, 13), UINT64_MAX, &parsed_unsigned, "order_update_time") < 0) return -1;
    record->order_update_time = (uint64_t) parsed_unsigned;
    if (parse_unsigned(PyTuple_GET_ITEM(args, 14), UINT64_MAX, &parsed_unsigned, "qty_traded") < 0) return -1;
    record->qty_traded = (uint64_t) parsed_unsigned;
    if (parse_unsigned(PyTuple_GET_ITEM(args, 15), UINT64_MAX, &parsed_unsigned, "qty_left") < 0) return -1;
    record->qty_left = (uint64_t) parsed_unsigned;
    if (parse_signed(PyTuple_GET_ITEM(args, 16), INT32_MIN, INT32_MAX, &parsed_signed, "trade_market") < 0) return -1;
    record->trade_market = (int32_t) parsed_signed;
    if (parse_finite_price(PyTuple_GET_ITEM(args, 17), &record->trade_price, "trade_price") < 0) return -1;
    if (parse_unsigned(PyTuple_GET_ITEM(args, 18), UINT64_MAX, &parsed_unsigned, "trade_report_index") < 0) return -1;
    record->trade_report_index = (uint64_t) parsed_unsigned;
    if (copy_utf8_text(
        PyTuple_GET_ITEM(args, 19), record->trade_execution_id, &record->trade_execution_id_length,
        FASTSPI_V2_MAX_EXECUTION_ID_BYTES, 0, NULL, "trade_execution_id"
    ) < 0) return -1;
    if (parse_unsigned(PyTuple_GET_ITEM(args, 20), UINT64_MAX, &parsed_unsigned, "query_reference") < 0) return -1;
    record->query_reference = (uint64_t) parsed_unsigned;
    return 0;
}

static void update_high_watermark(NativeFastSpiV2Ring *self, size_t depth) {
    size_t current = atomic_load_explicit(&self->high_watermark, memory_order_relaxed);
    while (depth > current && !atomic_compare_exchange_weak_explicit(
        &self->high_watermark, &current, depth, memory_order_relaxed, memory_order_relaxed)) {}
}

/*
 * The primitive deliberately has no Python interactions.  It is shared by
 * the public test wrapper and the C5 ingress object below so the latter never
 * constructs a Python callback record on its producer path.
 */
static int ring_try_push_record(NativeFastSpiV2Ring *self, const FastSpiV2Record *record) {
    FastSpiV2Slot *slot;
    size_t position;
    size_t sequence;
    intptr_t difference;
    size_t depth;
    uint64_t one = 1;
    ssize_t write_result;
    position = atomic_load_explicit(&self->enqueue_position, memory_order_relaxed);
    for (;;) {
        slot = &self->slots[position & self->mask];
        sequence = atomic_load_explicit(&slot->sequence, memory_order_acquire);
        difference = (intptr_t) sequence - (intptr_t) position;
        if (difference == 0) {
            if (atomic_compare_exchange_weak_explicit(
                &self->enqueue_position, &position, position + 1,
                memory_order_relaxed, memory_order_relaxed)) break;
        } else if (difference < 0) {
            atomic_fetch_add_explicit(&self->overflow_count, 1, memory_order_relaxed);
            return 0;
        } else {
            position = atomic_load_explicit(&self->enqueue_position, memory_order_relaxed);
        }
    }
    /*
     * Count the reservation before publishing the slot.  A consumer can only
     * pop after the release store below, so this ordering prevents an
     * underflow when a native producer and consumer run without the GIL.
     */
    depth = atomic_fetch_add_explicit(&self->depth, 1, memory_order_relaxed) + 1;
    update_high_watermark(self, depth);
    slot->record = *record;
    atomic_store_explicit(&slot->sequence, position + 1, memory_order_release);
    atomic_fetch_add_explicit(&self->pushed_count, 1, memory_order_relaxed);
    /*
     * ``depth == 1`` is not a safe notification condition for MPSC: a second
     * producer could publish while the first producer is paused before its
     * eventfd write.  The independent latch guarantees one best-effort
     * empty-to-nonempty notification until the consumer drains it.
     */
    if (atomic_exchange_explicit(&self->wakeup_pending, 1, memory_order_acq_rel) == 0) {
        write_result = write(self->wakeup_fd, &one, sizeof(one));
        if (write_result == (ssize_t) sizeof(one)) {
            atomic_fetch_add_explicit(&self->wakeup_count, 1, memory_order_relaxed);
        } else if (write_result < 0 && errno != EAGAIN) {
            atomic_fetch_add_explicit(&self->wakeup_failures, 1, memory_order_relaxed);
        }
    }
    return 1;
}

static int NativeFastSpiV2Ring_init(NativeFastSpiV2Ring *self, PyObject *args, PyObject *kwargs) {
    Py_ssize_t capacity;
    Py_ssize_t index;
    static char *keywords[] = {"capacity", NULL};
    self->wakeup_fd = -1;
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "n", keywords, &capacity)) return -1;
    if (capacity < 2 || capacity > (1 << 20) || !is_power_of_two((size_t) capacity)) {
        PyErr_SetString(PyExc_ValueError, "fastspi v2 capacity must be a supported power of two");
        return -1;
    }
    self->slots = PyMem_Calloc((size_t) capacity, sizeof(FastSpiV2Slot));
    if (self->slots == NULL) return PyErr_NoMemory(), -1;
    self->capacity = (size_t) capacity;
    self->mask = self->capacity - 1;
    for (index = 0; index < capacity; index++) atomic_init(&self->slots[index].sequence, (size_t) index);
    atomic_init(&self->enqueue_position, 0);
    atomic_init(&self->dequeue_position, 0);
    atomic_init(&self->depth, 0);
    atomic_init(&self->wakeup_pending, 0);
    atomic_init(&self->high_watermark, 0);
    atomic_init(&self->pushed_count, 0);
    atomic_init(&self->popped_count, 0);
    atomic_init(&self->overflow_count, 0);
    atomic_init(&self->wakeup_count, 0);
    atomic_init(&self->wakeup_failures, 0);
    self->wakeup_fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (self->wakeup_fd < 0) {
        PyMem_Free(self->slots);
        self->slots = NULL;
        return PyErr_SetFromErrno(PyExc_OSError), -1;
    }
    return 0;
}

static void NativeFastSpiV2Ring_dealloc(NativeFastSpiV2Ring *self) {
    if (self->wakeup_fd >= 0) close(self->wakeup_fd);
    PyMem_Free(self->slots);
    Py_TYPE(self)->tp_free((PyObject *) self);
}

static PyObject *NativeFastSpiV2Ring_try_push(NativeFastSpiV2Ring *self, PyObject *args) {
    FastSpiV2Record record;
    if (parse_record(args, &record) < 0) return NULL;
    if (ring_try_push_record(self, &record)) Py_RETURN_TRUE;
    Py_RETURN_FALSE;
}

static PyObject *record_to_tuple(const FastSpiV2Record *record) {
    PyObject *result = PyTuple_New(21);
    PyObject *value;
    int index = 0;
    if (result == NULL) return NULL;
#define SET_RECORD_VALUE(expression) do { value = (expression); if (value == NULL) goto error; PyTuple_SET_ITEM(result, index++, value); } while (0)
    SET_RECORD_VALUE(PyLong_FromUnsignedLong(record->callback_kind));
    SET_RECORD_VALUE(PyLong_FromUnsignedLong(record->producer_id));
    SET_RECORD_VALUE(PyLong_FromUnsignedLongLong(record->callback_seq));
    SET_RECORD_VALUE(PyLong_FromUnsignedLongLong(record->observed_monotonic_ns));
    SET_RECORD_VALUE(PyLong_FromUnsignedLongLong(record->client_id));
    SET_RECORD_VALUE(PyLong_FromUnsignedLongLong(record->order_client_id));
    SET_RECORD_VALUE(PyLong_FromUnsignedLongLong(record->order_emt_id));
    SET_RECORD_VALUE(PyLong_FromUnsignedLongLong(record->order_cancel_emt_id));
    SET_RECORD_VALUE(PyLong_FromLong(record->status));
    SET_RECORD_VALUE(PyLong_FromLongLong(record->quantity));
    SET_RECORD_VALUE(PyLong_FromUnsignedLongLong(record->trade_id_hash));
    SET_RECORD_VALUE(PyLong_FromUnsignedLong(record->flags));
    SET_RECORD_VALUE(PyBytes_FromStringAndSize((const char *) record->error_bytes, record->error_length));
    SET_RECORD_VALUE(PyLong_FromUnsignedLongLong(record->order_update_time));
    SET_RECORD_VALUE(PyLong_FromUnsignedLongLong(record->qty_traded));
    SET_RECORD_VALUE(PyLong_FromUnsignedLongLong(record->qty_left));
    SET_RECORD_VALUE(PyLong_FromLong(record->trade_market));
    SET_RECORD_VALUE(PyFloat_FromDouble(record->trade_price));
    SET_RECORD_VALUE(PyLong_FromUnsignedLongLong(record->trade_report_index));
    SET_RECORD_VALUE(PyBytes_FromStringAndSize(
        (const char *) record->trade_execution_id, record->trade_execution_id_length
    ));
    SET_RECORD_VALUE(PyLong_FromUnsignedLongLong(record->query_reference));
#undef SET_RECORD_VALUE
    return result;
error:
    Py_DECREF(result);
    return NULL;
}

static PyObject *NativeFastSpiV2Ring_pop(NativeFastSpiV2Ring *self, PyObject *ignored) {
    FastSpiV2Slot *slot;
    FastSpiV2Record record;
    size_t position = atomic_load_explicit(&self->dequeue_position, memory_order_relaxed);
    size_t sequence;
    intptr_t difference;
    (void) ignored;
    for (;;) {
        slot = &self->slots[position & self->mask];
        sequence = atomic_load_explicit(&slot->sequence, memory_order_acquire);
        difference = (intptr_t) sequence - (intptr_t) (position + 1);
        if (difference == 0) {
            if (atomic_compare_exchange_weak_explicit(
                &self->dequeue_position, &position, position + 1,
                memory_order_relaxed, memory_order_relaxed)) break;
        } else if (difference < 0) {
            Py_RETURN_NONE;
        } else {
            position = atomic_load_explicit(&self->dequeue_position, memory_order_relaxed);
        }
    }
    record = slot->record;
    atomic_store_explicit(&slot->sequence, position + self->mask + 1, memory_order_release);
    atomic_fetch_sub_explicit(&self->depth, 1, memory_order_relaxed);
    atomic_fetch_add_explicit(&self->popped_count, 1, memory_order_relaxed);
    return record_to_tuple(&record);
}

static PyObject *NativeFastSpiV2Ring_wait_for_items(NativeFastSpiV2Ring *self, PyObject *args) {
    double timeout_seconds;
    struct pollfd descriptor;
    int milliseconds;
    int result;
    if (!PyArg_ParseTuple(args, "d", &timeout_seconds)) return NULL;
    if (timeout_seconds < 0.0 || timeout_seconds > 60.0) {
        PyErr_SetString(PyExc_ValueError, "fastspi v2 timeout is out of range");
        return NULL;
    }
    if (atomic_load_explicit(&self->depth, memory_order_acquire) > 0) Py_RETURN_TRUE;
    milliseconds = (int) (timeout_seconds * 1000.0 + 0.999);
    descriptor.fd = self->wakeup_fd;
    descriptor.events = POLLIN;
    descriptor.revents = 0;
    Py_BEGIN_ALLOW_THREADS
    result = poll(&descriptor, 1, milliseconds);
    Py_END_ALLOW_THREADS
    if (result < 0) return PyErr_SetFromErrno(PyExc_OSError);
    if (result == 0) Py_RETURN_FALSE;
    Py_RETURN_TRUE;
}

static PyObject *NativeFastSpiV2Ring_drain_wakeup(NativeFastSpiV2Ring *self, PyObject *ignored) {
    uint64_t value = 0;
    ssize_t result = read(self->wakeup_fd, &value, sizeof(value));
    (void) ignored;
    if (result == (ssize_t) sizeof(value)) {
        atomic_store_explicit(&self->wakeup_pending, 0, memory_order_release);
        return PyLong_FromUnsignedLongLong(value);
    }
    if (result < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
        atomic_store_explicit(&self->wakeup_pending, 0, memory_order_release);
        return PyLong_FromLong(0);
    }
    return PyErr_SetFromErrno(PyExc_OSError);
}

static PyObject *get_depth(NativeFastSpiV2Ring *self, void *closure) {
    (void) closure;
    return PyLong_FromSize_t(atomic_load_explicit(&self->depth, memory_order_relaxed));
}

static PyObject *get_high_watermark(NativeFastSpiV2Ring *self, void *closure) {
    (void) closure;
    return PyLong_FromSize_t(atomic_load_explicit(&self->high_watermark, memory_order_relaxed));
}

static PyObject *get_pushed_count(NativeFastSpiV2Ring *self, void *closure) {
    (void) closure;
    return PyLong_FromUnsignedLongLong(atomic_load_explicit(&self->pushed_count, memory_order_relaxed));
}

static PyObject *get_popped_count(NativeFastSpiV2Ring *self, void *closure) {
    (void) closure;
    return PyLong_FromUnsignedLongLong(atomic_load_explicit(&self->popped_count, memory_order_relaxed));
}

static PyObject *get_overflow_count(NativeFastSpiV2Ring *self, void *closure) {
    (void) closure;
    return PyLong_FromUnsignedLongLong(atomic_load_explicit(&self->overflow_count, memory_order_relaxed));
}

static PyObject *get_wakeup_count(NativeFastSpiV2Ring *self, void *closure) {
    (void) closure;
    return PyLong_FromUnsignedLongLong(atomic_load_explicit(&self->wakeup_count, memory_order_relaxed));
}

static PyObject *get_wakeup_failures(NativeFastSpiV2Ring *self, void *closure) {
    (void) closure;
    return PyLong_FromUnsignedLongLong(atomic_load_explicit(&self->wakeup_failures, memory_order_relaxed));
}

static PyObject *get_capacity(NativeFastSpiV2Ring *self, void *closure) {
    (void) closure;
    return PyLong_FromSize_t(self->capacity);
}

static PyObject *get_wakeup_fd(NativeFastSpiV2Ring *self, void *closure) {
    (void) closure;
    return PyLong_FromLong(self->wakeup_fd);
}

static PyMethodDef NativeFastSpiV2Ring_methods[] = {
    {"try_push", (PyCFunction) NativeFastSpiV2Ring_try_push, METH_VARARGS, "Append one fixed scalar record."},
    {"pop", (PyCFunction) NativeFastSpiV2Ring_pop, METH_NOARGS, "Pop one fixed record or None."},
    {"wait_for_items", (PyCFunction) NativeFastSpiV2Ring_wait_for_items, METH_VARARGS, "Wait on eventfd for a producer wakeup."},
    {"drain_wakeup", (PyCFunction) NativeFastSpiV2Ring_drain_wakeup, METH_NOARGS, "Drain one eventfd wakeup counter."},
    {NULL, NULL, 0, NULL}
};

static PyGetSetDef NativeFastSpiV2Ring_getset[] = {
    {"capacity", (getter) get_capacity, NULL, "Ring capacity.", NULL},
    {"depth", (getter) get_depth, NULL, "Current ring depth.", NULL},
    {"high_watermark", (getter) get_high_watermark, NULL, "Highest observed ring depth.", NULL},
    {"pushed_count", (getter) get_pushed_count, NULL, "Successful producer count.", NULL},
    {"popped_count", (getter) get_popped_count, NULL, "Consumer pop count.", NULL},
    {"overflow_count", (getter) get_overflow_count, NULL, "Rejected full-ring count.", NULL},
    {"wakeup_count", (getter) get_wakeup_count, NULL, "Empty-to-nonempty wakeup count.", NULL},
    {"wakeup_failures", (getter) get_wakeup_failures, NULL, "Unexpected eventfd write failures.", NULL},
    {"wakeup_fd", (getter) get_wakeup_fd, NULL, "Linux eventfd used by the consumer.", NULL},
    {NULL, NULL, NULL, NULL, NULL}
};

static PyTypeObject NativeFastSpiV2RingType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "xuanling_tradecore._fastspi_v2.NativeFastSpiV2Ring",
    .tp_basicsize = sizeof(NativeFastSpiV2Ring),
    .tp_dealloc = (destructor) NativeFastSpiV2Ring_dealloc,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_doc = "Linux C11 fixed-record MPSC callback ring with eventfd wakeup.",
    .tp_methods = NativeFastSpiV2Ring_methods,
    .tp_getset = NativeFastSpiV2Ring_getset,
    .tp_init = (initproc) NativeFastSpiV2Ring_init,
    .tp_new = PyType_GenericNew,
};

#define FASTSPI_V2_MAX_GAP_PRODUCERS 16
#define FASTSPI_V2_MAX_GAP_CALLBACK_KINDS 1024
#define FASTSPI_V2_GAP_KIND_WORDS (FASTSPI_V2_MAX_GAP_CALLBACK_KINDS / 64)

typedef struct {
    uint32_t producer_id;
    _Atomic int configured;
    _Atomic uint64_t first_sequence;
    _Atomic uint64_t last_sequence;
    _Atomic uint64_t rejected_count;
    _Atomic uint64_t first_observed_ns;
    _Atomic uint64_t last_observed_ns;
} FastSpiV2GapProducer;

typedef struct {
    PyObject_HEAD
    size_t maximum_producers;
    size_t maximum_callback_kinds;
    _Atomic int active;
    _Atomic int registration_failure;
    FastSpiV2GapProducer producers[FASTSPI_V2_MAX_GAP_PRODUCERS];
    _Atomic uint64_t callback_kind_words[FASTSPI_V2_GAP_KIND_WORDS];
} NativeFastSpiV2GapLatch;

static void atomic_set_min_nonzero(_Atomic uint64_t *target, uint64_t value) {
    uint64_t current = atomic_load_explicit(target, memory_order_relaxed);
    while ((current == 0 || value < current) && !atomic_compare_exchange_weak_explicit(
        target, &current, value, memory_order_relaxed, memory_order_relaxed)) {}
}

static void atomic_set_max(_Atomic uint64_t *target, uint64_t value) {
    uint64_t current = atomic_load_explicit(target, memory_order_relaxed);
    while (value > current && !atomic_compare_exchange_weak_explicit(
        target, &current, value, memory_order_relaxed, memory_order_relaxed)) {}
}

static int native_gap_latch_record(
    NativeFastSpiV2GapLatch *self,
    uint32_t producer_id,
    uint64_t callback_seq,
    uint32_t callback_kind,
    uint64_t observed_ns
) {
    size_t index;
    FastSpiV2GapProducer *matched = NULL;
    if (
        callback_seq == 0
        || callback_kind == 0
        || callback_kind > self->maximum_callback_kinds
    ) {
        atomic_store_explicit(&self->registration_failure, 1, memory_order_release);
        atomic_store_explicit(&self->active, 1, memory_order_release);
        return 0;
    }
    for (index = 0; index < self->maximum_producers; index++) {
        FastSpiV2GapProducer *producer = &self->producers[index];
        if (
            atomic_load_explicit(&producer->configured, memory_order_acquire) == 1
            && producer->producer_id == producer_id
        ) {
            matched = producer;
            break;
        }
    }
    if (matched == NULL) {
        atomic_store_explicit(&self->registration_failure, 1, memory_order_release);
        atomic_store_explicit(&self->active, 1, memory_order_release);
        return 0;
    }
    atomic_set_min_nonzero(&matched->first_sequence, callback_seq);
    atomic_set_max(&matched->last_sequence, callback_seq);
    atomic_fetch_add_explicit(&matched->rejected_count, 1, memory_order_relaxed);
    atomic_set_min_nonzero(&matched->first_observed_ns, observed_ns);
    atomic_set_max(&matched->last_observed_ns, observed_ns);
    atomic_fetch_or_explicit(
        &self->callback_kind_words[(callback_kind - 1) / 64],
        UINT64_C(1) << ((callback_kind - 1) % 64),
        memory_order_relaxed
    );
    atomic_store_explicit(&self->active, 1, memory_order_release);
    return 1;
}

static int NativeFastSpiV2GapLatch_init(
    NativeFastSpiV2GapLatch *self,
    PyObject *args,
    PyObject *kwargs
) {
    Py_ssize_t maximum_producers = FASTSPI_V2_MAX_GAP_PRODUCERS;
    Py_ssize_t maximum_callback_kinds = FASTSPI_V2_MAX_GAP_CALLBACK_KINDS;
    Py_ssize_t index;
    static char *keywords[] = {"maximum_producers", "maximum_callback_kinds", NULL};
    if (!PyArg_ParseTupleAndKeywords(
        args, kwargs, "|nn", keywords, &maximum_producers, &maximum_callback_kinds
    )) return -1;
    if (
        maximum_producers < 1 || maximum_producers > FASTSPI_V2_MAX_GAP_PRODUCERS
        || maximum_callback_kinds < 1 || maximum_callback_kinds > FASTSPI_V2_MAX_GAP_CALLBACK_KINDS
    ) {
        PyErr_SetString(PyExc_ValueError, "fastspi v2 native gap latch bounds are invalid");
        return -1;
    }
    self->maximum_producers = (size_t) maximum_producers;
    self->maximum_callback_kinds = (size_t) maximum_callback_kinds;
    atomic_init(&self->active, 0);
    atomic_init(&self->registration_failure, 0);
    for (index = 0; index < FASTSPI_V2_MAX_GAP_PRODUCERS; index++) {
        self->producers[index].producer_id = 0;
        atomic_init(&self->producers[index].configured, 0);
        atomic_init(&self->producers[index].first_sequence, 0);
        atomic_init(&self->producers[index].last_sequence, 0);
        atomic_init(&self->producers[index].rejected_count, 0);
        atomic_init(&self->producers[index].first_observed_ns, 0);
        atomic_init(&self->producers[index].last_observed_ns, 0);
    }
    for (index = 0; index < FASTSPI_V2_GAP_KIND_WORDS; index++) {
        atomic_init(&self->callback_kind_words[index], 0);
    }
    return 0;
}

static PyObject *NativeFastSpiV2GapLatch_register_producer(
    NativeFastSpiV2GapLatch *self,
    PyObject *args
) {
    PyObject *value;
    unsigned long long parsed;
    size_t index;
    if (!PyArg_ParseTuple(args, "O", &value)) return NULL;
    if (parse_unsigned(value, 0xffffffffULL, &parsed, "producer_id") < 0) return NULL;
    for (index = 0; index < self->maximum_producers; index++) {
        FastSpiV2GapProducer *producer = &self->producers[index];
        int expected = 0;
        if (
            atomic_load_explicit(&producer->configured, memory_order_acquire) == 1
            && producer->producer_id == (uint32_t) parsed
        ) Py_RETURN_TRUE;
        if (atomic_compare_exchange_strong_explicit(
            &producer->configured, &expected, 2, memory_order_acq_rel, memory_order_relaxed
        )) {
            producer->producer_id = (uint32_t) parsed;
            atomic_store_explicit(&producer->configured, 1, memory_order_release);
            Py_RETURN_TRUE;
        }
    }
    atomic_store_explicit(&self->registration_failure, 1, memory_order_release);
    atomic_store_explicit(&self->active, 1, memory_order_release);
    Py_RETURN_FALSE;
}

static PyObject *NativeFastSpiV2GapLatch_record_rejection(
    NativeFastSpiV2GapLatch *self,
    PyObject *args
) {
    PyObject *producer_value;
    PyObject *sequence_value;
    PyObject *kind_value;
    PyObject *observed_value;
    unsigned long long producer_id;
    unsigned long long callback_seq;
    unsigned long long callback_kind;
    unsigned long long observed_ns;
    if (!PyArg_ParseTuple(args, "OOOO", &producer_value, &sequence_value, &kind_value, &observed_value)) {
        return NULL;
    }
    if (
        parse_unsigned(producer_value, 0xffffffffULL, &producer_id, "producer_id") < 0
        || parse_unsigned(sequence_value, UINT64_MAX, &callback_seq, "callback_seq") < 0
        || parse_unsigned(kind_value, 0xffffULL, &callback_kind, "callback_kind") < 0
        || parse_unsigned(observed_value, UINT64_MAX, &observed_ns, "observed_monotonic_ns") < 0
    ) return NULL;
    if (native_gap_latch_record(
        self,
        (uint32_t) producer_id,
        (uint64_t) callback_seq,
        (uint32_t) callback_kind,
        (uint64_t) observed_ns
    )) Py_RETURN_TRUE;
    Py_RETURN_FALSE;
}

static PyObject *NativeFastSpiV2GapLatch_snapshot(
    NativeFastSpiV2GapLatch *self,
    PyObject *ignored
) {
    PyObject *producers;
    PyObject *kinds;
    PyObject *result;
    size_t producer_count = 0;
    size_t kind_count = 0;
    size_t index;
    size_t output_index;
    (void) ignored;
    for (index = 0; index < self->maximum_producers; index++) {
        if (
            atomic_load_explicit(&self->producers[index].configured, memory_order_acquire) == 1
            && atomic_load_explicit(&self->producers[index].rejected_count, memory_order_acquire) > 0
        ) {
            producer_count++;
        }
    }
    for (index = 0; index < self->maximum_callback_kinds; index++) {
        uint64_t word = atomic_load_explicit(
            &self->callback_kind_words[index / 64], memory_order_acquire
        );
        if (word & (UINT64_C(1) << (index % 64))) kind_count++;
    }
    producers = PyTuple_New((Py_ssize_t) producer_count);
    if (producers == NULL) return NULL;
    output_index = 0;
    for (index = 0; index < self->maximum_producers; index++) {
        FastSpiV2GapProducer *producer = &self->producers[index];
        PyObject *entry;
        if (
            atomic_load_explicit(&producer->configured, memory_order_acquire) != 1
            || atomic_load_explicit(&producer->rejected_count, memory_order_acquire) == 0
        ) continue;
        entry = Py_BuildValue(
            "(IKKKKK)",
            producer->producer_id,
            (unsigned long long) atomic_load_explicit(&producer->first_sequence, memory_order_acquire),
            (unsigned long long) atomic_load_explicit(&producer->last_sequence, memory_order_acquire),
            (unsigned long long) atomic_load_explicit(&producer->rejected_count, memory_order_acquire),
            (unsigned long long) atomic_load_explicit(&producer->first_observed_ns, memory_order_acquire),
            (unsigned long long) atomic_load_explicit(&producer->last_observed_ns, memory_order_acquire)
        );
        if (entry == NULL) {
            Py_DECREF(producers);
            return NULL;
        }
        PyTuple_SET_ITEM(producers, (Py_ssize_t) output_index++, entry);
    }
    kinds = PyTuple_New((Py_ssize_t) kind_count);
    if (kinds == NULL) {
        Py_DECREF(producers);
        return NULL;
    }
    output_index = 0;
    for (index = 0; index < self->maximum_callback_kinds; index++) {
        uint64_t word = atomic_load_explicit(
            &self->callback_kind_words[index / 64], memory_order_acquire
        );
        if (word & (UINT64_C(1) << (index % 64))) {
            PyObject *kind = PyLong_FromSize_t(index + 1);
            if (kind == NULL) {
                Py_DECREF(kinds);
                Py_DECREF(producers);
                return NULL;
            }
            PyTuple_SET_ITEM(kinds, (Py_ssize_t) output_index++, kind);
        }
    }
    result = PyTuple_New(4);
    if (result == NULL) {
        Py_DECREF(kinds);
        Py_DECREF(producers);
        return NULL;
    }
    PyTuple_SET_ITEM(
        result,
        0,
        PyBool_FromLong(atomic_load_explicit(&self->active, memory_order_acquire))
    );
    PyTuple_SET_ITEM(
        result,
        1,
        PyBool_FromLong(atomic_load_explicit(&self->registration_failure, memory_order_acquire))
    );
    PyTuple_SET_ITEM(result, 2, kinds);
    PyTuple_SET_ITEM(result, 3, producers);
    return result;
}

static PyObject *get_gap_active(NativeFastSpiV2GapLatch *self, void *closure) {
    (void) closure;
    return PyBool_FromLong(atomic_load_explicit(&self->active, memory_order_acquire));
}

static PyObject *get_gap_registration_failure(NativeFastSpiV2GapLatch *self, void *closure) {
    (void) closure;
    return PyBool_FromLong(atomic_load_explicit(&self->registration_failure, memory_order_acquire));
}

static PyMethodDef NativeFastSpiV2GapLatch_methods[] = {
    {"register_producer", (PyCFunction) NativeFastSpiV2GapLatch_register_producer, METH_VARARGS, "Register one fixed producer before callbacks start."},
    {"record_rejection", (PyCFunction) NativeFastSpiV2GapLatch_record_rejection, METH_VARARGS, "Atomically latch one rejected callback."},
    {"snapshot", (PyCFunction) NativeFastSpiV2GapLatch_snapshot, METH_NOARGS, "Return fixed scalar gap evidence."},
    {NULL, NULL, 0, NULL}
};

static PyGetSetDef NativeFastSpiV2GapLatch_getset[] = {
    {"active", (getter) get_gap_active, NULL, "Whether callback admission is closed.", NULL},
    {"registration_failure", (getter) get_gap_registration_failure, NULL, "Whether a fixed registration bound failed.", NULL},
    {NULL, NULL, NULL, NULL, NULL}
};

static PyTypeObject NativeFastSpiV2GapLatchType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "xuanling_tradecore._fastspi_v2.NativeFastSpiV2GapLatch",
    .tp_basicsize = sizeof(NativeFastSpiV2GapLatch),
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_doc = "Linux C11 bounded atomic callback-gap latch.",
    .tp_methods = NativeFastSpiV2GapLatch_methods,
    .tp_getset = NativeFastSpiV2GapLatch_getset,
    .tp_init = (initproc) NativeFastSpiV2GapLatch_init,
    .tp_new = PyType_GenericNew,
};

typedef struct {
    PyObject_HEAD
    NativeFastSpiV2Ring *ring;
    NativeFastSpiV2GapLatch *gap_latch;
    uint32_t producer_id;
    _Atomic uint64_t callback_sequence;
    _Atomic int disarmed;
    _Atomic int callback_gap_latch_failed;
} NativeFastSpiV2Ingress;

static int native_gap_latch_has_producer(
    NativeFastSpiV2GapLatch *self,
    uint32_t producer_id
) {
    size_t index;
    for (index = 0; index < self->maximum_producers; index++) {
        FastSpiV2GapProducer *producer = &self->producers[index];
        if (
            atomic_load_explicit(&producer->configured, memory_order_acquire) == 1
            && producer->producer_id == producer_id
        ) return 1;
    }
    return 0;
}

static int parse_ingress_record(
    PyObject *args,
    uint32_t producer_id,
    uint64_t callback_seq,
    FastSpiV2Record *record
) {
    unsigned long long parsed_unsigned;
    long long parsed_signed;
    PyObject *error_value;
    int error_truncated;
    if (!PyTuple_Check(args) || PyTuple_GET_SIZE(args) != 19) {
        PyErr_SetString(PyExc_TypeError, "fastspi v2 ingress offer requires exactly 19 scalar fields");
        return -1;
    }
    if (parse_unsigned(PyTuple_GET_ITEM(args, 0), 0xffffULL, &parsed_unsigned, "callback_kind") < 0 || parsed_unsigned == 0) return -1;
    record->callback_kind = (uint32_t) parsed_unsigned;
    record->producer_id = producer_id;
    record->callback_seq = callback_seq;
    if (parse_unsigned(PyTuple_GET_ITEM(args, 1), UINT64_MAX, &parsed_unsigned, "observed_monotonic_ns") < 0) return -1;
    record->observed_monotonic_ns = (uint64_t) parsed_unsigned;
    if (parse_unsigned(PyTuple_GET_ITEM(args, 2), UINT64_MAX, &parsed_unsigned, "client_id") < 0) return -1;
    record->client_id = (uint64_t) parsed_unsigned;
    if (parse_unsigned(PyTuple_GET_ITEM(args, 3), UINT64_MAX, &parsed_unsigned, "order_client_id") < 0) return -1;
    record->order_client_id = (uint64_t) parsed_unsigned;
    if (parse_unsigned(PyTuple_GET_ITEM(args, 4), UINT64_MAX, &parsed_unsigned, "order_emt_id") < 0) return -1;
    record->order_emt_id = (uint64_t) parsed_unsigned;
    if (parse_unsigned(PyTuple_GET_ITEM(args, 5), UINT64_MAX, &parsed_unsigned, "order_cancel_emt_id") < 0) return -1;
    record->order_cancel_emt_id = (uint64_t) parsed_unsigned;
    if (parse_signed(PyTuple_GET_ITEM(args, 6), INT32_MIN, INT32_MAX, &parsed_signed, "status") < 0) return -1;
    record->status = (int32_t) parsed_signed;
    if (parse_signed(PyTuple_GET_ITEM(args, 7), INT64_MIN, INT64_MAX, &parsed_signed, "quantity") < 0) return -1;
    record->quantity = (int64_t) parsed_signed;
    if (parse_unsigned(PyTuple_GET_ITEM(args, 8), UINT64_MAX, &parsed_unsigned, "trade_id_hash") < 0) return -1;
    record->trade_id_hash = (uint64_t) parsed_unsigned;
    if (parse_unsigned(PyTuple_GET_ITEM(args, 9), 0xffffffffULL, &parsed_unsigned, "flags") < 0) return -1;
    record->flags = (uint32_t) parsed_unsigned;
    error_value = PyTuple_GET_ITEM(args, 10);
    if (copy_utf8_text(
        error_value, record->error_bytes, &record->error_length, FASTSPI_V2_MAX_ERROR_BYTES,
        1, &error_truncated, "fastspi v2 ingress error_bytes"
    ) < 0) return -1;
    if (error_truncated) record->flags |= FASTSPI_V2_FLAG_ERROR_TRUNCATED;
    if (parse_unsigned(PyTuple_GET_ITEM(args, 11), UINT64_MAX, &parsed_unsigned, "order_update_time") < 0) return -1;
    record->order_update_time = (uint64_t) parsed_unsigned;
    if (parse_unsigned(PyTuple_GET_ITEM(args, 12), UINT64_MAX, &parsed_unsigned, "qty_traded") < 0) return -1;
    record->qty_traded = (uint64_t) parsed_unsigned;
    if (parse_unsigned(PyTuple_GET_ITEM(args, 13), UINT64_MAX, &parsed_unsigned, "qty_left") < 0) return -1;
    record->qty_left = (uint64_t) parsed_unsigned;
    if (parse_signed(PyTuple_GET_ITEM(args, 14), INT32_MIN, INT32_MAX, &parsed_signed, "trade_market") < 0) return -1;
    record->trade_market = (int32_t) parsed_signed;
    if (parse_finite_price(PyTuple_GET_ITEM(args, 15), &record->trade_price, "trade_price") < 0) return -1;
    if (parse_unsigned(PyTuple_GET_ITEM(args, 16), UINT64_MAX, &parsed_unsigned, "trade_report_index") < 0) return -1;
    record->trade_report_index = (uint64_t) parsed_unsigned;
    if (copy_utf8_text(
        PyTuple_GET_ITEM(args, 17), record->trade_execution_id, &record->trade_execution_id_length,
        FASTSPI_V2_MAX_EXECUTION_ID_BYTES, 0, NULL, "fastspi v2 ingress trade_execution_id"
    ) < 0) return -1;
    if (parse_unsigned(PyTuple_GET_ITEM(args, 18), UINT64_MAX, &parsed_unsigned, "query_reference") < 0) return -1;
    record->query_reference = (uint64_t) parsed_unsigned;
    return 0;
}

static int NativeFastSpiV2Ingress_init(
    NativeFastSpiV2Ingress *self,
    PyObject *args,
    PyObject *kwargs
) {
    PyObject *ring_object;
    PyObject *gap_latch_object;
    unsigned int producer_id;
    static char *keywords[] = {"ring", "gap_latch", "producer_id", NULL};
    if (!PyArg_ParseTupleAndKeywords(
        args, kwargs, "OOI", keywords, &ring_object, &gap_latch_object, &producer_id
    )) return -1;
    if (!PyObject_TypeCheck(ring_object, &NativeFastSpiV2RingType)) {
        PyErr_SetString(PyExc_TypeError, "fastspi v2 ingress requires a native ring");
        return -1;
    }
    if (!PyObject_TypeCheck(gap_latch_object, &NativeFastSpiV2GapLatchType)) {
        PyErr_SetString(PyExc_TypeError, "fastspi v2 ingress requires a native gap latch");
        return -1;
    }
    self->ring = (NativeFastSpiV2Ring *) ring_object;
    self->gap_latch = (NativeFastSpiV2GapLatch *) gap_latch_object;
    if (!native_gap_latch_has_producer(self->gap_latch, (uint32_t) producer_id)) {
        PyErr_SetString(PyExc_ValueError, "fastspi v2 ingress producer is not registered in its gap latch");
        return -1;
    }
    self->producer_id = (uint32_t) producer_id;
    Py_INCREF(ring_object);
    Py_INCREF(gap_latch_object);
    atomic_init(&self->callback_sequence, 0);
    atomic_init(&self->disarmed, 0);
    atomic_init(&self->callback_gap_latch_failed, 0);
    return 0;
}

static void NativeFastSpiV2Ingress_dealloc(NativeFastSpiV2Ingress *self) {
    Py_XDECREF((PyObject *) self->ring);
    Py_XDECREF((PyObject *) self->gap_latch);
    Py_TYPE(self)->tp_free((PyObject *) self);
}

static PyObject *NativeFastSpiV2Ingress_offer(NativeFastSpiV2Ingress *self, PyObject *args) {
    FastSpiV2Record record;
    uint32_t callback_kind;
    uint64_t observed_ns;
    if (parse_ingress_record(args, self->producer_id, 0, &record) < 0) return NULL;
    callback_kind = record.callback_kind;
    observed_ns = record.observed_monotonic_ns;
    uint64_t previous_sequence = atomic_fetch_add_explicit(
        &self->callback_sequence, 1, memory_order_relaxed
    );
    uint64_t callback_seq = previous_sequence + 1;
    if (previous_sequence == UINT64_MAX || callback_seq == 0) {
        atomic_store_explicit(&self->disarmed, 1, memory_order_release);
        atomic_store_explicit(&self->callback_gap_latch_failed, 1, memory_order_release);
        Py_RETURN_FALSE;
    }
    record.callback_seq = callback_seq;
    if (
        atomic_load_explicit(&self->disarmed, memory_order_acquire)
        || atomic_load_explicit(&self->callback_gap_latch_failed, memory_order_acquire)
        || atomic_load_explicit(&self->gap_latch->active, memory_order_acquire)
    ) {
        atomic_store_explicit(&self->disarmed, 1, memory_order_release);
        if (
            !atomic_load_explicit(&self->callback_gap_latch_failed, memory_order_acquire)
            && !native_gap_latch_record(
                self->gap_latch,
                self->producer_id,
                callback_seq,
                callback_kind,
                observed_ns
            )
        ) {
            atomic_store_explicit(&self->callback_gap_latch_failed, 1, memory_order_release);
        }
        Py_RETURN_FALSE;
    }
    if (ring_try_push_record(self->ring, &record)) Py_RETURN_TRUE;
    atomic_store_explicit(&self->disarmed, 1, memory_order_release);
    if (!native_gap_latch_record(
        self->gap_latch,
        self->producer_id,
        callback_seq,
        callback_kind,
        observed_ns
    )) {
        atomic_store_explicit(&self->callback_gap_latch_failed, 1, memory_order_release);
    }
    Py_RETURN_FALSE;
}

static PyObject *NativeFastSpiV2Ingress_force_gap(NativeFastSpiV2Ingress *self, PyObject *args) {
    PyObject *kind_value;
    PyObject *observed_value;
    unsigned long long callback_kind;
    unsigned long long observed_ns;
    uint64_t previous_sequence;
    uint64_t callback_seq;
    if (!PyArg_ParseTuple(args, "OO", &kind_value, &observed_value)) return NULL;
    if (
        parse_unsigned(kind_value, 0xffffULL, &callback_kind, "callback_kind") < 0
        || parse_unsigned(observed_value, UINT64_MAX, &observed_ns, "observed_monotonic_ns") < 0
    ) return NULL;
    previous_sequence = atomic_fetch_add_explicit(
        &self->callback_sequence, 1, memory_order_relaxed
    );
    callback_seq = previous_sequence + 1;
    atomic_store_explicit(&self->disarmed, 1, memory_order_release);
    if (
        previous_sequence == UINT64_MAX
        || callback_seq == 0
        || !native_gap_latch_record(
            self->gap_latch,
            self->producer_id,
            callback_seq,
            (uint32_t) callback_kind,
            (uint64_t) observed_ns
        )
    ) {
        atomic_store_explicit(&self->callback_gap_latch_failed, 1, memory_order_release);
    }
    Py_RETURN_FALSE;
}

static PyObject *get_ingress_disarmed(NativeFastSpiV2Ingress *self, void *closure) {
    (void) closure;
    return PyBool_FromLong(atomic_load_explicit(&self->disarmed, memory_order_acquire));
}

static PyObject *get_ingress_callback_gap_latch_failed(
    NativeFastSpiV2Ingress *self,
    void *closure
) {
    (void) closure;
    return PyBool_FromLong(
        atomic_load_explicit(&self->callback_gap_latch_failed, memory_order_acquire)
    );
}

static PyMethodDef NativeFastSpiV2Ingress_methods[] = {
    {"offer", (PyCFunction) NativeFastSpiV2Ingress_offer, METH_VARARGS, "Offer one C5 scalar record once."},
    {"force_gap", (PyCFunction) NativeFastSpiV2Ingress_force_gap, METH_VARARGS, "Fail closed after an unexpected callback-boundary failure."},
    {NULL, NULL, 0, NULL}
};

static PyGetSetDef NativeFastSpiV2Ingress_getset[] = {
    {"disarmed", (getter) get_ingress_disarmed, NULL, "Whether overflow closed local admission.", NULL},
    {"callback_gap_latch_failed", (getter) get_ingress_callback_gap_latch_failed, NULL, "Whether fixed native gap recording failed.", NULL},
    {NULL, NULL, NULL, NULL, NULL}
};

static PyTypeObject NativeFastSpiV2IngressType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "xuanling_tradecore._fastspi_v2.NativeFastSpiV2Ingress",
    .tp_basicsize = sizeof(NativeFastSpiV2Ingress),
    .tp_dealloc = (destructor) NativeFastSpiV2Ingress_dealloc,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_doc = "C5 scalar fastspi v2 ingress with atomic callback-gap latch.",
    .tp_methods = NativeFastSpiV2Ingress_methods,
    .tp_getset = NativeFastSpiV2Ingress_getset,
    .tp_init = (initproc) NativeFastSpiV2Ingress_init,
    .tp_new = PyType_GenericNew,
};

static struct PyModuleDef module = {
    PyModuleDef_HEAD_INIT,
    "_fastspi_v2",
    "Linux fixed-record fastspi v2 ring.",
    -1,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
};

PyMODINIT_FUNC PyInit__fastspi_v2(void) {
    PyObject *module_object;
    if (PyType_Ready(&NativeFastSpiV2RingType) < 0) return NULL;
    if (PyType_Ready(&NativeFastSpiV2GapLatchType) < 0) return NULL;
    if (PyType_Ready(&NativeFastSpiV2IngressType) < 0) return NULL;
    module_object = PyModule_Create(&module);
    if (module_object == NULL) return NULL;
    Py_INCREF(&NativeFastSpiV2RingType);
    if (PyModule_AddObject(module_object, "NativeFastSpiV2Ring", (PyObject *) &NativeFastSpiV2RingType) < 0) {
        Py_DECREF(&NativeFastSpiV2RingType);
        Py_DECREF(module_object);
        return NULL;
    }
    Py_INCREF(&NativeFastSpiV2GapLatchType);
    if (PyModule_AddObject(
        module_object,
        "NativeFastSpiV2GapLatch",
        (PyObject *) &NativeFastSpiV2GapLatchType
    ) < 0) {
        Py_DECREF(&NativeFastSpiV2GapLatchType);
        Py_DECREF(module_object);
        return NULL;
    }
    Py_INCREF(&NativeFastSpiV2IngressType);
    if (PyModule_AddObject(
        module_object,
        "NativeFastSpiV2Ingress",
        (PyObject *) &NativeFastSpiV2IngressType
    ) < 0) {
        Py_DECREF(&NativeFastSpiV2IngressType);
        Py_DECREF(module_object);
        return NULL;
    }
    return module_object;
}
