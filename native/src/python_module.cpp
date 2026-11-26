#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include "xuntrade/kernels.hpp"

#include <exception>
#include <new>
#include <stdexcept>

namespace {
// Release the GIL only after inputs are copied/immutable. Destruction reacquires
// it before error translation or any CPython API access, even on C++ exceptions.
class AllowThreads {
    PyThreadState* state_ = PyEval_SaveThread();
public:
    ~AllowThreads() { PyEval_RestoreThread(state_); }
};

struct Owned {
    PyObject* value;
    ~Owned() { Py_XDECREF(value); }
};

void translate_exception() noexcept {
    try { throw; }
    catch (const std::bad_alloc&) { PyErr_NoMemory(); }
    catch (const std::invalid_argument& error) { PyErr_SetString(PyExc_ValueError, error.what()); }
    catch (const std::exception& error) { PyErr_SetString(PyExc_RuntimeError, error.what()); }
    catch (...) { PyErr_SetString(PyExc_RuntimeError, "native compute failed"); }
}

bool integer(PyObject* object, std::int64_t& value) {
    if (!PyLong_CheckExact(object)) {
        PyErr_SetString(PyExc_TypeError, "values must be builtin int (bool is forbidden)");
        return false;
    }
    value = PyLong_AsLongLong(object);
    return !PyErr_Occurred();
}

bool bounded_integer(PyObject* object, unsigned maximum, unsigned& value) {
    std::int64_t number;
    if (!integer(object, number)) return false;
    if (number < 1 || number > maximum) {
        PyErr_SetString(PyExc_ValueError, "count is outside the supported range");
        return false;
    }
    value = static_cast<unsigned>(number);
    return true;
}

bool sequence(PyObject* object) {
    if (!PyList_CheckExact(object) && !PyTuple_CheckExact(object)) {
        PyErr_SetString(PyExc_TypeError, "batch must be a list or tuple");
        return false;
    }
    if (PySequence_Size(object) > static_cast<Py_ssize_t>(xuntrade::maximum_batch_size)) {
        PyErr_SetString(PyExc_ValueError, "batch exceeds 1000000 rows");
        return false;
    }
    return true;
}

PyObject* checksum(PyObject*, PyObject* object) {
    // Exact immutable bytes avoid races with bytearray/mmap writers during GIL release.
    if (!PyBytes_CheckExact(object)) {
        PyErr_SetString(PyExc_TypeError, "crc32c requires immutable bytes");
        return nullptr;
    }
    const auto data = std::span(reinterpret_cast<const std::uint8_t*>(PyBytes_AS_STRING(object)),
                                static_cast<std::size_t>(PyBytes_GET_SIZE(object)));
    std::uint32_t result;
    if (data.size() >= 8192) {
        AllowThreads unlocked;
        result = xuntrade::crc32c(data);
    } else result = xuntrade::crc32c(data);
    return PyLong_FromUnsignedLong(result);
}

PyObject* shards(PyObject*, PyObject* args) {
    PyObject *ids, *count, *worker_count;
    if (!PyArg_ParseTuple(args, "OOO", &ids, &count, &worker_count)) return nullptr;
    unsigned shard_count, workers;
    if (!bounded_integer(count, 32, shard_count) ||
        !bounded_integer(worker_count, xuntrade::maximum_workers, workers) || !sequence(ids))
        return nullptr;
    try {
        // A tuple snapshot owns references while copying even if the caller used a list.
        Owned snapshot{PySequence_Tuple(ids)};
        if (!snapshot.value) return nullptr;
        const auto size = PyTuple_GET_SIZE(snapshot.value);
        // Conservative peak estimate includes UTF-8 cache, string copies,
        // vector storage, owned references and Python output containers.
        std::size_t estimated_bytes = static_cast<std::size_t>(size) * 96;
        for (Py_ssize_t i = 0; i < size; ++i) {
            auto* item = PyTuple_GET_ITEM(snapshot.value, i);
            if (!PyUnicode_CheckExact(item)) {
                PyErr_SetString(PyExc_TypeError, "execution IDs must be builtin str");
                return nullptr;
            }
            auto length = PyUnicode_GET_LENGTH(item);
            if (length == 0 || length > 256) {
                PyErr_SetString(PyExc_ValueError, "execution ID length must be in [1, 256]");
                return nullptr;
            }
            estimated_bytes += static_cast<std::size_t>(length) * 8;
        }
        xuntrade::ComputeLease lease(estimated_bytes, workers);
        std::vector<std::string> input;
        input.reserve(size);
        for (Py_ssize_t i = 0; i < size; ++i) {
            auto* item = PyTuple_GET_ITEM(snapshot.value, i);
            if (!PyUnicode_CheckExact(item)) {
                PyErr_SetString(PyExc_TypeError, "execution IDs must be builtin str");
                return nullptr;
            }
            // 256 Unicode code points match the existing domain's identifier budget.
            if (PyUnicode_GET_LENGTH(item) == 0 || PyUnicode_GET_LENGTH(item) > 256) {
                PyErr_SetString(PyExc_ValueError, "execution ID length must be in [1, 256]");
                return nullptr;
            }
            Py_ssize_t length;
            const char* utf8 = PyUnicode_AsUTF8AndSize(item, &length);
            if (!utf8) return nullptr;
            input.emplace_back(utf8, length);
        }
        std::vector<std::uint32_t> output;
        {
            AllowThreads unlocked;
            output = xuntrade::session_shards(input, shard_count, workers);
        }
        Owned result{PyTuple_New(size)};
        if (!result.value) return nullptr;
        for (Py_ssize_t i = 0; i < size; ++i) {
            auto* item = PyLong_FromUnsignedLong(output[i]);
            if (!item) return nullptr;
            PyTuple_SET_ITEM(result.value, i, item);
        }
        auto* returned = result.value;
        result.value = nullptr;
        return returned;
    } catch (...) { translate_exception(); return nullptr; }
}

PyObject* assessments(PyObject*, PyObject* args) {
    PyObject *rows, *worker_count;
    if (!PyArg_ParseTuple(args, "OO", &rows, &worker_count)) return nullptr;
    unsigned workers;
    if (!bounded_integer(worker_count, xuntrade::maximum_workers, workers) || !sequence(rows))
        return nullptr;
    try {
        Owned snapshot{PySequence_Tuple(rows)};
        if (!snapshot.value) return nullptr;
        const auto size = PyTuple_GET_SIZE(snapshot.value);
        xuntrade::ComputeLease lease(static_cast<std::size_t>(size) * 160, workers);
        std::vector<xuntrade::OrderInput> input;
        input.reserve(size);
        for (Py_ssize_t i = 0; i < size; ++i) {
            auto* row = PyTuple_GET_ITEM(snapshot.value, i);
            if (!PyTuple_CheckExact(row) || PyTuple_GET_SIZE(row) != 4) {
                PyErr_SetString(PyExc_TypeError, "each order must be a tuple of four int64 values");
                return nullptr;
            }
            xuntrade::OrderInput converted;
            if (!integer(PyTuple_GET_ITEM(row, 0), converted.quantity) ||
                !integer(PyTuple_GET_ITEM(row, 1), converted.price_minor) ||
                !integer(PyTuple_GET_ITEM(row, 2), converted.available_minor) ||
                !integer(PyTuple_GET_ITEM(row, 3), converted.order_limit_minor)) return nullptr;
            input.push_back(converted);
        }
        std::vector<xuntrade::Assessment> output;
        {
            AllowThreads unlocked;
            output = xuntrade::assess_orders(input, workers);
        }
        Owned result{PyTuple_New(size)};
        if (!result.value) return nullptr;
        for (Py_ssize_t i = 0; i < size; ++i) {
            auto* item = Py_BuildValue("Li", static_cast<long long>(output[i].notional_minor),
                                       static_cast<int>(output[i].code));
            if (!item) return nullptr;
            PyTuple_SET_ITEM(result.value, i, item);
        }
        auto* returned = result.value;
        result.value = nullptr;
        return returned;
    } catch (...) { translate_exception(); return nullptr; }
}

PyObject* backend_info(PyObject*, PyObject*) {
    const auto capacity = xuntrade::compute_capacity();
    return Py_BuildValue("{s:s,s:s,s:i,s:i,s:l,s:K,s:K,s:I,s:I,s:I,s:K}", "engine", "cpp20", "crc32c",
                        xuntrade::crc32c_implementation(), "api_version", 1,
                        "maximum_workers", xuntrade::maximum_workers,
                        "cxx_standard", static_cast<long>(__cplusplus),
                        "maximum_batch_bytes", static_cast<unsigned long long>(xuntrade::maximum_batch_bytes),
                        "maximum_compute_bytes", static_cast<unsigned long long>(xuntrade::maximum_compute_bytes),
                        "maximum_compute_calls", xuntrade::maximum_compute_calls,
                        "active_calls", capacity.calls, "active_workers", capacity.workers,
                        "active_bytes", static_cast<unsigned long long>(capacity.bytes));
}

PyMethodDef methods[]{
    {"crc32c", checksum, METH_O, "CRC32C Castagnoli of immutable bytes."},
    {"session_shards", shards, METH_VARARGS, "BLAKE2s-128 batch routing; releases the GIL."},
    {"assess_orders", assessments, METH_VARARGS, "Independent int64 order assessments."},
    {"backend_info", backend_info, METH_NOARGS, "Native engine capabilities."},
    {nullptr, nullptr, 0, nullptr}};
PyModuleDef module{PyModuleDef_HEAD_INIT, "_native", "XunTradeOMSEMS C++20 kernels.", -1, methods};
}  // namespace

PyMODINIT_FUNC PyInit__native() { return PyModule_Create(&module); }
