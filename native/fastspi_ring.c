#define PY_SSIZE_T_CLEAN
#include <Python.h>

typedef struct {
    PyObject_HEAD
    PyObject **items;
    Py_ssize_t capacity;
    Py_ssize_t head;
    Py_ssize_t tail;
    Py_ssize_t size;
} NativeMpscRing;

static int NativeMpscRing_init(NativeMpscRing *self, PyObject *args, PyObject *kwargs) {
    Py_ssize_t capacity = 0;
    static char *keywords[] = {"capacity", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "n", keywords, &capacity)) return -1;
    if (capacity <= 0) {
        PyErr_SetString(PyExc_ValueError, "capacity must be positive");
        return -1;
    }
    self->items = PyMem_Calloc((size_t) capacity, sizeof(PyObject *));
    if (self->items == NULL) return PyErr_NoMemory(), -1;
    self->capacity = capacity;
    self->head = self->tail = self->size = 0;
    return 0;
}

static void NativeMpscRing_dealloc(NativeMpscRing *self) {
    Py_ssize_t index;
    if (self->items != NULL) {
        for (index = 0; index < self->capacity; index++) Py_XDECREF(self->items[index]);
        PyMem_Free(self->items);
    }
    Py_TYPE(self)->tp_free((PyObject *) self);
}

static PyObject *NativeMpscRing_try_push(NativeMpscRing *self, PyObject *item) {
    if (self->size >= self->capacity) Py_RETURN_FALSE;
    Py_INCREF(item);
    self->items[self->tail] = item;
    self->tail = (self->tail + 1) % self->capacity;
    self->size++;
    Py_RETURN_TRUE;
}

static PyObject *NativeMpscRing_pop(NativeMpscRing *self, PyObject *ignored) {
    PyObject *item;
    if (self->size == 0) Py_RETURN_NONE;
    item = self->items[self->head];
    self->items[self->head] = NULL;
    self->head = (self->head + 1) % self->capacity;
    self->size--;
    return item;
}

static PyObject *NativeMpscRing_get_depth(NativeMpscRing *self, void *closure) {
    return PyLong_FromSsize_t(self->size);
}

static PyMethodDef NativeMpscRing_methods[] = {
    {"try_push", (PyCFunction) NativeMpscRing_try_push, METH_O, "Try to append one item."},
    {"pop", (PyCFunction) NativeMpscRing_pop, METH_NOARGS, "Pop one item or None."},
    {NULL, NULL, 0, NULL}
};

static PyGetSetDef NativeMpscRing_getset[] = {
    {"depth", (getter) NativeMpscRing_get_depth, NULL, "Current queue depth.", NULL},
    {NULL, NULL, NULL, NULL, NULL}
};

static PyTypeObject NativeMpscRingType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "emt_gateway._fastspi.NativeMpscRing",
    .tp_basicsize = sizeof(NativeMpscRing),
    .tp_dealloc = (destructor) NativeMpscRing_dealloc,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_doc = "Bounded native MPSC ring (serialized by the CPython GIL).",
    .tp_methods = NativeMpscRing_methods,
    .tp_getset = NativeMpscRing_getset,
    .tp_init = (initproc) NativeMpscRing_init,
    .tp_new = PyType_GenericNew,
};

static struct PyModuleDef module = {
    PyModuleDef_HEAD_INIT, "_fastspi", "Native bounded callback ring.", -1, NULL
};

PyMODINIT_FUNC PyInit__fastspi(void) {
    PyObject *module_object;
    if (PyType_Ready(&NativeMpscRingType) < 0) return NULL;
    module_object = PyModule_Create(&module);
    if (module_object == NULL) return NULL;
    Py_INCREF(&NativeMpscRingType);
    if (PyModule_AddObject(module_object, "NativeMpscRing", (PyObject *) &NativeMpscRingType) < 0) {
        Py_DECREF(&NativeMpscRingType);
        Py_DECREF(module_object);
        return NULL;
    }
    return module_object;
}
