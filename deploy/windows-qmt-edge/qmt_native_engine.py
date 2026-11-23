# -*- coding: utf-8 -*-
"""Big QMT Python 3.6 side of the TradeCore QMT Adapter Path.

This model opens pre-created local rings once.  Its timer and callbacks only
read/write fixed binary frames and call documented QMT functions.  It has no
network, SQLite, OMS, EMS, logging-file, directory-scanning or strategy code.
It starts disarmed and requires a reviewed P0 configuration before mutation.
"""

from __future__ import print_function

import ctypes
import hashlib
import json
import mmap
import os
import struct
import zlib


MAGIC = b"TCQMT001"
ABI = 2
HEADER_SIZE = 128
SLOT_SIZE = 512
COMMIT_OFFSET = 504
BODY_SIZE = 504
PAYLOAD_SIZE = 388
HEADER = struct.Struct("<8sHH32s16sII4xQQQQ")
PREFIX = struct.Struct("<QHHI32s32sQQQQI")
COMMIT = struct.Struct("<Q")
SUBMIT = struct.Struct("<iiidq64s64s64s64s")
CANCEL = struct.Struct("<64s64s16s")
QUERY = struct.Struct("<64s16s16s64s")
CALLBACK = struct.Struct("<64s64s64s64s64sqqi")
COMPLETE = struct.Struct("<Q")
FAULT = struct.Struct("<64s")

K_SUBMIT, K_CANCEL, K_QUERY = 1, 2, 3
K_CALL_STARTED, K_CALL_RETURNED = 10, 11
K_ORDER_CALLBACK, K_TRADE_CALLBACK, K_QUERY_COMPLETE, K_FAULT, K_HEALTH = 20, 21, 24, 30, 31


class _G(object):
    command = None
    event = None
    config = None
    draining = False
    halted = True
    probe_ticks = 0


G = _G()

_KERNEL32 = ctypes.WinDLL("kernel32", use_last_error=True)
_QPC_FREQUENCY = ctypes.c_longlong()
if not _KERNEL32.QueryPerformanceFrequency(ctypes.byref(_QPC_FREQUENCY)) or _QPC_FREQUENCY.value <= 0:
    raise RuntimeError("QueryPerformanceFrequency is unavailable")


def _now():
    """Return integer QPC nanoseconds without Python 3.6 float precision loss."""
    counter = ctypes.c_longlong()
    if not _KERNEL32.QueryPerformanceCounter(ctypes.byref(counter)):
        raise RuntimeError("QueryPerformanceCounter failed")
    return (counter.value * 1000000000) // _QPC_FREQUENCY.value


def _text(value, size, name):
    if not isinstance(value, str):
        value = str(value)
    raw = value.encode("utf-8")
    if not raw or len(raw) >= size or b"\x00" in raw:
        raise RuntimeError("invalid " + name)
    return raw + (b"\x00" * (size - len(raw)))


def _read_text(raw, name):
    raw = raw.split(b"\x00", 1)[0]
    if not raw:
        raise RuntimeError("missing " + name)
    return raw.decode("utf-8")


class _Atomic(object):
    def __init__(self, path):
        dll = ctypes.WinDLL(path)
        self.load = dll.tradecore_qmt_load_u64
        self.store = dll.tradecore_qmt_store_u64
        self.aligned = dll.tradecore_qmt_is_aligned_u64
        self.load.argtypes, self.load.restype = [ctypes.POINTER(ctypes.c_longlong)], ctypes.c_longlong
        self.store.argtypes, self.store.restype = [ctypes.POINTER(ctypes.c_longlong), ctypes.c_longlong], ctypes.c_longlong
        self.aligned.argtypes, self.aligned.restype = [ctypes.POINTER(ctypes.c_longlong)], ctypes.c_int

    def get(self, pointer):
        if not self.is_aligned(pointer):
            raise RuntimeError("unaligned atomic")
        return int(self.load(ctypes.byref(pointer)))

    def set(self, pointer, value):
        if value < 0 or value > (1 << 63) - 1 or not self.is_aligned(pointer):
            raise RuntimeError("invalid atomic value")
        self.store(ctypes.byref(pointer), value)

    def is_aligned(self, pointer):
        return ctypes.addressof(pointer) % 8 == 0 and bool(self.aligned(ctypes.byref(pointer)))


class _Ring(object):
    def __init__(self, path, scope, generation, atomic):
        self.file = open(path, "r+b")
        try:
            self.map = mmap.mmap(self.file.fileno(), 0, access=mmap.ACCESS_WRITE)
            self.atomic = atomic
            values = HEADER.unpack_from(self.map, 0)
            if values[0] != MAGIC or values[1] != ABI or values[2] != 0:
                raise RuntimeError("ring ABI mismatch")
            if values[3] != scope or values[4] != generation or values[5] != SLOT_SIZE or values[6] < 2:
                raise RuntimeError("ring identity mismatch")
            self.capacity = values[6]
            if self.map.size() != HEADER_SIZE + self.capacity * SLOT_SIZE:
                raise RuntimeError("ring size mismatch")
            self.producer = ctypes.c_longlong.from_buffer(self.map, 72)
            self.consumer = ctypes.c_longlong.from_buffer(self.map, 80)
            self.producer_hb = ctypes.c_longlong.from_buffer(self.map, 88)
            self.consumer_hb = ctypes.c_longlong.from_buffer(self.map, 96)
            for name in ("producer", "consumer", "producer_hb", "consumer_hb"):
                if not atomic.is_aligned(getattr(self, name)):
                    raise RuntimeError("unaligned ring counter")
        except BaseException:
            self.close()
            raise

    def close(self):
        for name in ("producer", "consumer", "producer_hb", "consumer_hb"):
            if hasattr(self, name):
                delattr(self, name)
        if hasattr(self, "map"):
            self.map.close()
        self.file.close()

    def _offset(self, sequence):
        return HEADER_SIZE + ((sequence - 1) % self.capacity) * SLOT_SIZE

    def read(self):
        consumed, produced = self.atomic.get(self.consumer), self.atomic.get(self.producer)
        if produced < consumed or produced - consumed > self.capacity:
            raise RuntimeError("ring cursor corrupt")
        if produced == consumed:
            return None
        sequence, offset = consumed + 1, self._offset(consumed + 1)
        raw = self.map[offset:offset + SLOT_SIZE]
        if COMMIT.unpack_from(raw, COMMIT_OFFSET)[0] != sequence:
            raise RuntimeError("ring commit mismatch")
        fields = PREFIX.unpack_from(raw, 0)
        if fields[0] != sequence or fields[3] > PAYLOAD_SIZE:
            raise RuntimeError("ring frame mismatch")
        body = bytearray(raw[:BODY_SIZE])
        PREFIX.pack_into(body, 0, fields[0], fields[1], fields[2], fields[3], fields[4], fields[5], fields[6], fields[7], fields[8], fields[9], 0)
        if (zlib.crc32(body) & 0xffffffff) != fields[10]:
            raise RuntimeError("ring crc mismatch")
        payload = raw[PREFIX.size:PREFIX.size + fields[3]]
        self.atomic.set(self.consumer, sequence)
        self.atomic.set(self.consumer_hb, _now())
        return (fields[1], fields[4], fields[5], fields[6:10], payload)

    def write(self, kind, request_digest, intent_digest, timestamps, payload):
        if len(request_digest) != 32 or len(intent_digest) != 32 or len(payload) > PAYLOAD_SIZE:
            raise RuntimeError("event bounds invalid")
        produced, consumed = self.atomic.get(self.producer), self.atomic.get(self.consumer)
        if produced < consumed or produced - consumed >= self.capacity:
            raise RuntimeError("event ring full")
        sequence, offset = produced + 1, self._offset(produced + 1)
        body = bytearray(BODY_SIZE)
        PREFIX.pack_into(body, 0, sequence, kind, 0, len(payload), request_digest, intent_digest, timestamps[0], timestamps[1], timestamps[2], timestamps[3], 0)
        body[PREFIX.size:PREFIX.size + len(payload)] = payload
        crc = zlib.crc32(body) & 0xffffffff
        PREFIX.pack_into(body, 0, sequence, kind, 0, len(payload), request_digest, intent_digest, timestamps[0], timestamps[1], timestamps[2], timestamps[3], crc)
        self.map[offset:offset + BODY_SIZE] = body
        self.atomic.set(ctypes.c_longlong.from_buffer(self.map, offset + COMMIT_OFFSET), sequence)
        self.atomic.set(self.producer, sequence)
        self.atomic.set(self.producer_hb, _now())


def _load_config():
    path = os.environ.get("TRADECORE_QMT_ENGINE_CONFIG")
    if not path:
        raise RuntimeError("TRADECORE_QMT_ENGINE_CONFIG is required")
    with open(path, "rb") as config_file:
        cfg = json.loads(config_file.read().decode("utf-8"))
    required = ("scope_digest_hex", "generation_hex", "command_ring_path", "event_ring_path", "atomic_dll_path", "qmt_timer_period", "qmt_timer_start_time", "max_drain", "mode", "allow_mutation", "callback_attributes")
    if not isinstance(cfg, dict) or set(required) - set(cfg):
        raise RuntimeError("engine configuration is incomplete")
    if cfg["mode"] not in ("PROBE", "ARMED") or not isinstance(cfg["allow_mutation"], bool) or cfg["max_drain"] < 1:
        raise RuntimeError("engine configuration is invalid")
    if cfg["mode"] == "ARMED" and not cfg["allow_mutation"]:
        raise RuntimeError("armed engine lacks mutation approval")
    if cfg["mode"] == "PROBE" and (cfg["allow_mutation"] or cfg.get("native_policy") is not None):
        raise RuntimeError("probe engine forbids mutation and native policy")
    return cfg


def _fault(code, request=None, intent=None):
    G.halted = True
    if G.event is not None:
        try:
            G.event.write(K_FAULT, request or (b"\x00" * 32), intent or (b"\x00" * 32), (_now(), 0, 0, 0), FAULT.pack(_text(code, 64, "fault")))
        except Exception:
            pass


def init(ContextInfo):
    try:
        if G.command is not None or G.event is not None:
            raise RuntimeError("model is already initialized; stop before restart")
        cfg = _load_config()
        scope, generation = bytes.fromhex(cfg["scope_digest_hex"]), bytes.fromhex(cfg["generation_hex"])
        if len(scope) != 32 or len(generation) != 16:
            raise RuntimeError("scope generation is invalid")
        atomics = _Atomic(cfg["atomic_dll_path"])
        G.command = _Ring(cfg["command_ring_path"], scope, generation, atomics)
        G.event = _Ring(cfg["event_ring_path"], scope, generation, atomics)
        G.config = cfg
        G.probe_ticks = 0
        G.halted = True
        ContextInfo.run_time("tradecore_qmt_drain", cfg["qmt_timer_period"], cfg["qmt_timer_start_time"])
        G.event.write(K_HEALTH, b"\x00" * 32, b"\x00" * 32, (_now(), 0, 0, 0), b"PROBE_INIT" if cfg["mode"] == "PROBE" else b"ARMED_INIT")
        G.halted = False
    except Exception as error:
        _fault("INIT_" + error.__class__.__name__)


def tradecore_qmt_drain(ContextInfo):
    if G.halted or G.draining:
        return
    G.draining = True
    try:
        entered_ns = _now()
        count = 0
        while count < G.config["max_drain"]:
            frame = G.command.read()
            if frame is None:
                break
            _dispatch(ContextInfo, frame)
            count += 1
        if G.config["mode"] == "PROBE":
            G.probe_ticks += 1
            G.event.write(K_HEALTH, b"\x00" * 32, b"\x00" * 32,
                          (entered_ns, 0, 0, 0), b"PROBE_TIMER" + struct.pack("<Q", G.probe_ticks))
    except Exception as error:
        _fault("DRAIN_" + error.__class__.__name__)
    finally:
        G.draining = False


def _dispatch(ContextInfo, frame):
    kind, request, intent, times, payload = frame
    if G.config["mode"] != "ARMED" or not G.config["allow_mutation"]:
        raise RuntimeError("engine is disarmed")
    before = _now()
    G.event.write(K_CALL_STARTED, request, intent, (times[0], times[1], before, 0), b"")
    if kind == K_SUBMIT:
        values = SUBMIT.unpack(payload)
        passorder(values[0], values[1], _read_text(values[5], "account"), _read_text(values[6], "code"), values[2], values[3], values[4], _read_text(values[7], "strategy"), 2, _read_text(values[8], "userOrderId"), ContextInfo)
    elif kind == K_CANCEL:
        values = CANCEL.unpack(payload)
        cancel(_read_text(values[0], "order"), _read_text(values[1], "account"), _read_text(values[2], "account type"), ContextInfo)
    elif kind == K_QUERY:
        values = QUERY.unpack(payload)
        rows = get_trade_detail_data(_read_text(values[0], "account"), _read_text(values[1], "account type"), _read_text(values[2], "data type"), _read_text(values[3], "strategy"))
        G.event.write(K_QUERY_COMPLETE, request, intent, (times[0], times[1], before, _now()), COMPLETE.pack(len(rows)))
        return
    else:
        raise RuntimeError("unsupported command")
    G.event.write(K_CALL_RETURNED, request, intent, (times[0], times[1], before, _now()), b"")


def _callback(payload, kind):
    if G.halted:
        return
    try:
        attrs = G.config["callback_attributes"]
        event_key = _text(getattr(payload, attrs["event_key"]), 64, "event key")
        ref = _text(getattr(payload, attrs["client_order_ref"]), 64, "client ref")
        order_id = _text(getattr(payload, attrs["venue_order_id"]), 64, "order id")
        trade_key = _text(getattr(payload, attrs.get("trade_key", attrs["event_key"])), 64, "trade key")
        filled, leaves, error_code = int(getattr(payload, attrs["filled_qty"])), int(getattr(payload, attrs["leaves_qty"])), int(getattr(payload, attrs["error_code"]))
        now = _now()
        G.event.write(kind, hashlib.sha256(ref).digest(), hashlib.sha256(event_key).digest(), (now, 0, 0, 0), CALLBACK.pack(event_key, ref, order_id, b"\x00" * 64, trade_key, filled, leaves, error_code))
    except Exception as error:
        _fault("CALLBACK_" + error.__class__.__name__)


def order_callback(ContextInfo, order_info):
    _callback(order_info, K_ORDER_CALLBACK)


def deal_callback(ContextInfo, deal_info):
    _callback(deal_info, K_TRADE_CALLBACK)


def stop(ContextInfo):
    if not G.halted and G.event is not None:
        try:
            G.event.write(K_HEALTH, b"\x00" * 32, b"\x00" * 32, (_now(), 0, 0, 0), b"MODEL_STOP")
        except Exception:
            pass
    G.halted = True
    for name in ("command", "event"):
        ring = getattr(G, name)
        if ring is not None:
            ring.close()
            setattr(G, name, None)
