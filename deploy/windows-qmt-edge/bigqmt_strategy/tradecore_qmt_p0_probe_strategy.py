# -*- coding: utf-8 -*-
"""Direct Big QMT strategy entry point for the TradeCore P0 probe.

Select this file as the QMT strategy (or paste its full contents into a new,
dedicated QMT strategy).  It follows the established QMT gateway loader model:
pin the installed helper by SHA-256, validate the local deployment profile, and
then execute the helper in the strategy namespace so Big QMT can discover its
``init``/timer/``stop`` callbacks.

This loader is deliberately P0-only.  It rejects every non-PROBE profile
before loading the engine.  The loaded engine has no network, SQLite, query,
cancel, or order action in PROBE mode.
"""

import hashlib
import json
import os

STRATEGY_NAME = "TradeCore_QMT_P0_PROBE"
CONFIG_PATH = r"D:\projects\TradeCore\qmt-edge\config\edge.json"
ENGINE_PATH = r"D:\projects\TradeCore\qmt-edge\src\deploy\windows-qmt-edge\qmt_native_engine.py"
EXPECTED_ENGINE_SHA256 = "5e80b6f5d4ddf137388ea0d1df96a4d387296fa5be9dfb883079b7b4a5ccd9c7"


def _same_path(left, right):
    return os.path.normcase(os.path.normpath(left)) == os.path.normcase(os.path.normpath(right))


def _require_probe_config():
    if not os.path.isfile(CONFIG_PATH):
        raise RuntimeError("TradeCore Edge configuration is missing")
    with open(CONFIG_PATH, "rb") as config_file:
        config = json.loads(config_file.read().decode("utf-8"))
    if not isinstance(config, dict):
        raise RuntimeError("TradeCore Edge configuration is malformed")
    if config.get("mode") != "PROBE" or config.get("allow_mutation") is not False:
        raise RuntimeError("TradeCore QMT P0 strategy requires PROBE with mutation disabled")
    if config.get("native_policy") is not None:
        raise RuntimeError("TradeCore QMT P0 strategy forbids a native policy")


def _load_pinned_engine():
    _require_probe_config()
    configured_path = os.environ.get("TRADECORE_QMT_ENGINE_CONFIG")
    if configured_path and not _same_path(configured_path, CONFIG_PATH):
        raise RuntimeError("TradeCore engine configuration path is conflicting")
    os.environ["TRADECORE_QMT_ENGINE_CONFIG"] = CONFIG_PATH

    if not os.path.isfile(ENGINE_PATH):
        raise RuntimeError("TradeCore QMT Native Engine is missing")
    with open(ENGINE_PATH, "rb") as engine_file:
        source = engine_file.read()
    actual_sha256 = hashlib.sha256(source).hexdigest()
    if actual_sha256.lower() != EXPECTED_ENGINE_SHA256:
        raise RuntimeError("TradeCore QMT Native Engine SHA-256 mismatch")

    namespace = globals()
    exec(compile(source, ENGINE_PATH, "exec"), namespace, namespace)
    required_callbacks = ("init", "tradecore_qmt_drain", "stop")
    if any(not callable(namespace.get(name)) for name in required_callbacks):
        raise RuntimeError("TradeCore QMT Native Engine strategy callbacks are incomplete")
    print(
        "[tradecore_qmt_p0_probe_strategy] loaded strategy=%s engine_sha256=%s mode=PROBE"
        % (STRATEGY_NAME, actual_sha256)
    )


_load_pinned_engine()
