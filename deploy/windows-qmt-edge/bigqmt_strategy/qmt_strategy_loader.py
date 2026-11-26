# coding: utf-8
"""QMT-side stable loader for the TradeCore QMT P0 probe strategy.

Paste this small script ONCE into QMT as the strategy.  On every run it reads
the real strategy loader from disk, so updates to the strategy loader / native
engine under D:\\projects\\TradeCore\\qmt-edge do not require re-pasting into
QMT.
"""


HELPER_PATH = r"D:\projects\TradeCore\qmt-edge\src\deploy\windows-qmt-edge\bigqmt_strategy\tradecore_qmt_p0_probe_strategy.py"

print("[QMT_LOADER] helper_path=%s" % HELPER_PATH)

with open(HELPER_PATH, "r", encoding="utf-8") as helper_file:
    code = helper_file.read()

globals()["__file__"] = HELPER_PATH
exec(compile(code, HELPER_PATH, "exec"), globals())

print("[QMT_LOADER] loaded helper from %s" % HELPER_PATH)
