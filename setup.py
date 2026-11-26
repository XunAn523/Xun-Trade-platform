"""Build the required C++ engine and optional legacy C callback rings."""

from __future__ import annotations

import sys

from setuptools import Extension, setup

cpp_flags = (
    ["/std:c++20", "/Zc:__cplusplus", "/O2", "/EHsc"]
    if sys.platform == "win32"
    else ["-std=c++20", "-O3", "-pthread"]
)
extensions = [
    Extension(
        "xuntrade_omsems._native",
        ["native/src/kernels.cpp", "native/src/python_module.cpp"],
        include_dirs=["native/include"],
        language="c++",
        extra_compile_args=cpp_flags,
        extra_link_args=[] if sys.platform == "win32" else ["-pthread"],
    ),
    Extension("emt_gateway._fastspi", ["native/fastspi_ring.c"], optional=True),
]
if sys.platform.startswith("linux"):
    extensions.append(
        Extension(
            "xuanling_tradecore._fastspi_v2",
            ["native/fastspi_v2_ring.c"],
            extra_compile_args=["-std=c11"],
            optional=True,
        )
    )


setup(
    ext_modules=extensions,
)
