"""The one cross-platform dynamic-library boundary for the EMT binding."""

from __future__ import annotations

import ctypes
import importlib
import os
import platform
import sys
from contextlib import contextmanager
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import Any, Iterator, Mapping


class PlatformLoadError(RuntimeError):
    """The local Python/SDK platform combination cannot be proven safe to load."""


@dataclass(frozen=True)
class SdkLoadSettings:
    sdk_root: Path
    expected_hashes: Mapping[str, str] | None = None
    required_python: tuple[int, int] = (3, 10)
    require_project_venv: bool = True


@dataclass(frozen=True)
class LoadedSdk:
    library_dir: Path
    module: Any

    @property
    def trader_api_type(self) -> type[Any]:
        try:
            return self.module.TraderApi
        except AttributeError as exc:
            raise PlatformLoadError("vnemttrader does not expose TraderApi") from exc


def sdk_library_dir(sdk_root: Path, *, system: str | None = None) -> Path:
    """Resolve the only permitted per-platform binary directory."""

    normalized = (system or platform.system()).lower()
    directory_name = {"windows": "windows", "linux": "linux"}.get(normalized)
    if directory_name is None:
        raise PlatformLoadError(f"unsupported operating system: {system or platform.system()}")
    directory = sdk_root.resolve() / "lib" / directory_name
    if not directory.is_dir():
        raise PlatformLoadError(f"SDK library directory is missing: {directory}")
    return directory


def verify_sdk_hashes(directory: Path, expected_hashes: Mapping[str, str] | None) -> None:
    """Verify only an explicit manifest; never discover and trust arbitrary files."""

    if expected_hashes is None:
        return
    for relative_name, expected in expected_hashes.items():
        if Path(relative_name).name != relative_name:
            raise PlatformLoadError("SDK hash manifest entries must be plain file names")
        if len(expected) != 64 or any(
            character not in "0123456789abcdef" for character in expected
        ):
            raise PlatformLoadError(f"invalid SHA-256 digest for {relative_name}")
        path = directory / relative_name
        if not path.is_file():
            raise PlatformLoadError(f"SDK manifest file is missing: {relative_name}")
        actual = sha256(path.read_bytes()).hexdigest()
        if actual != expected:
            raise PlatformLoadError(f"SDK SHA-256 mismatch: {relative_name}")


def is_project_venv_runtime(
    *,
    prefix: str | Path | None = None,
    base_prefix: str | Path | None = None,
) -> bool:
    """Return whether execution is from this deployment's ``.venv``.

    This deliberately assesses the interpreter running the gateway, rather
    than inspecting a distribution-level ``python3`` executable.  Services
    must invoke ``.venv/bin/python`` (or ``.venv\\Scripts\\python.exe``).
    """

    runtime_prefix = Path(sys.prefix if prefix is None else prefix)
    runtime_base_prefix = Path(sys.base_prefix if base_prefix is None else base_prefix)
    return runtime_prefix.name == ".venv" and runtime_prefix != runtime_base_prefix


class EmtPlatformLoader:
    """Loads ``vnemttrader`` only after ABI, directory and optional hashes pass."""

    def __init__(self, settings: SdkLoadSettings) -> None:
        self._settings = settings
        self._dll_directory_handle: Any | None = None

    def load(self) -> LoadedSdk:
        if self._settings.require_project_venv and not is_project_venv_runtime():
            raise PlatformLoadError(
                "EMT SDK load requires the project .venv interpreter; "
                "do not invoke a server default Python"
            )
        if sys.implementation.name != "cpython":
            raise PlatformLoadError(
                f"CPython is required; current implementation is {sys.implementation.name}"
            )
        if sys.version_info[:2] != self._settings.required_python:
            expected = ".".join(str(item) for item in self._settings.required_python)
            raise PlatformLoadError(
                f"CPython {expected} is required; current is {sys.version.split()[0]}"
            )
        directory = sdk_library_dir(self._settings.sdk_root)
        verify_sdk_hashes(directory, self._settings.expected_hashes)
        system = platform.system()
        if system == "Windows":
            add_directory = getattr(os, "add_dll_directory", None)
            if add_directory is None:
                raise PlatformLoadError("os.add_dll_directory is unavailable on this interpreter")
            self._dll_directory_handle = add_directory(str(directory))
        elif system == "Linux":
            configured = os.environ.get("LD_LIBRARY_PATH", "").split(os.pathsep)
            if str(directory) not in configured:
                raise PlatformLoadError(
                    "Linux SDK directory must be set in this service process LD_LIBRARY_PATH before start"
                )
        if str(directory) not in sys.path:
            sys.path.insert(0, str(directory))
        try:
            module = importlib.import_module("vnemttrader")
        except ImportError as exc:
            raise PlatformLoadError(f"cannot import vnemttrader from {directory}: {exc}") from exc
        module_file = getattr(module, "__file__", None)
        if not module_file or Path(module_file).resolve().parent != directory:
            raise PlatformLoadError(
                "vnemttrader was loaded from outside the configured SDK directory"
            )
        return LoadedSdk(library_dir=directory, module=module)


@contextmanager
def suppress_native_console_output(*, flush_after: bool = True) -> Iterator[None]:
    """Suppress vendor C/C++ console output while native calls receive secrets.

    EMT's native bindings can bypass Python's text streams on both Windows and
    Linux.  Redirect the process file descriptors for the short critical
    section rather than assuming platform-specific logging behaviour.  Native
    teardown may hold its own stdio lock after ``release``; callers that have
    permanently closed that session can opt out of the final global flush.
    """

    _flush_native_stdio()
    stdout_fd, stderr_fd = 1, 2
    saved_stdout, saved_stderr = os.dup(stdout_fd), os.dup(stderr_fd)
    null_fd = os.open(os.devnull, os.O_WRONLY)
    try:
        os.dup2(null_fd, stdout_fd)
        os.dup2(null_fd, stderr_fd)
        yield
    finally:
        # Native C/C++ streams can buffer until after ``login`` returns.  Flush
        # them while they still point at the null device, before restoring FDs.
        # A completed release is the one exception: the vendor may retain a
        # native stdio lock while stopping its worker threads, and no further
        # native call is allowed on that closed session.
        if flush_after:
            _flush_native_stdio()
        os.dup2(saved_stdout, stdout_fd)
        os.dup2(saved_stderr, stderr_fd)
        os.close(null_fd)
        os.close(saved_stdout)
        os.close(saved_stderr)


def _flush_native_stdio() -> None:
    """Best-effort process-wide C stdio flush without changing call outcome."""

    try:
        runtime = ctypes.CDLL("msvcrt") if os.name == "nt" else ctypes.CDLL(None)
        flush = runtime.fflush
        flush.argtypes = [ctypes.c_void_p]
        flush.restype = ctypes.c_int
        flush(None)
    except (AttributeError, OSError, TypeError):
        return
