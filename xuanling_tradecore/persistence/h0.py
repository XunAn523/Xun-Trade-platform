"""Read-only H0 persistent-memory Go/No-Go probe.

This module never creates a Journal, maps ordinary files as persistent memory,
or falls back to ``msync``.  It verifies the observable host preconditions and
requires a separately recorded physical-media/recovery-drill attestation.  A
successful report permits M1-03 implementation testing; it does not by itself
arm an account or authorize broker activity.
"""

from __future__ import annotations

import ctypes
import ctypes.util
import json
import platform
import stat
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Mapping, cast

_REQUIRED_PMEM2_SYMBOLS = (
    "pmem2_map_new",
    "pmem2_get_persist_fn",
    "pmem2_get_flush_fn",
    "pmem2_get_drain_fn",
)
_PERSISTENCE_DOMAINS = frozenset({"ADR", "EADR"})
_VIRTUALIZATION_MARKERS = (
    "amazon ec2",
    "bhyve",
    "bochs",
    "google compute engine",
    "hyper-v",
    "kvm",
    "microsoft corporation",
    "openstack",
    "parallels",
    "qemu",
    "virtualbox",
    "vmware",
    "xen",
)


class H0EvidenceError(ValueError):
    """An operator-supplied H0 persistence-domain attestation is malformed."""


@dataclass(frozen=True)
class H0DaxDevice:
    """One Linux device-DAX candidate observed from sysfs and /dev."""

    sysfs_name: str
    device_path: str
    size_bytes: int
    is_character_device: bool


@dataclass(frozen=True)
class H0HostSnapshot:
    """Read-only observable state used to make an H0 admission decision."""

    system: str
    hostname: str
    os_release: str
    dax_devices: tuple[H0DaxDevice, ...]
    pmem2_library_path: str | None
    pmem2_symbols: frozenset[str]
    dmi_product_name: str = ""
    dmi_sys_vendor: str = ""
    hypervisor_present: bool = False

    @property
    def is_wsl_or_virtualized_linux(self) -> bool:
        """Reject the explicitly unsupported WSL/virtual DAX environment category."""

        fingerprint = " ".join(
            (
                self.system,
                self.os_release,
                self.dmi_product_name,
                self.dmi_sys_vendor,
            )
        ).casefold()
        return (
            self.hypervisor_present
            or "microsoft" in fingerprint
            or "wsl" in fingerprint
            or any(marker in fingerprint for marker in _VIRTUALIZATION_MARKERS)
        )


@dataclass(frozen=True)
class H0Evidence:
    """Human-reviewed physical-domain and recovery-drill evidence for one host."""

    schema_version: int
    host_id: str
    dax_device: str
    dax_sysfs_name: str
    namespace_uuid: str
    persistence_domain: str
    mapping_length_bytes: int
    physical_media_verified: bool
    recovery_drill_id: str
    recovery_drill_completed: bool
    verified_at_utc: str

    def __post_init__(self) -> None:
        schema_version: object = self.schema_version
        if schema_version != 1:
            raise H0EvidenceError("H0 evidence schema_version is unsupported")
        for value, name in (
            (cast(object, self.host_id), "host_id"),
            (cast(object, self.dax_device), "dax_device"),
            (cast(object, self.dax_sysfs_name), "dax_sysfs_name"),
            (cast(object, self.namespace_uuid), "namespace_uuid"),
            (cast(object, self.recovery_drill_id), "recovery_drill_id"),
            (cast(object, self.verified_at_utc), "verified_at_utc"),
        ):
            if not isinstance(value, str) or not value.strip() or len(value) > 256:
                raise H0EvidenceError(f"H0 evidence {name} is invalid")
        if self.persistence_domain not in _PERSISTENCE_DOMAINS:
            raise H0EvidenceError("H0 evidence persistence_domain must be ADR or EADR")
        if self.mapping_length_bytes <= 0:
            raise H0EvidenceError("H0 evidence mapping_length_bytes must be positive")


@dataclass(frozen=True)
class H0Check:
    """One non-sensitive H0 decision predicate and its observed result."""

    code: str
    passed: bool
    detail: str


@dataclass(frozen=True)
class H0ProbeReport:
    """The fail-closed H0 outcome suitable for a restricted evidence artifact."""

    schema_version: int
    eligible: bool
    checks: tuple[H0Check, ...]

    def as_dict(self) -> Mapping[str, object]:
        """Return deterministic, JSON-ready metadata without journal or credential contents."""

        return asdict(self)


def collect_h0_host_snapshot(
    *,
    dax_sysfs_root: Path = Path("/sys/bus/dax/devices"),
    device_root: Path = Path("/dev"),
    os_release_path: Path = Path("/proc/sys/kernel/osrelease"),
    dmi_product_name_path: Path = Path("/sys/class/dmi/id/product_name"),
    dmi_sys_vendor_path: Path = Path("/sys/class/dmi/id/sys_vendor"),
    cpuinfo_path: Path = Path("/proc/cpuinfo"),
) -> H0HostSnapshot:
    """Collect only read-only Linux PMem/DAX and ``libpmem2`` capability facts."""

    devices: list[H0DaxDevice] = []
    if dax_sysfs_root.is_dir():
        for entry in sorted(dax_sysfs_root.iterdir(), key=lambda item: item.name):
            size = _read_nonnegative_int(entry / "size")
            device_path = device_root / entry.name
            try:
                is_character_device = stat.S_ISCHR(device_path.stat().st_mode)
            except OSError:
                is_character_device = False
            devices.append(
                H0DaxDevice(
                    sysfs_name=entry.name,
                    device_path=str(device_path),
                    size_bytes=size,
                    is_character_device=is_character_device,
                )
            )
    library_path, symbols = _probe_libpmem2()
    return H0HostSnapshot(
        system=platform.system(),
        hostname=platform.node(),
        os_release=_read_text(os_release_path),
        dax_devices=tuple(devices),
        pmem2_library_path=library_path,
        pmem2_symbols=frozenset(symbols),
        dmi_product_name=_read_text(dmi_product_name_path),
        dmi_sys_vendor=_read_text(dmi_sys_vendor_path),
        hypervisor_present="hypervisor" in _read_text(cpuinfo_path).casefold(),
    )


def load_h0_evidence(path: Path) -> H0Evidence:
    """Load one strict JSON attestation without accepting unrecognized fields."""

    try:
        decoded: object = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise H0EvidenceError("H0 evidence cannot be read as JSON") from error
    if not isinstance(decoded, dict):
        raise H0EvidenceError("H0 evidence root must be an object")
    raw_payload = cast(dict[object, object], decoded)
    if not all(isinstance(key, str) for key in raw_payload):
        raise H0EvidenceError("H0 evidence root keys must be text")
    payload = cast(dict[str, object], raw_payload)
    expected = {
        "schema_version",
        "host_id",
        "dax_device",
        "dax_sysfs_name",
        "namespace_uuid",
        "persistence_domain",
        "mapping_length_bytes",
        "physical_media_verified",
        "recovery_drill_id",
        "recovery_drill_completed",
        "verified_at_utc",
    }
    if set(payload) != expected:
        raise H0EvidenceError("H0 evidence fields do not match the approved schema")
    return H0Evidence(
        schema_version=_evidence_int(payload, "schema_version"),
        host_id=_evidence_text(payload, "host_id"),
        dax_device=_evidence_text(payload, "dax_device"),
        dax_sysfs_name=_evidence_text(payload, "dax_sysfs_name"),
        namespace_uuid=_evidence_text(payload, "namespace_uuid"),
        persistence_domain=_evidence_text(payload, "persistence_domain"),
        mapping_length_bytes=_evidence_int(payload, "mapping_length_bytes"),
        physical_media_verified=_evidence_bool(payload, "physical_media_verified"),
        recovery_drill_id=_evidence_text(payload, "recovery_drill_id"),
        recovery_drill_completed=_evidence_bool(payload, "recovery_drill_completed"),
        verified_at_utc=_evidence_text(payload, "verified_at_utc"),
    )


def _evidence_text(payload: Mapping[str, object], field: str) -> str:
    """Read one explicitly typed text field from the strict evidence payload."""

    value = payload[field]
    if not isinstance(value, str):
        raise H0EvidenceError(f"H0 evidence {field} must be text")
    return value


def _evidence_int(payload: Mapping[str, object], field: str) -> int:
    """Read one non-boolean integer field from the strict evidence payload."""

    value = payload[field]
    if isinstance(value, bool) or not isinstance(value, int):
        raise H0EvidenceError(f"H0 evidence {field} must be an integer")
    return value


def _evidence_bool(payload: Mapping[str, object], field: str) -> bool:
    """Read one explicitly typed boolean field from the strict evidence payload."""

    value = payload[field]
    if not isinstance(value, bool):
        raise H0EvidenceError(f"H0 evidence {field} must be boolean")
    return value


def evaluate_h0(snapshot: H0HostSnapshot, evidence: H0Evidence | None) -> H0ProbeReport:
    """Evaluate H0 with no fallback: every required check must explicitly pass."""

    checks: list[H0Check] = [
        H0Check(
            code="LINUX_NATIVE",
            passed=snapshot.system == "Linux",
            detail="Linux host is required"
            if snapshot.system != "Linux"
            else "Linux host detected",
        ),
        H0Check(
            code="NO_WSL_OR_VIRTUAL_DAX",
            passed=not snapshot.is_wsl_or_virtualized_linux,
            detail=(
                "WSL/virtualized Linux is not an H0 persistent-memory environment"
                if snapshot.is_wsl_or_virtualized_linux
                else "no WSL marker detected"
            ),
        ),
        H0Check(
            code="DEVICE_DAX_PRESENT",
            passed=bool(snapshot.dax_devices),
            detail=(
                "at least one device-DAX node is visible"
                if snapshot.dax_devices
                else "no device-DAX node is visible"
            ),
        ),
        H0Check(
            code="LIBPMEM2_FLUSH_DRAIN",
            passed=set(_REQUIRED_PMEM2_SYMBOLS).issubset(snapshot.pmem2_symbols),
            detail=(
                "libpmem2 persist/flush/drain capability is available"
                if set(_REQUIRED_PMEM2_SYMBOLS).issubset(snapshot.pmem2_symbols)
                else "libpmem2 persist/flush/drain symbols are unavailable"
            ),
        ),
    ]
    if evidence is None:
        checks.append(
            H0Check(
                code="ATTESTED_PHYSICAL_DAX_AND_RECOVERY_DRILL",
                passed=False,
                detail="approved physical-media and recovery-drill evidence is required",
            )
        )
        return H0ProbeReport(schema_version=1, eligible=False, checks=tuple(checks))

    matching_device = next(
        (
            device
            for device in snapshot.dax_devices
            if device.sysfs_name == evidence.dax_sysfs_name
            and device.device_path == evidence.dax_device
        ),
        None,
    )
    checks.extend(
        (
            H0Check(
                code="EVIDENCE_HOST_MATCH",
                passed=evidence.host_id == snapshot.hostname,
                detail="evidence host matches"
                if evidence.host_id == snapshot.hostname
                else "evidence host differs",
            ),
            H0Check(
                code="DEVICE_DAX_CHARACTER_NODE",
                passed=matching_device is not None and matching_device.is_character_device,
                detail=(
                    "attested device-DAX character node is present"
                    if matching_device is not None and matching_device.is_character_device
                    else "attested device-DAX character node is absent"
                ),
            ),
            H0Check(
                code="DAX_CAPACITY",
                passed=matching_device is not None
                and matching_device.size_bytes >= evidence.mapping_length_bytes,
                detail=(
                    "attested DAX capacity covers the hot mapping"
                    if matching_device is not None
                    and matching_device.size_bytes >= evidence.mapping_length_bytes
                    else "attested DAX capacity is unavailable or insufficient"
                ),
            ),
            H0Check(
                code="PERSISTENCE_DOMAIN",
                passed=evidence.persistence_domain in _PERSISTENCE_DOMAINS
                and evidence.physical_media_verified,
                detail=(
                    "ADR/eADR physical media evidence is present"
                    if evidence.persistence_domain in _PERSISTENCE_DOMAINS
                    and evidence.physical_media_verified
                    else "physical ADR/eADR media evidence is incomplete"
                ),
            ),
            H0Check(
                code="RECOVERY_DRILL",
                passed=evidence.recovery_drill_completed,
                detail=(
                    "recorded recovery drill is complete"
                    if evidence.recovery_drill_completed
                    else "recorded recovery drill is incomplete"
                ),
            ),
        )
    )
    return H0ProbeReport(
        schema_version=1,
        eligible=all(check.passed for check in checks),
        checks=tuple(checks),
    )


def _probe_libpmem2() -> tuple[str | None, frozenset[str]]:
    library_path = ctypes.util.find_library("pmem2")
    if library_path is None:
        return None, frozenset()
    try:
        library = ctypes.CDLL(library_path)
    except OSError:
        return library_path, frozenset()
    return library_path, frozenset(
        symbol for symbol in _REQUIRED_PMEM2_SYMBOLS if hasattr(library, symbol)
    )


def _read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8").strip()
    except OSError:
        return ""


def _read_nonnegative_int(path: Path) -> int:
    raw = _read_text(path)
    try:
        value = int(raw, 0)
    except ValueError:
        return 0
    return value if value >= 0 else 0
