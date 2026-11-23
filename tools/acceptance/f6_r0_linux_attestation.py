"""Build and verify a sanitized F6 R0 Linux release-host attestation.

This is a pre-session, non-hot-path operator tool.  It records hashes and
booleans only: no account identifier, order data, hostname, address, key, or
filesystem path is emitted into the evidence document.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import re
import stat
import subprocess
import sys
import time
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import Mapping, Sequence, cast

from tools.performance.build_f6_release_manifest import (
    F6ReleaseManifestToolError,
    verify_release_manifest,
)
from xuanling_tradecore.runtime import (
    ReleaseManifestActivationErrorV1,
    ReleaseMaterialV1,
    SignedReleaseManifestV1,
    load_signed_release_manifest_v1_json,
)

_NATIVE_SUFFIXES = frozenset({".so", ".pyd", ".dll"})
_LOCAL_FILESYSTEM_DENYLIST = frozenset(
    {"nfs", "nfs4", "cifs", "smbfs", "fuse.sshfs", "9p", "afs", "ceph", "glusterfs"}
)
_REQUIRED_UNIT_VALUES = {
    "NoNewPrivileges": "yes",
    "PrivateTmp": "yes",
    "ProtectSystem": "strict",
    "ProtectHome": "yes",
}
_REQUIRED_UNIT_KEYS = frozenset(
    {
        "ExecStart",
        "LimitNOFILE",
        "MemoryMax",
        "ReadWritePaths",
        "TasksMax",
        *_REQUIRED_UNIT_VALUES,
    }
)


class F6R0LinuxAttestationError(RuntimeError):
    """A release candidate is not safe to attest on this Linux host."""


@dataclass(frozen=True)
class _RootProbe:
    label: str
    filesystem_type: str
    free_bytes: int
    mode: int

    def as_policy_value(self) -> dict[str, int | str]:
        """Return only non-identifying root properties used in policy hashes."""

        return {
            "filesystem_type": self.filesystem_type,
            "mode": self.mode,
        }


def collect_release_material(
    *,
    repository: Path,
    profiles: Mapping[str, Path],
    sdk_root: Path,
    native_extension: Path,
    systemd_unit: Path,
    control_root: Path,
    journal_root: Path,
    archive_root: Path,
    release_public_key: Path,
    release_revocations: Path,
    minimum_free_bytes: int,
) -> tuple[ReleaseMaterialV1, dict[str, bool | int | str]]:
    """Measure all F6 R0 material without starting an EMT session."""

    if platform.system() != "Linux":
        raise F6R0LinuxAttestationError("F6 R0 attestation requires Linux")
    if sys.version_info[:2] != (3, 10):
        raise F6R0LinuxAttestationError("F6 R0 attestation requires project CPython 3.10")
    if minimum_free_bytes <= 0:
        raise F6R0LinuxAttestationError("F6 R0 minimum free bytes must be positive")
    profile_hashes = {label: _sha256_file(path) for label, path in profiles.items()}
    _require_disarmed_profiles(profiles)
    control = _probe_root(control_root, "control", minimum_free_bytes)
    journal = _probe_root(journal_root, "journal", minimum_free_bytes)
    archive = _probe_root(archive_root, "archive", minimum_free_bytes)
    unit_values = _validate_systemd_unit(systemd_unit)
    material = ReleaseMaterialV1(
        artifact_hash=_git_archive_sha256(repository),
        emt_sdk_hash=_sdk_binary_inventory_hash(sdk_root),
        native_extension_hash=_sha256_file(
            _require_regular_file(native_extension, "native extension")
        ),
        elf_abi_hash=_elf_abi_hash(native_extension),
        systemd_unit_hash=_sha256_file(_require_regular_file(systemd_unit, "systemd unit")),
        cgroup_policy_hash=_canonical_hash(
            {key: unit_values[key] for key in ("LimitNOFILE", "MemoryMax", "TasksMax")}
        ),
        host_health_policy_hash=_canonical_hash(
            {
                "clock": "ntp-synchronized",
                "minimum_free_bytes": minimum_free_bytes,
                "roots": ("archive", "control", "journal"),
            }
        ),
        journal_policy_hash=_canonical_hash(
            {
                "minimum_free_bytes": minimum_free_bytes,
                "root": journal.as_policy_value(),
                "synchronous_hot_path_io": False,
            }
        ),
        control_root_policy_hash=_canonical_hash(
            {
                "minimum_free_bytes": minimum_free_bytes,
                "root": control.as_policy_value(),
                "symlinks_allowed": False,
            }
        ),
        archive_policy_hash=_canonical_hash(
            {
                "asynchronous_audit_only": True,
                "minimum_free_bytes": minimum_free_bytes,
                "root": archive.as_policy_value(),
            }
        ),
        bundle_trust_policy_hash=_canonical_hash(
            {
                "release_public_key_hash": _sha256_file(release_public_key),
                "release_revocations_hash": _sha256_file(release_revocations),
            }
        ),
        account_profile_hashes=profile_hashes,
    )
    checks: dict[str, bool | int | str] = {
        "archive_free_bytes": archive.free_bytes,
        "clock_synchronized": _clock_is_synchronized(),
        "control_free_bytes": control.free_bytes,
        "journal_free_bytes": journal.free_bytes,
        "linux": True,
        "native_extension_elf_resolved": True,
        "profile_disarmed": True,
        "python_3_10": True,
        "source_tree_clean": True,
        "systemd_disarmed": "--disarmed" in unit_values["ExecStart"],
    }
    if not checks["clock_synchronized"] or not checks["systemd_disarmed"]:
        raise F6R0LinuxAttestationError("F6 R0 host-health or systemd DISARMED gate failed")
    return material, checks


def write_unsigned_request(*, request: SignedReleaseManifestV1, output: Path) -> None:
    """Persist an overwrite-safe non-secret request for the offline signer."""

    if output.exists():
        raise F6R0LinuxAttestationError("F6 R0 request output already exists")
    document = request.as_dict()
    del document["signature"]
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(document, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def build_evidence(
    *, manifest: SignedReleaseManifestV1, checks: Mapping[str, bool | int | str]
) -> dict[str, object]:
    """Return a payload-free, path-free F6 R0 evidence document."""

    return {
        "schema_version": 1,
        "checked_at_wall_ns": time.time_ns(),
        "manifest_hash": manifest.content_hash,
        "manifest_id": manifest.manifest_id,
        "release_epoch": manifest.release_epoch,
        "checks": dict(sorted(checks.items())),
        "result": "PASS",
        "runtime_mode": "DISARMED",
    }


def _parse_profiles(values: Sequence[str]) -> Mapping[str, Path]:
    profiles: dict[str, Path] = {}
    for value in values:
        label, separator, raw_path = value.partition("=")
        if not separator or not label or not raw_path or label in profiles:
            raise F6R0LinuxAttestationError("each --profile must be one unique label=path pair")
        profiles[label] = Path(raw_path)
    if not profiles:
        raise F6R0LinuxAttestationError("F6 R0 requires at least one --profile")
    return profiles


def _require_disarmed_profiles(profiles: Mapping[str, Path]) -> None:
    for path in profiles.values():
        try:
            document = cast(object, json.loads(path.read_text(encoding="utf-8")))
        except (OSError, json.JSONDecodeError) as exc:
            raise F6R0LinuxAttestationError("F6 R0 profile is unreadable") from exc
        if not isinstance(document, dict):
            raise F6R0LinuxAttestationError("F6 R0 profile must be an object")
        approval = document.get("approval")
        if not isinstance(approval, dict) or approval.get("approved") is not False:
            raise F6R0LinuxAttestationError("F6 R0 requires each profile to remain DISARMED")


def _probe_root(path: Path, label: str, minimum_free_bytes: int) -> _RootProbe:
    if not path.is_absolute() or path.is_symlink() or not path.is_dir():
        raise F6R0LinuxAttestationError(f"F6 R0 {label} root is not a real absolute directory")
    metadata = path.stat()
    mode = stat.S_IMODE(metadata.st_mode)
    if mode & 0o022:
        raise F6R0LinuxAttestationError(f"F6 R0 {label} root permissions are too broad")
    if not os.access(path, os.R_OK | os.W_OK | os.X_OK):
        raise F6R0LinuxAttestationError(f"F6 R0 {label} root is inaccessible")
    free_bytes = os.statvfs(path).f_bavail * os.statvfs(path).f_frsize
    if free_bytes < minimum_free_bytes:
        raise F6R0LinuxAttestationError(f"F6 R0 {label} root capacity is below the policy floor")
    filesystem_type = _filesystem_type(path)
    if filesystem_type in _LOCAL_FILESYSTEM_DENYLIST:
        raise F6R0LinuxAttestationError(f"F6 R0 {label} root is on a remote filesystem")
    return _RootProbe(
        label=label, filesystem_type=filesystem_type, free_bytes=free_bytes, mode=mode
    )


def _filesystem_type(path: Path) -> str:
    result = subprocess.run(
        ["findmnt", "--noheadings", "--output", "FSTYPE", "--target", str(path)],
        check=False,
        capture_output=True,
        text=True,
    )
    value = result.stdout.strip().splitlines()
    if result.returncode != 0 or len(value) != 1 or not value[0]:
        raise F6R0LinuxAttestationError("F6 R0 root filesystem type is unavailable")
    return value[0]


def _validate_systemd_unit(path: Path) -> Mapping[str, str]:
    raw = _require_regular_file(path, "systemd unit").read_text(encoding="utf-8")
    values: dict[str, str] = {}
    for line in raw.splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key] = value.strip()
    if not _REQUIRED_UNIT_KEYS.issubset(values):
        raise F6R0LinuxAttestationError("F6 R0 systemd unit lacks required hardening keys")
    if any(values[key].casefold() != expected for key, expected in _REQUIRED_UNIT_VALUES.items()):
        raise F6R0LinuxAttestationError("F6 R0 systemd unit hardening value differs")
    if "--disarmed" not in values["ExecStart"]:
        raise F6R0LinuxAttestationError("F6 R0 systemd unit is not explicitly DISARMED")
    return values


def _git_archive_sha256(repository: Path) -> str:
    if not repository.is_dir():
        raise F6R0LinuxAttestationError("F6 R0 repository is unavailable")
    status = subprocess.run(
        ["git", "-C", str(repository), "status", "--porcelain"],
        check=False,
        capture_output=True,
        text=True,
    )
    if status.returncode != 0 or not _only_non_source_untracked_files(status.stdout):
        raise F6R0LinuxAttestationError("F6 R0 source tree is not clean")
    process = subprocess.Popen(
        ["git", "-C", str(repository), "archive", "--format=tar", "HEAD"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    assert process.stdout is not None
    digest = sha256()
    while chunk := process.stdout.read(1_048_576):
        digest.update(chunk)
    _, stderr = process.communicate()
    if process.returncode != 0:
        raise F6R0LinuxAttestationError("F6 R0 source artifact archive failed") from RuntimeError(
            stderr.decode("utf-8", errors="replace")
        )
    return digest.hexdigest()


def _only_non_source_untracked_files(status_output: str) -> bool:
    """Allow only evidence/text diagnostics which cannot alter imported source.

    The target host retains historical acceptance evidence outside Git.  Git
    archive binds the tracked release exactly; accepting those non-source files
    avoids a false deployment block while still rejecting every tracked change
    and untracked executable/configuration source.
    """

    for line in status_output.splitlines():
        if not line.startswith("?? "):
            return False
        path = line[3:]
        if path.startswith("artifacts/") or path.endswith(".txt"):
            continue
        return False
    return True


def _sdk_binary_inventory_hash(sdk_root: Path) -> str:
    if not sdk_root.is_dir():
        raise F6R0LinuxAttestationError("F6 R0 EMT SDK root is unavailable")
    files = [
        path
        for path in sorted(sdk_root.rglob("*"))
        if path.is_file()
        and (path.suffix.casefold() in _NATIVE_SUFFIXES or ".so." in path.name.casefold())
    ]
    if not files:
        raise F6R0LinuxAttestationError("F6 R0 EMT SDK root has no native binary")
    return _canonical_hash(
        {path.relative_to(sdk_root).as_posix(): _sha256_file(path) for path in files}
    )


def _elf_abi_hash(native_extension: Path) -> str:
    _require_regular_file(native_extension, "native extension")
    result = subprocess.run(
        ["ldd", "--", str(native_extension)], check=False, capture_output=True, text=True
    )
    if result.returncode != 0 or "not found" in result.stdout.casefold():
        raise F6R0LinuxAttestationError("F6 R0 native extension ELF dependencies are unresolved")
    return sha256(_normalized_elf_dependencies(result.stdout).encode("utf-8")).hexdigest()


def _normalized_elf_dependencies(raw_output: str) -> str:
    """Keep ABI dependency identities while removing ASLR-dependent addresses."""

    normalized = [
        re.sub(r"\s*\(0x[0-9a-fA-F]+\)", "", line).strip()
        for line in raw_output.splitlines()
        if line.strip()
    ]
    return "\n".join(normalized) + "\n"


def _clock_is_synchronized() -> bool:
    result = subprocess.run(
        ["timedatectl", "show", "--property=NTPSynchronized", "--value"],
        check=False,
        capture_output=True,
        text=True,
    )
    return result.returncode == 0 and result.stdout.strip().casefold() == "yes"


def _require_regular_file(path: Path, label: str) -> Path:
    if path.is_symlink() or not path.is_file():
        raise F6R0LinuxAttestationError(f"F6 R0 {label} is not a regular file")
    return path


def _sha256_file(path: Path) -> str:
    file_path = _require_regular_file(path, "release asset")
    digest = sha256()
    with file_path.open("rb") as stream:
        while chunk := stream.read(1_048_576):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_hash(value: object) -> str:
    return sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    ).hexdigest()


def _common_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--repository", type=Path, required=True)
    parser.add_argument("--profile", action="append", default=[], required=True)
    parser.add_argument("--sdk-root", type=Path, required=True)
    parser.add_argument("--native-extension", type=Path, required=True)
    parser.add_argument("--systemd-unit", type=Path, required=True)
    parser.add_argument("--control-root", type=Path, required=True)
    parser.add_argument("--journal-root", type=Path, required=True)
    parser.add_argument("--archive-root", type=Path, required=True)
    parser.add_argument("--release-public-key", type=Path, required=True)
    parser.add_argument("--release-revocations", type=Path, required=True)
    parser.add_argument("--minimum-free-bytes", type=int, default=1_073_741_824)


def _collect_from_arguments(
    args: argparse.Namespace,
) -> tuple[ReleaseMaterialV1, dict[str, bool | int | str]]:
    return collect_release_material(
        repository=args.repository,
        profiles=_parse_profiles(args.profile),
        sdk_root=args.sdk_root,
        native_extension=args.native_extension,
        systemd_unit=args.systemd_unit,
        control_root=args.control_root,
        journal_root=args.journal_root,
        archive_root=args.archive_root,
        release_public_key=args.release_public_key,
        release_revocations=args.release_revocations,
        minimum_free_bytes=args.minimum_free_bytes,
    )


def main(argv: Sequence[str] | None = None) -> int:
    """Build a signable request or verify one signed F6 R0 release candidate."""

    parser = argparse.ArgumentParser(description="F6 R0 Linux release-host attestation")
    commands = parser.add_subparsers(dest="command", required=True)
    request = commands.add_parser(
        "request", help="measure material and write an unsigned release request"
    )
    _common_arguments(request)
    request.add_argument("--manifest-id", required=True)
    request.add_argument("--release-epoch", type=int, required=True)
    request.add_argument("--signer-id", required=True)
    request.add_argument("--valid-from-wall-ns", type=int, required=True)
    request.add_argument("--valid-until-wall-ns", type=int, required=True)
    request.add_argument("--output", type=Path, required=True)
    verify = commands.add_parser("verify", help="verify manifest signature and host material")
    _common_arguments(verify)
    verify.add_argument("--manifest", type=Path, required=True)
    verify.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    material, checks = _collect_from_arguments(args)
    if args.command == "request":
        unsigned = material.unsigned_manifest(
            manifest_id=args.manifest_id,
            release_epoch=args.release_epoch,
            signer_id=args.signer_id,
            valid_from_wall_ns=args.valid_from_wall_ns,
            valid_until_wall_ns=args.valid_until_wall_ns,
        )
        write_unsigned_request(request=unsigned, output=args.output)
        print("F6_R0_RELEASE_REQUEST=CREATED")
        return 0
    try:
        manifest = load_signed_release_manifest_v1_json(args.manifest.read_text(encoding="utf-8"))
        verify_release_manifest(
            manifest=manifest,
            public_key_path=args.release_public_key,
            revocations_path=args.release_revocations,
            now_wall_ns=time.time_ns(),
        )
        material.require_matches(manifest)
    except (F6ReleaseManifestToolError, OSError, ReleaseManifestActivationErrorV1) as exc:
        raise F6R0LinuxAttestationError("F6 R0 signed release verification failed") from exc
    if args.output.exists():
        raise F6R0LinuxAttestationError("F6 R0 evidence output already exists")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(build_evidence(manifest=manifest, checks=checks), ensure_ascii=False, indent=2)
        + "\n",
        encoding="utf-8",
    )
    print("F6_R0_RELEASE_ATTESTATION=PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
