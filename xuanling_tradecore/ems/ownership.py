"""Stable, fail-closed Linux ownership fence for one account Adapter scope.

The fence is deliberately independent of every Journal generation.  Holding it
is only a prerequisite for creating an SDK session; it never arms an account or
authorizes order flow by itself.
"""

from __future__ import annotations

import errno
import json
import os
import stat
import sys
import unicodedata
from dataclasses import asdict, dataclass
from hashlib import sha256
from pathlib import Path
from typing import Final

_SCOPE_HASH_DOMAIN: Final = b"xuanling-account-ownership-fence-v1\x00"
_LOCK_FILE_NAME: Final = "ownership.lock"
_DIAGNOSTIC_SCHEMA_VERSION: Final = 1
_MAX_DIAGNOSTIC_BYTES: Final = 16_384
_REMOTE_FILESYSTEM_TYPES: Final = frozenset(
    {
        "9p",
        "afs",
        "ceph",
        "cifs",
        "fuse.ceph",
        "fuse.glusterfs",
        "fuse.sshfs",
        "glusterfs",
        "nfs",
        "nfs4",
        "smb3",
    }
)


class AccountOwnershipFenceError(RuntimeError):
    """Base class for ownership-fence failures."""


class AccountOwnershipFenceUnsupportedError(AccountOwnershipFenceError):
    """The host cannot provide the required Linux flock contract."""


class AccountOwnershipFenceIntegrityError(AccountOwnershipFenceError):
    """The control-root layout or held fence cannot be proven safe."""


class AccountOwnershipFenceContendedError(AccountOwnershipFenceError):
    """Another local process currently owns the account scope."""

    def __init__(self, scope_hash: str, incumbent: FenceDiagnosticV1 | None) -> None:
        super().__init__(f"account ownership fence is already held for scope {scope_hash}")
        self.scope_hash = scope_hash
        self.incumbent = incumbent


def _untrusted(value: object) -> object:
    """Prevent static contract types from replacing validation of decoded JSON."""

    return value


def _canonical_text(value: object, field_name: str, *, max_bytes: int = 256) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{field_name} must be non-empty text")
    if value != value.strip():
        raise ValueError(f"{field_name} must not contain surrounding whitespace")
    if unicodedata.normalize("NFC", value) != value:
        raise ValueError(f"{field_name} must use Unicode NFC")
    if any(ord(character) < 0x20 or ord(character) == 0x7F for character in value):
        raise ValueError(f"{field_name} must not contain control characters")
    if len(value.encode("utf-8")) > max_bytes:
        raise ValueError(f"{field_name} exceeds {max_bytes} UTF-8 bytes")
    return value


@dataclass(frozen=True)
class AccountFenceScope:
    """Canonical identity of one account's native Adapter client namespace."""

    account_id: str
    venue_id: str
    adapter_client_scope: str

    def __post_init__(self) -> None:
        _canonical_text(self.account_id, "account_id")
        venue_id = _canonical_text(self.venue_id, "venue_id", max_bytes=32)
        _canonical_text(self.adapter_client_scope, "adapter_client_scope")
        if venue_id.upper() != venue_id:
            raise ValueError("venue_id must use canonical uppercase text")

    @property
    def scope_hash(self) -> str:
        return canonical_account_scope_hash(self)


def canonical_account_scope_hash(scope: AccountFenceScope) -> str:
    """Derive a stable, irreversible identifier without delimiter ambiguity."""

    digest = sha256(_SCOPE_HASH_DOMAIN)
    for field_name, value in (
        ("account_id", scope.account_id),
        ("venue_id", scope.venue_id),
        ("adapter_client_scope", scope.adapter_client_scope),
    ):
        name_bytes = field_name.encode("ascii")
        value_bytes = value.encode("utf-8")
        digest.update(len(name_bytes).to_bytes(2, "big"))
        digest.update(name_bytes)
        digest.update(len(value_bytes).to_bytes(4, "big"))
        digest.update(value_bytes)
    return digest.hexdigest()


@dataclass(frozen=True)
class FenceDiagnosticV1:
    """Bounded diagnostic record stored inside the locked inode."""

    schema_version: int
    scope_hash: str
    instance_id: str
    pid: int
    host_id: str
    process_started_monotonic_ns: int
    acquired_monotonic_ns: int
    journal_id: str
    journal_root: str
    stream_generation: int
    filesystem_type: str
    startup_disposition: str = "DISARMED_REPLAY_RECONCILIATION_MANUAL_READY_REQUIRED"

    def to_bytes(self) -> bytes:
        encoded = json.dumps(
            asdict(self),
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("ascii")
        if len(encoded) > _MAX_DIAGNOSTIC_BYTES:
            raise AccountOwnershipFenceIntegrityError("ownership diagnostic exceeds its bound")
        return encoded + b"\n"

    @classmethod
    def from_bytes(cls, raw: bytes) -> FenceDiagnosticV1:
        if not raw or len(raw) > _MAX_DIAGNOSTIC_BYTES:
            raise AccountOwnershipFenceIntegrityError("ownership diagnostic has invalid size")
        try:
            values = json.loads(raw)
            diagnostic = cls(**values)
        except (json.JSONDecodeError, TypeError, UnicodeError) as exc:
            raise AccountOwnershipFenceIntegrityError("ownership diagnostic is invalid") from exc
        schema_version = _untrusted(diagnostic.schema_version)
        scope_hash = _untrusted(diagnostic.scope_hash)
        if type(schema_version) is not int or schema_version != _DIAGNOSTIC_SCHEMA_VERSION:
            raise AccountOwnershipFenceIntegrityError(
                "ownership diagnostic schema version is unsupported"
            )
        if (
            not isinstance(scope_hash, str)
            or len(scope_hash) != 64
            or any(character not in "0123456789abcdef" for character in scope_hash)
        ):
            raise AccountOwnershipFenceIntegrityError("ownership diagnostic scope hash is invalid")
        text_fields = (
            _untrusted(diagnostic.instance_id),
            _untrusted(diagnostic.host_id),
            _untrusted(diagnostic.journal_id),
            _untrusted(diagnostic.journal_root),
            _untrusted(diagnostic.filesystem_type),
            _untrusted(diagnostic.startup_disposition),
        )
        counter_fields = (
            _untrusted(diagnostic.pid),
            _untrusted(diagnostic.process_started_monotonic_ns),
            _untrusted(diagnostic.acquired_monotonic_ns),
            _untrusted(diagnostic.stream_generation),
        )
        if not all(isinstance(value, str) and value for value in text_fields):
            raise AccountOwnershipFenceIntegrityError("ownership diagnostic text is invalid")
        if not all(
            isinstance(value, int) and not isinstance(value, bool) for value in counter_fields
        ):
            raise AccountOwnershipFenceIntegrityError("ownership diagnostic counters are invalid")
        if diagnostic.pid <= 0 or diagnostic.stream_generation <= 0:
            raise AccountOwnershipFenceIntegrityError("ownership diagnostic counters are invalid")
        return diagnostic


@dataclass(frozen=True)
class OwnershipFenceProof:
    """Process-bound proof; it is intentionally not an account-ready grant."""

    scope_hash: str
    instance_id: str
    owner_pid: int
    acquired_monotonic_ns: int
    requires_replay_reconciliation_and_manual_ready: bool = True
    authorizes_ready: bool = False


def _path_contains(parent: Path, child: Path) -> bool:
    try:
        child.relative_to(parent)
    except ValueError:
        return False
    return True


def _decode_mount_path(value: str) -> str:
    for escaped, decoded in (("\\040", " "), ("\\011", "\t"), ("\\012", "\n"), ("\\134", "\\")):
        value = value.replace(escaped, decoded)
    return value


def _linux_filesystem_type(path: Path) -> str:
    try:
        lines = Path("/proc/self/mountinfo").read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        raise AccountOwnershipFenceIntegrityError("cannot inspect Linux mount topology") from exc
    resolved = path.resolve(strict=True)
    best_mount: Path | None = None
    best_type: str | None = None
    for line in lines:
        left, separator, right = line.partition(" - ")
        if not separator:
            continue
        left_fields = left.split()
        right_fields = right.split()
        if len(left_fields) < 5 or not right_fields:
            continue
        mount_path = Path(_decode_mount_path(left_fields[4]))
        if _path_contains(mount_path, resolved) and (
            best_mount is None or len(mount_path.parts) > len(best_mount.parts)
        ):
            best_mount = mount_path
            best_type = right_fields[0]
    if best_type is None:
        raise AccountOwnershipFenceIntegrityError("control root has no Linux mount record")
    if best_type in _REMOTE_FILESYSTEM_TYPES:
        raise AccountOwnershipFenceIntegrityError(
            f"control root filesystem {best_type!r} is not a supported local filesystem"
        )
    return best_type


def _flock_exclusive_nonblocking(fd: int) -> None:
    import fcntl

    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)


def _flock_unlock(fd: int) -> None:
    import fcntl

    fcntl.flock(fd, fcntl.LOCK_UN)


def _validate_directory(path: Path, *, exact_mode: int | None) -> os.stat_result:
    try:
        metadata = path.lstat()
    except OSError as exc:
        raise AccountOwnershipFenceIntegrityError(
            f"required control directory is unavailable: {path}"
        ) from exc
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
        raise AccountOwnershipFenceIntegrityError(f"control path is not a real directory: {path}")
    if metadata.st_uid != os.geteuid():
        raise AccountOwnershipFenceIntegrityError(
            f"control directory is not owned by the service uid: {path}"
        )
    mode = stat.S_IMODE(metadata.st_mode)
    if exact_mode is not None and mode != exact_mode:
        raise AccountOwnershipFenceIntegrityError(
            f"account control directory mode must be {oct(exact_mode)}, got {oct(mode)}"
        )
    if exact_mode is None and mode & 0o022:
        raise AccountOwnershipFenceIntegrityError(
            f"control directory must not be group/world writable: {path}"
        )
    return metadata


class AccountOwnershipFence:
    """Own one stable account scope through ``flock(LOCK_EX | LOCK_NB)``."""

    def __init__(
        self,
        *,
        runtime_control_root: Path,
        scope: AccountFenceScope,
        instance_id: str,
        host_id: str,
        process_started_monotonic_ns: int,
        journal_id: str,
        journal_root: Path,
        stream_generation: int,
    ) -> None:
        if not runtime_control_root.is_absolute():
            raise ValueError("runtime_control_root must be an absolute Path")
        if not journal_root.is_absolute():
            raise ValueError("journal_root must be an absolute Path")
        self._runtime_control_root = runtime_control_root
        self._scope = scope
        self._instance_id = _canonical_text(instance_id, "instance_id")
        self._host_id = _canonical_text(host_id, "host_id")
        self._journal_id = _canonical_text(journal_id, "journal_id")
        if process_started_monotonic_ns < 0:
            raise ValueError("process_started_monotonic_ns must be a nonnegative integer")
        if stream_generation <= 0:
            raise ValueError("stream_generation must be a positive integer")
        self._process_started_monotonic_ns = process_started_monotonic_ns
        self._journal_root = journal_root
        self._stream_generation = stream_generation
        self._fd: int | None = None
        self._owner_pid: int | None = None
        self._locked_device_inode: tuple[int, int] | None = None
        self._proof: OwnershipFenceProof | None = None

    @property
    def scope_hash(self) -> str:
        return self._scope.scope_hash

    @property
    def account_control_root(self) -> Path:
        return self._runtime_control_root / "accounts" / self.scope_hash

    @property
    def lock_path(self) -> Path:
        return self.account_control_root / _LOCK_FILE_NAME

    @property
    def held(self) -> bool:
        return self._fd is not None and self._owner_pid == os.getpid()

    def _validate_layout(self) -> str:
        control_root = self._runtime_control_root
        accounts_root = control_root / "accounts"
        account_root = accounts_root / self.scope_hash
        control_stat = _validate_directory(control_root, exact_mode=None)
        accounts_stat = _validate_directory(accounts_root, exact_mode=None)
        account_stat = _validate_directory(account_root, exact_mode=0o700)
        resolved_control_root = control_root.resolve(strict=True)
        resolved_account_root = account_root.resolve(strict=True)
        if (
            resolved_control_root != control_root.absolute()
            or resolved_account_root != account_root.absolute()
        ):
            raise AccountOwnershipFenceIntegrityError(
                "runtime control paths must not traverse symbolic links"
            )
        journal_root = self._journal_root.resolve(strict=False)
        if _path_contains(journal_root, resolved_account_root) or _path_contains(
            resolved_account_root, journal_root
        ):
            raise AccountOwnershipFenceIntegrityError(
                "account control root and Journal root must be independent"
            )
        if len({control_stat.st_dev, accounts_stat.st_dev, account_stat.st_dev}) != 1:
            raise AccountOwnershipFenceIntegrityError(
                "runtime and account control directories must share one local device"
            )
        return _linux_filesystem_type(account_root)

    def acquire(self, *, acquired_monotonic_ns: int) -> OwnershipFenceProof:
        """Acquire without waiting and persist a bounded diagnostic after locking."""

        if sys.platform != "linux":
            raise AccountOwnershipFenceUnsupportedError(
                "AccountOwnershipFence requires Linux flock semantics"
            )
        if self._fd is not None:
            raise AccountOwnershipFenceIntegrityError("ownership fence instance is already open")
        if acquired_monotonic_ns < 0:
            raise ValueError("acquired_monotonic_ns must be a nonnegative integer")
        if acquired_monotonic_ns < self._process_started_monotonic_ns:
            raise ValueError("acquisition time precedes process start")

        filesystem_type = self._validate_layout()
        lock_path = self.lock_path
        existed_before_open = lock_path.exists()
        flags = os.O_RDWR | os.O_CREAT | os.O_CLOEXEC | os.O_NOFOLLOW
        try:
            fd = os.open(lock_path, flags, 0o600)
        except OSError as exc:
            raise AccountOwnershipFenceIntegrityError("cannot open ownership lock inode") from exc
        try:
            metadata = os.fstat(fd)
            if not stat.S_ISREG(metadata.st_mode):
                raise AccountOwnershipFenceIntegrityError("ownership lock must be a regular file")
            if (
                metadata.st_uid != os.geteuid()
                or metadata.st_dev != self.account_control_root.stat().st_dev
            ):
                raise AccountOwnershipFenceIntegrityError(
                    "ownership lock owner or device does not match the account control root"
                )
            if existed_before_open and stat.S_IMODE(metadata.st_mode) != 0o600:
                raise AccountOwnershipFenceIntegrityError(
                    "existing ownership lock mode must be exactly 0o600"
                )
            if not existed_before_open:
                os.fchmod(fd, 0o600)
            try:
                _flock_exclusive_nonblocking(fd)
            except OSError as exc:
                if exc.errno not in (errno.EACCES, errno.EAGAIN):
                    raise AccountOwnershipFenceIntegrityError(
                        "ownership flock failed unexpectedly"
                    ) from exc
                incumbent = self._read_incumbent(fd)
                raise AccountOwnershipFenceContendedError(self.scope_hash, incumbent) from exc

            diagnostic = FenceDiagnosticV1(
                schema_version=_DIAGNOSTIC_SCHEMA_VERSION,
                scope_hash=self.scope_hash,
                instance_id=self._instance_id,
                pid=os.getpid(),
                host_id=self._host_id,
                process_started_monotonic_ns=self._process_started_monotonic_ns,
                acquired_monotonic_ns=acquired_monotonic_ns,
                journal_id=self._journal_id,
                journal_root=str(self._journal_root),
                stream_generation=self._stream_generation,
                filesystem_type=filesystem_type,
            )
            encoded = diagnostic.to_bytes()
            os.lseek(fd, 0, os.SEEK_SET)
            os.ftruncate(fd, 0)
            written = os.write(fd, encoded)
            if written != len(encoded):
                raise AccountOwnershipFenceIntegrityError(
                    "ownership diagnostic write was incomplete"
                )
            os.fsync(fd)
            self._fd = fd
            self._owner_pid = os.getpid()
            self._locked_device_inode = (metadata.st_dev, metadata.st_ino)
            self._proof = OwnershipFenceProof(
                scope_hash=self.scope_hash,
                instance_id=self._instance_id,
                owner_pid=os.getpid(),
                acquired_monotonic_ns=acquired_monotonic_ns,
            )
            return self._proof
        except BaseException:
            try:
                _flock_unlock(fd)
            except OSError:
                pass
            os.close(fd)
            raise

    def _read_incumbent(self, fd: int) -> FenceDiagnosticV1 | None:
        try:
            os.lseek(fd, 0, os.SEEK_SET)
            raw = os.read(fd, _MAX_DIAGNOSTIC_BYTES + 1)
            incumbent = FenceDiagnosticV1.from_bytes(raw)
        except (OSError, AccountOwnershipFenceIntegrityError):
            return None
        if incumbent.scope_hash != self.scope_hash:
            return None
        return incumbent

    def require_held(self) -> OwnershipFenceProof:
        """Revalidate process and inode identity before an Adapter side effect."""

        if self._fd is None or self._proof is None or self._owner_pid != os.getpid():
            raise AccountOwnershipFenceIntegrityError(
                "current process does not hold this ownership fence"
            )
        try:
            opened = os.fstat(self._fd)
            named = self.lock_path.lstat()
        except OSError as exc:
            raise AccountOwnershipFenceIntegrityError(
                "ownership lock inode can no longer be verified"
            ) from exc
        actual = (opened.st_dev, opened.st_ino)
        named_actual = (named.st_dev, named.st_ino)
        if (
            actual != self._locked_device_inode
            or named_actual != actual
            or stat.S_ISLNK(named.st_mode)
        ):
            raise AccountOwnershipFenceIntegrityError("ownership lock inode identity changed")
        return self._proof

    def release(self) -> None:
        """Release only from the process that acquired the fence."""

        if self._fd is None:
            return
        if self._owner_pid != os.getpid():
            raise AccountOwnershipFenceIntegrityError(
                "a forked or foreign process cannot release the ownership fence"
            )
        fd = self._fd
        self._fd = None
        self._proof = None
        self._owner_pid = None
        self._locked_device_inode = None
        try:
            _flock_unlock(fd)
        finally:
            os.close(fd)

    def __enter__(self) -> AccountOwnershipFence:
        if not self.held:
            raise AccountOwnershipFenceIntegrityError(
                "acquire must succeed before entering the fence context"
            )
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.release()
