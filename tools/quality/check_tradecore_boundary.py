"""Enforce the M0-01 TradeCore package and import boundary.

The checker is intentionally standard-library only so it can run before the
project environment is synchronized. It validates both Python imports and the
distribution/CLI identity declared in ``pyproject.toml``.
"""

from __future__ import annotations

import argparse
import ast
import json
import re
import sys
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

POLICY_PATH = Path(__file__).with_name("tradecore-import-policy.v1.json")


@dataclass(frozen=True, order=True)
class BoundaryViolation:
    """One deterministic package-boundary violation."""

    path: str
    line: int
    code: str
    detail: str

    def render(self) -> str:
        return f"{self.path}:{self.line}: {self.code} {self.detail}"


@dataclass(frozen=True)
class ImportPolicy:
    """Validated import and product-identity policy."""

    schema_version: int
    core_package: str
    legacy_package: str
    legacy_import_allowed_prefixes: tuple[str, ...]
    generic_layer_prefixes: tuple[str, ...]
    peer_adapter_prefixes: tuple[str, ...]
    vendor_import_roots: frozenset[str]
    required_distribution: str
    required_cli_name: str
    required_cli_target: str
    required_package_patterns: tuple[str, ...]

    @classmethod
    def load(cls, path: Path = POLICY_PATH) -> ImportPolicy:
        raw: Any = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            raise ValueError("import policy must be a JSON object")
        if raw.get("schema_version") != 1:
            raise ValueError("unsupported import policy schema_version")

        def required_string(name: str) -> str:
            value = raw.get(name)
            if not isinstance(value, str) or not value:
                raise ValueError(f"{name} must be a non-empty string")
            return value

        def string_tuple(name: str) -> tuple[str, ...]:
            value = raw.get(name)
            if not isinstance(value, list) or not value:
                raise ValueError(f"{name} must be a non-empty string array")
            if any(not isinstance(item, str) or not item for item in value):
                raise ValueError(f"{name} must contain only non-empty strings")
            return tuple(value)

        return cls(
            schema_version=1,
            core_package=required_string("core_package"),
            legacy_package=required_string("legacy_package"),
            legacy_import_allowed_prefixes=string_tuple("legacy_import_allowed_prefixes"),
            generic_layer_prefixes=string_tuple("generic_layer_prefixes"),
            peer_adapter_prefixes=string_tuple("peer_adapter_prefixes"),
            vendor_import_roots=frozenset(string_tuple("vendor_import_roots")),
            required_distribution=required_string("required_distribution"),
            required_cli_name=required_string("required_cli_name"),
            required_cli_target=required_string("required_cli_target"),
            required_package_patterns=string_tuple("required_package_patterns"),
        )


def _is_within(module: str, prefixes: Iterable[str]) -> bool:
    return any(module == prefix or module.startswith(f"{prefix}.") for prefix in prefixes)


def _module_name(core_root: Path, source: Path, core_package: str) -> tuple[str, bool]:
    relative = source.relative_to(core_root).with_suffix("")
    parts = list(relative.parts)
    is_package = parts[-1] == "__init__"
    if is_package:
        parts.pop()
    suffix = ".".join(parts)
    return (core_package if not suffix else f"{core_package}.{suffix}", is_package)


def _resolve_from_import(
    current_module: str,
    *,
    is_package: bool,
    imported_module: str | None,
    level: int,
) -> str:
    if level == 0:
        return imported_module or ""
    package_parts = current_module.split(".") if is_package else current_module.split(".")[:-1]
    remove_count = level - 1
    if remove_count > len(package_parts):
        return ""
    base_parts = package_parts[: len(package_parts) - remove_count]
    if imported_module:
        base_parts.extend(imported_module.split("."))
    return ".".join(base_parts)


def _imports(tree: ast.AST, current_module: str, *, is_package: bool) -> Iterable[tuple[str, int]]:
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                yield alias.name, node.lineno
        elif isinstance(node, ast.ImportFrom):
            resolved = _resolve_from_import(
                current_module,
                is_package=is_package,
                imported_module=node.module,
                level=node.level,
            )
            if resolved:
                yield resolved, node.lineno
                # Resolve `from ..adapters import qmt` as well as imports
                # naming the venue package directly, without duplicating
                # legacy/vendor violations for ordinary imported symbols.
                if resolved.endswith(".adapters"):
                    for alias in node.names:
                        yield f"{resolved}.{alias.name}", node.lineno


def scan_imports(repository_root: Path, policy: ImportPolicy) -> list[BoundaryViolation]:
    """Return all import violations under the configured core package."""

    core_root = repository_root / policy.core_package
    if not core_root.is_dir():
        return [
            BoundaryViolation(
                path=policy.core_package,
                line=1,
                code="TCB001",
                detail="required TradeCore package directory is missing",
            )
        ]

    violations: list[BoundaryViolation] = []
    for source in sorted(core_root.rglob("*.py")):
        relative_path = source.relative_to(repository_root).as_posix()
        module, is_package = _module_name(core_root, source, policy.core_package)
        try:
            tree = ast.parse(source.read_text(encoding="utf-8"), filename=relative_path)
        except (OSError, SyntaxError, UnicodeError) as exc:
            violations.append(
                BoundaryViolation(relative_path, 1, "TCB002", f"cannot parse source: {exc}")
            )
            continue

        for imported_module, line in _imports(tree, module, is_package=is_package):
            imported_root = imported_module.split(".", maxsplit=1)[0]
            target_peer = next(
                (
                    prefix
                    for prefix in policy.peer_adapter_prefixes
                    if _is_within(imported_module, (prefix,))
                ),
                None,
            )
            source_peer = next(
                (
                    prefix
                    for prefix in policy.peer_adapter_prefixes
                    if _is_within(module, (prefix,))
                ),
                None,
            )
            # Common adapter contracts must not re-export a venue implementation.
            common_adapter = (
                _is_within(module, (f"{policy.core_package}.adapters",)) and source_peer is None
            )
            if target_peer is not None and (
                _is_within(module, policy.generic_layer_prefixes) or common_adapter
            ):
                violations.append(
                    BoundaryViolation(
                        relative_path,
                        line,
                        "TCB104",
                        f"generic layer {module} must depend on BrokerAdapter, not {target_peer}",
                    )
                )
            if target_peer is not None and source_peer is not None and target_peer != source_peer:
                violations.append(
                    BoundaryViolation(
                        relative_path,
                        line,
                        "TCB105",
                        f"peer adapter {source_peer} cannot depend on {target_peer}",
                    )
                )
            if imported_root == policy.legacy_package and not _is_within(
                module, policy.legacy_import_allowed_prefixes
            ):
                violations.append(
                    BoundaryViolation(
                        relative_path,
                        line,
                        "TCB101",
                        f"{module} imports legacy package {policy.legacy_package}; "
                        "only xuanling_tradecore.compatibility may do so",
                    )
                )
            if _is_within(module, policy.generic_layer_prefixes):
                if imported_root in policy.vendor_import_roots:
                    violations.append(
                        BoundaryViolation(
                            relative_path,
                            line,
                            "TCB102",
                            f"vendor import {imported_root} is forbidden in generic layer {module}",
                        )
                    )
                if _is_within(imported_module, policy.legacy_import_allowed_prefixes):
                    violations.append(
                        BoundaryViolation(
                            relative_path,
                            line,
                            "TCB103",
                            f"generic layer {module} cannot depend on the compatibility layer",
                        )
                    )
    return sorted(violations)


def _toml_section(text: str, section_name: str) -> str | None:
    match = re.search(
        rf"(?ms)^\[{re.escape(section_name)}\]\s*\n(.*?)(?=^\[|\Z)",
        text,
    )
    return None if match is None else match.group(1)


def _quoted_assignment(section: str | None, key: str) -> str | None:
    if section is None:
        return None
    match = re.search(rf'(?m)^{re.escape(key)}\s*=\s*"([^"]+)"\s*$', section)
    return None if match is None else match.group(1)


def validate_metadata(repository_root: Path, policy: ImportPolicy) -> list[BoundaryViolation]:
    """Validate pyproject distribution, CLI, package discovery, and version identity."""

    pyproject = repository_root / "pyproject.toml"
    init_file = repository_root / policy.core_package / "__init__.py"
    if not pyproject.is_file():
        return [BoundaryViolation("pyproject.toml", 1, "TCB201", "pyproject.toml is missing")]

    text = pyproject.read_text(encoding="utf-8")
    project = _toml_section(text, "project")
    scripts = _toml_section(text, "project.scripts")
    package_find = _toml_section(text, "tool.setuptools.packages.find")
    violations: list[BoundaryViolation] = []

    if _quoted_assignment(project, "name") != policy.required_distribution:
        violations.append(
            BoundaryViolation(
                "pyproject.toml",
                1,
                "TCB202",
                f"project.name must be {policy.required_distribution!r}",
            )
        )
    if _quoted_assignment(scripts, policy.required_cli_name) != policy.required_cli_target:
        violations.append(
            BoundaryViolation(
                "pyproject.toml",
                1,
                "TCB203",
                f"CLI {policy.required_cli_name!r} must target {policy.required_cli_target!r}",
            )
        )
    for pattern in policy.required_package_patterns:
        if package_find is None or f'"{pattern}"' not in package_find:
            violations.append(
                BoundaryViolation(
                    "pyproject.toml",
                    1,
                    "TCB204",
                    f"package discovery must include {pattern!r}",
                )
            )

    project_version = _quoted_assignment(project, "version")
    init_text = init_file.read_text(encoding="utf-8") if init_file.is_file() else ""
    init_version_match = re.search(r'(?m)^__version__\s*=\s*"([^"]+)"\s*$', init_text)
    init_version = None if init_version_match is None else init_version_match.group(1)
    if project_version is None or init_version != project_version:
        violations.append(
            BoundaryViolation(
                init_file.relative_to(repository_root).as_posix()
                if init_file.is_file()
                else f"{policy.core_package}/__init__.py",
                1,
                "TCB205",
                "package __version__ must exactly match project.version",
            )
        )

    cli_module = policy.required_cli_target.split(":", maxsplit=1)[0]
    cli_file = repository_root / Path(*cli_module.split(".")).with_suffix(".py")
    if not cli_file.is_file():
        violations.append(
            BoundaryViolation(
                cli_file.relative_to(repository_root).as_posix(),
                1,
                "TCB206",
                "configured CLI module is missing",
            )
        )
    return sorted(violations)


def validate_repository(
    repository_root: Path,
    policy_path: Path = POLICY_PATH,
) -> list[BoundaryViolation]:
    policy = ImportPolicy.load(policy_path)
    return sorted(
        [
            *scan_imports(repository_root, policy),
            *validate_metadata(repository_root, policy),
        ]
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Validate the M0-01 TradeCore package boundary")
    parser.add_argument("--root", type=Path, default=Path.cwd(), help="repository root")
    parser.add_argument("--policy", type=Path, default=POLICY_PATH, help="policy JSON path")
    parser.add_argument("--format", choices=("text", "json"), default="text")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    arguments = _parser().parse_args(argv)
    root = arguments.root.resolve()
    violations = validate_repository(root, arguments.policy.resolve())
    if arguments.format == "json":
        print(
            json.dumps(
                {
                    "schema_version": 1,
                    "implementation_id": "M0-01",
                    "passed": not violations,
                    "violation_count": len(violations),
                    "violations": [violation.__dict__ for violation in violations],
                },
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            )
        )
    elif violations:
        for violation in violations:
            print(violation.render(), file=sys.stderr)
    else:
        print("M0-01 TradeCore package boundary: PASS")
    return 1 if violations else 0


if __name__ == "__main__":
    raise SystemExit(main())
