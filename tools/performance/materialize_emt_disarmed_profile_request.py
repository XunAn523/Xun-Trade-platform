"""Materialize a local three-scope DISARMED request without emitting account data."""

from __future__ import annotations

import argparse
import copy
import json
import time
from hashlib import sha256
from pathlib import Path
from typing import Mapping, Sequence

if __package__:
    from .perfbench.simulated_accounts import (
        AccountConfigurationError,
        SimulatedAccounts,
        load_simulated_accounts,
    )
else:
    from perfbench.simulated_accounts import (  # type: ignore[reportMissingImports]  # Script mode.
        AccountConfigurationError,
        SimulatedAccounts,
        load_simulated_accounts,
    )
from xuanling_tradecore.adapters.emt import EmtDisarmedProfileError

_ACCOUNT_PROFILES = ("ordinary", "credit", "option")


def materialize_disarmed_profile_request(
    *,
    template_path: Path,
    accounts_path: Path,
    output_path: Path,
    now_wall_ns: int,
) -> None:
    """Fill account identifiers locally, leaving all scope probes unsatisfied.

    The account parser returns credentials as part of its existing local model,
    but this function only copies the account identifier into the ignored
    request; passwords are neither serialized nor returned to callers.
    """

    if output_path.exists():
        raise EmtDisarmedProfileError("EMT DISARMED request materializer refuses to replace output")
    root = _read_template(template_path)
    try:
        accounts = load_simulated_accounts(accounts_path)
    except AccountConfigurationError as exc:
        raise EmtDisarmedProfileError(
            "EMT local simulated-account configuration is invalid"
        ) from exc
    materialized = copy.deepcopy(root)
    materialized["template"] = False
    materialized["scope_ownerships"] = _materialized_scopes(
        _scope_objects(root.get("scope_ownerships")),
        accounts=accounts,
        now_wall_ns=now_wall_ns,
    )
    materialized["instructions"] = (
        "Materialized locally from simulated-account configuration. "
        "uniqueness_probe_succeeded remains false until a separate approved probe succeeds."
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("x", encoding="utf-8") as file:
        json.dump(materialized, file, ensure_ascii=False, indent=2)
        file.write("\n")


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Materialize a local three-scope EMT DISARMED signing request"
    )
    parser.add_argument("--template", type=Path, required=True)
    parser.add_argument("--accounts-file", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    materialize_disarmed_profile_request(
        template_path=args.template,
        accounts_path=args.accounts_file,
        output_path=args.output,
        now_wall_ns=time.time_ns(),
    )
    print("EMT_DISARMED_REQUEST_MATERIALIZED=3")
    return 0


def _read_template(path: Path) -> Mapping[str, object]:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise EmtDisarmedProfileError("EMT DISARMED request template is unreadable") from exc
    if not isinstance(raw, dict):
        raise EmtDisarmedProfileError("EMT DISARMED request template must be an object")
    if raw.get("schema_version") != 1 or raw.get("template") is not True:
        raise EmtDisarmedProfileError("EMT request materializer requires a schema v1 template")
    approval = raw.get("approval")
    if not isinstance(approval, dict) or approval != {"approved": False, "lifecycle": "DISARMED"}:
        raise EmtDisarmedProfileError("EMT request template must remain DISARMED")
    return raw


def _scope_objects(value: object) -> tuple[Mapping[str, object], ...]:
    if not isinstance(value, list) or len(value) != len(_ACCOUNT_PROFILES):
        raise EmtDisarmedProfileError(
            "EMT request template requires exactly three scope ownerships"
        )
    scopes: list[Mapping[str, object]] = []
    for item in value:
        if not isinstance(item, dict):
            raise EmtDisarmedProfileError("EMT request template scope ownership must be an object")
        scopes.append(item)
    return tuple(scopes)


def _materialized_scopes(
    scopes: Sequence[Mapping[str, object]],
    *,
    accounts: SimulatedAccounts,
    now_wall_ns: int,
) -> list[dict[str, object]]:
    by_profile = {
        "ordinary": accounts.ordinary.account,
        "credit": accounts.credit.account,
        "option": accounts.option.account,
    }
    materialized: list[dict[str, object]] = []
    seen_profiles: set[str] = set()
    for scope in scopes:
        mutable = dict(scope)
        profile = _profile_for_placeholder(mutable.get("account_id"))
        if profile in seen_profiles:
            raise EmtDisarmedProfileError("EMT request template repeats an account profile")
        seen_profiles.add(profile)
        _validate_validity(mutable, now_wall_ns)
        mutable["account_id"] = by_profile[profile]
        source_hash = _scope_hash(mutable, prefix="config-source")
        mutable["config_source_hash"] = source_hash
        mutable["uniqueness_probe_hash"] = _scope_hash(mutable, prefix="uniqueness-unprobed")
        mutable["uniqueness_probe_succeeded"] = False
        materialized.append(mutable)
    if seen_profiles != set(_ACCOUNT_PROFILES):
        raise EmtDisarmedProfileError("EMT request template must cover ordinary, credit and option")
    return materialized


def _profile_for_placeholder(value: object) -> str:
    if not isinstance(value, str):
        raise EmtDisarmedProfileError("EMT request template account placeholder must be text")
    for profile in _ACCOUNT_PROFILES:
        if value == f"__REPLACE_{profile.upper()}_ACCOUNT_ID__":
            return profile
    raise EmtDisarmedProfileError("EMT request template has an unexpected account placeholder")


def _validate_validity(scope: Mapping[str, object], now_wall_ns: int) -> None:
    start = scope.get("valid_from_wall_ns")
    end = scope.get("valid_until_wall_ns")
    if (
        isinstance(start, bool)
        or isinstance(end, bool)
        or not isinstance(start, int)
        or not isinstance(end, int)
        or not start <= now_wall_ns < end
    ):
        raise EmtDisarmedProfileError("EMT request template validity window is not active")


def _scope_hash(scope: Mapping[str, object], *, prefix: str) -> str:
    protected = {
        name: value
        for name, value in scope.items()
        if name not in {"config_source_hash", "uniqueness_probe_hash", "uniqueness_probe_succeeded"}
    }
    canonical = json.dumps(
        {"purpose": prefix, "scope": protected},
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )
    return sha256(canonical.encode("utf-8")).hexdigest()


if __name__ == "__main__":
    raise SystemExit(main())
