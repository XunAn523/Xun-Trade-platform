"""R1 EMT recovery-certificate collection from payload-free query terminals.

This is deliberately an Adapter-core module: it has no SDK dependency and it
does not retain an EMT row.  The platform bridge reduces every query callback
to terminal status, row/page counts and an opaque page cursor before this
collector sees it.  Consequently a completed certificate can prove *coverage*
and the exact terminal protocol without making the OMS a second store of
broker account data.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, replace
from hashlib import sha256
from typing import Mapping

from ...domain import (
    BrokerEvent,
    BrokerEventKind,
    BrokerSourceKind,
    RecoveryCompletenessCertificate,
    RecoveryEvidenceProfile,
    TradingDayEpoch,
)
from ...domain._validation import require_text


class EmtRecoveryCertificateErrorV1(RuntimeError):
    """A query terminal cannot form a fail-closed R1 certificate."""


_ORDINARY_REQUIRED_QUERY_KINDS = frozenset(
    {
        "orders_by_page",
        "unfinished_orders",
        "trades_by_page",
        # The recovery contract requires paged order and trade histories.
        # EMT's native manual advises a cursor scan for large position
        # snapshots. Recovery therefore does not treat a one-shot position
        # query as authority for an unbounded account.
        "positions_by_page",
        "assets",
        "funds",
    }
)
_CREDIT_REQUIRED_QUERY_KINDS = frozenset(
    {
        "credit_fund_info",
        "credit_asset_debt",
        "credit_position_extra_info_sz",
        "credit_position_extra_info_sh",
    }
)
_PAGED_QUERY_KINDS = frozenset({"orders_by_page", "trades_by_page", "positions_by_page"})


def emt_r1_recovery_evidence_profile_v1(*, account_profile: str) -> RecoveryEvidenceProfile:
    """Return the immutable R1 profile for an ordinary or credit EMT scope.

    Credit-only queries are not silently sent for an ordinary account.  A
    credit profile has the stricter query set and the live platform must still
    prove that the supplied binding exposes every named operation before it
    can produce a complete certificate.
    """

    if account_profile not in {"ordinary", "credit"}:
        raise EmtRecoveryCertificateErrorV1("R1 account profile must be ordinary or credit")
    required = _ORDINARY_REQUIRED_QUERY_KINDS
    if account_profile == "credit":
        required = required | _CREDIT_REQUIRED_QUERY_KINDS
    profile_id = f"emt-r1-authoritative-recovery-{account_profile}-v1"
    profile_payload = {
        "schema_version": 1,
        "profile_id": profile_id,
        "required_query_kinds": sorted(required),
        "requires_public_replay": True,
        "requires_callback_gap_coverage": True,
        "requires_route_token_echo": False,
        "requires_negative_confirmation": True,
        "requires_stable_double_scan_without_as_of": True,
        "max_snapshot_age_ns": 60_000_000_000,
    }
    profile_hash = sha256(
        json.dumps(profile_payload, sort_keys=True, separators=(",", ":")).encode("ascii")
    ).hexdigest()
    return RecoveryEvidenceProfile(
        profile_id=profile_id,
        version=1,
        profile_hash=profile_hash,
        required_query_kinds=required,
        requires_public_replay=True,
        requires_callback_gap_coverage=True,
        requires_route_token_echo=False,
        requires_negative_confirmation=True,
        requires_stable_double_scan_without_as_of=True,
        max_snapshot_age_ns=60_000_000_000,
    )


@dataclass(frozen=True)
class EmtRecoveryCertificateResultV1:
    """A certificate plus explicit coverage state, never a READY decision."""

    certificate: RecoveryCompletenessCertificate
    profile: RecoveryEvidenceProfile
    missing_query_kinds: tuple[str, ...]
    pending_paged_query_kinds: tuple[str, ...]
    negative_confirmation_complete: bool

    @property
    def incompleteness_reasons(self) -> tuple[str, ...]:
        return self.certificate.incompleteness_reasons(self.profile)


class EmtRecoveryCertificateCollectorV1:
    """Collect exactly one serial R1 query window from normalized terminals.

    ``record_terminal`` accepts only the existing payload-free ``BrokerEvent``
    representation.  A paged query stays incomplete until a terminal page
    reports ``page_has_more=false``.  Its next opaque cursor can be returned
    to the outer query worker through :meth:`next_continuation`; the collector
    itself has neither an SDK handle nor authority to issue a query.
    """

    def __init__(
        self,
        *,
        certificate_id: str,
        account_id: str,
        adapter_instance_id: str,
        adapter_client_scope: str,
        trading_day: TradingDayEpoch,
        account_profile: str,
        captured_at_wall_ns: int | None = None,
    ) -> None:
        require_text(certificate_id, "R1 recovery certificate_id", max_length=128)
        require_text(account_id, "R1 recovery account_id", max_length=128)
        require_text(adapter_instance_id, "R1 recovery adapter_instance_id", max_length=128)
        require_text(adapter_client_scope, "R1 recovery adapter_client_scope", max_length=128)
        self._profile = emt_r1_recovery_evidence_profile_v1(account_profile=account_profile)
        self._certificate_id = certificate_id
        self._account_id = account_id
        self._adapter_instance_id = adapter_instance_id
        self._adapter_client_scope = adapter_client_scope
        self._trading_day = trading_day
        self._captured_at_wall_ns = (
            time.time_ns() if captured_at_wall_ns is None else captured_at_wall_ns
        )
        if self._captured_at_wall_ns < 0:
            raise EmtRecoveryCertificateErrorV1("R1 certificate capture time is invalid")
        self._page_digests: dict[str, list[str]] = {
            kind: [] for kind in self._profile.required_query_kinds
        }
        self._seen_page_references: dict[str, set[int]] = {
            kind: set()
            for kind in _PAGED_QUERY_KINDS.intersection(self._profile.required_query_kinds)
        }
        self._complete_kinds: set[str] = set()
        self._next_continuations: dict[str, int] = {}
        self._unfinished_result_count: int | None = None

    @property
    def required_query_kinds(self) -> frozenset[str]:
        return self._profile.required_query_kinds

    def next_continuation(self, query_kind: str) -> int | None:
        """Return the next approved cursor only while its series is incomplete."""

        self._require_kind(query_kind)
        return self._next_continuations.get(query_kind)

    def record_terminal(self, event: BrokerEvent) -> None:
        """Record one terminal callback summary and reject malformed coverage."""

        self._require_event_identity(event)
        summary = event.normalized_summary
        query_kind = summary.get("query_kind")
        if type(query_kind) is not str:
            raise EmtRecoveryCertificateErrorV1("R1 query terminal lacks query kind")
        self._require_kind(query_kind)
        if query_kind in self._complete_kinds:
            raise EmtRecoveryCertificateErrorV1(
                "R1 query series received a terminal after completion"
            )
        if summary.get("terminal") != "true" or summary.get("error_id") != "0":
            raise EmtRecoveryCertificateErrorV1("R1 query terminal is unsuccessful")

        canonical_summary = self._canonical_summary(summary, query_kind=query_kind)
        self._page_digests[query_kind].append(sha256(canonical_summary.encode("ascii")).hexdigest())
        if query_kind in _PAGED_QUERY_KINDS:
            page_reference = self._positive_or_zero(
                summary.get("page_reference"), "R1 paged query reference"
            )
            page_has_more = summary.get("page_has_more")
            if page_has_more not in {"true", "false"}:
                raise EmtRecoveryCertificateErrorV1("R1 paged query lacks terminal page state")
            seen = self._seen_page_references[query_kind]
            if page_reference in seen:
                raise EmtRecoveryCertificateErrorV1("R1 paged query repeated a cursor")
            seen.add(page_reference)
            if page_has_more == "true":
                self._next_continuations[query_kind] = page_reference
            else:
                self._next_continuations.pop(query_kind, None)
                self._complete_kinds.add(query_kind)
            return

        result_count = self._positive_or_zero(
            summary.get("result_count"), "R1 non-page query result_count"
        )
        if query_kind == "unfinished_orders":
            self._unfinished_result_count = result_count
        self._complete_kinds.add(query_kind)

    def build(
        self,
        *,
        callback_replay_complete: bool,
        callback_gaps_closed: bool,
        stable_double_scan: bool,
        route_token_echo_complete: bool = False,
        completed_at_wall_ns: int | None = None,
    ) -> EmtRecoveryCertificateResultV1:
        """Seal a certificate without treating missing evidence as a pass."""

        if type(callback_replay_complete) is not bool:
            raise EmtRecoveryCertificateErrorV1("R1 callback replay state must be boolean")
        if type(callback_gaps_closed) is not bool:
            raise EmtRecoveryCertificateErrorV1("R1 callback gap state must be boolean")
        if type(stable_double_scan) is not bool:
            raise EmtRecoveryCertificateErrorV1("R1 double scan state must be boolean")
        if type(route_token_echo_complete) is not bool:
            raise EmtRecoveryCertificateErrorV1("R1 route token state must be boolean")
        completed = time.time_ns() if completed_at_wall_ns is None else completed_at_wall_ns
        if completed < self._captured_at_wall_ns:
            raise EmtRecoveryCertificateErrorV1("R1 certificate completion precedes capture")
        missing = tuple(sorted(self._profile.required_query_kinds - self._complete_kinds))
        query_digests = {
            kind: self._series_digest(kind)
            for kind in sorted(self._page_digests)
            if self._page_digests[kind]
        }
        certificate = RecoveryCompletenessCertificate(
            certificate_id=self._certificate_id,
            certificate_hash="0" * 64,
            profile_id=self._profile.profile_id,
            profile_hash=self._profile.profile_hash,
            account_id=self._account_id,
            venue_id="EMT",
            adapter_instance_id=self._adapter_instance_id,
            adapter_client_scope=self._adapter_client_scope,
            trading_day=self._trading_day,
            query_digests=query_digests,
            callback_replay_complete=callback_replay_complete,
            callback_gaps_closed=callback_gaps_closed,
            route_token_echo_complete=route_token_echo_complete,
            negative_confirmation_available=self._unfinished_result_count == 0,
            snapshot_window_stable=stable_double_scan,
            captured_at_wall_ns=self._captured_at_wall_ns,
            completed_at_wall_ns=completed,
        )
        certificate = replace(certificate, certificate_hash=certificate.content_sha256)
        return EmtRecoveryCertificateResultV1(
            certificate=certificate,
            profile=self._profile,
            missing_query_kinds=missing,
            pending_paged_query_kinds=tuple(sorted(self._next_continuations)),
            negative_confirmation_complete=self._unfinished_result_count == 0,
        )

    def _require_event_identity(self, event: BrokerEvent) -> None:
        if (
            event.event_kind is not BrokerEventKind.QUERY
            or event.source_kind is not BrokerSourceKind.CALLBACK
            or event.account_id != self._account_id
            or event.venue_id != "EMT"
            or event.adapter_instance_id != self._adapter_instance_id
            or event.trading_day != self._trading_day
        ):
            raise EmtRecoveryCertificateErrorV1("R1 query terminal belongs to another scope")

    def _require_kind(self, query_kind: str) -> None:
        if query_kind not in self._profile.required_query_kinds:
            raise EmtRecoveryCertificateErrorV1("R1 query kind is not required by this profile")

    @staticmethod
    def _positive_or_zero(value: object, label: str) -> int:
        if type(value) is not str or not value.isdecimal():
            raise EmtRecoveryCertificateErrorV1(f"{label} is invalid")
        return int(value)

    @staticmethod
    def _canonical_summary(summary: Mapping[str, str], *, query_kind: str) -> str:
        allowed = {
            "callback_name",
            "query_kind",
            "terminal",
            "error_id",
            "result_count",
            "page_reference",
            "page_result_count",
            "page_has_more",
        }
        if any(type(key) is not str or type(value) is not str for key, value in summary.items()):
            raise EmtRecoveryCertificateErrorV1("R1 query summary contains non-text metadata")
        if set(summary) - allowed:
            raise EmtRecoveryCertificateErrorV1("R1 query summary contains unapproved metadata")
        if summary.get("query_kind") != query_kind:
            raise EmtRecoveryCertificateErrorV1("R1 query summary kind is inconsistent")
        return json.dumps(dict(summary), sort_keys=True, separators=(",", ":"), ensure_ascii=True)

    def _series_digest(self, query_kind: str) -> str:
        return sha256("\x00".join(self._page_digests[query_kind]).encode("ascii")).hexdigest()
