from hashlib import sha256

import pytest
from xuanling_tradecore.consumer.broker_diagnostic_trace import BrokerDiagnosticTrace
from xuanling_tradecore.consumer.owner_ipc_v1 import OwnerProtocolError

from test_owner_reactor_ipc import command


class Sink:
    def __init__(self) -> None:
        self.records: list[dict[str, object]] = []

    def submit(self, record: dict[str, object]) -> None:
        self.records.append(record)


def test_broker_trace_redacts_identity_and_records_bounded_send():
    request = command()
    sink = Sink()
    trace = BrokerDiagnosticTrace(sink, request.identity.generation)
    trace.ingested(request, 2, 10, 12, 1)
    with pytest.raises(OwnerProtocolError):
        trace.require_drained()
    trace.sent(request.identity.serial, 15, 18)
    trace.require_drained()
    assert sink.records == [{
        "scope": "candidate_broker_handoff",
        "generation": request.identity.generation,
        "serial": request.identity.serial,
        "request_id_sha256": sha256(request.identity.request_id.encode()).hexdigest(),
        "operation": "SubmitSignal",
        "worker_id": 2,
        "broker_outstanding_before": 1,
        "broker_frontend_received_ns": 10,
        "broker_owner_offered_ns": 12,
        "broker_final_send_before_ns": 15,
        "broker_final_send_after_ns": 18,
    }]
    assert request.identity.request_id not in repr(sink.records)


def test_broker_trace_rejects_duplicate_or_unsolicited_boundary():
    request = command()
    trace = BrokerDiagnosticTrace(Sink(), request.identity.generation)
    trace.ingested(request, 0, 10, 12, 0)
    with pytest.raises(OwnerProtocolError):
        trace.ingested(request, 0, 10, 12, 0)
    with pytest.raises(OwnerProtocolError):
        trace.sent(999, 12, 13)
