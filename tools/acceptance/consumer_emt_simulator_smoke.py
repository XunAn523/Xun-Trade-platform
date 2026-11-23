"""One real simulator order through the public Consumer UDS, with callback proof.

No native SDK import and no cancellation. Keep the same strategy-order-id
when retrying after an uncertain transport outcome; never create a new key
to compensate for a missing response.
"""

import argparse
import json
import time
from pathlib import Path

from xuanling_tradecore.consumer import ConsumerUnixClientConfigV2, ConsumerUnixClientV2


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--socket-path", type=Path, required=True)
    parser.add_argument("--trading-day", required=True)
    parser.add_argument("--strategy-order-id", required=True)
    parser.add_argument("--price-ticks", type=int, default=1039)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        parser.error("refusing to replace acceptance evidence")
    client = ConsumerUnixClientV2(ConsumerUnixClientConfigV2(args.socket_path, 5))
    index = 0

    def call(operation, payload):
        nonlocal index
        index += 1
        return client.call({"message_type": "request", "schema_version": 2,
                            "operation": operation, "request_id": f"sim-smoke-{index}", "payload": payload})

    report = {"simulator_only": True, "native_sdk_imported": False, "cancellation_attempted": False, "passed": False}
    exit_code = 2
    try:
        accounts = call("ListAccounts", {})["payload"]["accounts"]
        if len(accounts) != 1 or accounts[0]["venue_id"] != "EMT" or accounts[0]["lifecycle"] != "SIMULATOR_ACTIVE":
            raise RuntimeError("SIMULATOR_NOT_ACTIVE")
        payload = {
            "consumer_account_id": "emt-sim-ordinary", "strategy_id": "consumer-sim-strategy",
            "strategy_order_id": args.strategy_order_id, "trading_day": args.trading_day,
            "symbol": "000001", "market": "SZ", "side": "BUY", "price_type": "LIMIT",
            "business_type": "CASH", "quantity": 100,
            "limit_price_ticks": args.price_ticks, "protection_price_ticks": args.price_ticks,
        }
        first = call("SubmitSignal", payload)
        if first.get("status") != "ACCEPTED":
            raise RuntimeError("CONSUMER_ADMISSION_FAILED")
        receipt = first["payload"]
        duplicate = call("SubmitSignal", payload)
        if duplicate.get("status") != "ACCEPTED" or duplicate["payload"]["oms_order_id"] != receipt["oms_order_id"] or duplicate["payload"]["execution_id"] != receipt["execution_id"]:
            raise RuntimeError("BUSINESS_IDEMPOTENCY_FAILED")
        report["duplicate_returned_same_execution"] = True
        deadline = time.monotonic() + 20
        states = []
        while time.monotonic() < deadline:
            current = call("GetOrder", {"oms_order_id": receipt["oms_order_id"]})
            state = current["payload"]["order"]["payload"]["order_state"]
            if not states or states[-1] != state:
                states.append(state)
            if state in {"WORKING", "PARTIALLY_FILLED", "FILLED", "REJECTED", "CANCELED"}:
                break
            time.sleep(0.05)
        else:
            raise RuntimeError("EMT_CALLBACK_TIMEOUT")
        report["observed_states"] = states
        if state not in {"WORKING", "PARTIALLY_FILLED", "FILLED"}:
            raise RuntimeError("EMT_ORDER_NOT_ACCEPTED")
        plan = call("CreateReplayPartitions", {"consumer_account_id": "emt-sim-ordinary", "partition_count": 1})["payload"]
        events = []
        for partition in plan["partitions"]:
            token = partition["cursor"]
            while True:
                page = call("PollEvents", {"cursor": token, "limit": 100})["payload"]
                events.extend(page["events"])
                if not page["has_more"]:
                    break
                token = page["next_cursor"]
        matched = [event for event in events if event["payload"].get("execution_id") == receipt["execution_id"] and event["payload"].get("order_state") in {"WORKING", "PARTIALLY_FILLED", "FILLED"}]
        if not matched:
            raise RuntimeError("CONSUMER_CALLBACK_REPLAY_MISSING")
        report.update(passed=True, consumer_callback_events=len(matched), final_observed_state=state)
        exit_code = 0
    except Exception as exc:
        report["failure_type"] = type(exc).__name__
        # Only static error codes generated in this program are emitted.
        if isinstance(exc, RuntimeError) and str(exc).isupper():
            report["failure_code"] = str(exc)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("x", encoding="utf-8") as handle:
        json.dump(report, handle, indent=2)
    print(json.dumps(report))
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
