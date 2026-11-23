"""HTTP contracts and executable server for the isolated OMS load platform."""

from __future__ import annotations

import argparse
import os
import re
from contextlib import asynccontextmanager
from typing import Annotated, AsyncIterator

import msgspec
import uvicorn
from fastapi import FastAPI, Header, HTTPException, Query, Request, Response, status
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel, Field
from xuanling_tradecore.oms.sidecar_v1 import OmsSidecarFactKind, OmsSidecarOrderState

from .service_v1 import (
    OmsFactInputV1,
    OmsIngressOverloadedError,
    OmsLoadPlatformError,
    OmsLoadPlatformSettingsV1,
    OmsLoadPlatformV1,
    OmsProjectorPausedError,
    OmsSnapshotStaleError,
)


class _FactWireV1(msgspec.Struct, forbid_unknown_fields=True):
    """Allocation-light wire shape for the W1/W3 high-frequency endpoint."""

    scope_id: str
    execution_id: str
    canonical_client_order_ref: str
    kind: str = "PRE_NATIVE_COMMITTED"
    order_state: str = "PRE_NATIVE_COMMITTED"
    external_order_id: str | None = None
    state_version: int = 1
    cumulative_quantity: int | None = None
    leaves_quantity: int | None = None


class FaultRequestV1(BaseModel):
    scope_id: str = Field(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9_.:-]+$")


_TOKEN = re.compile(r"^[A-Za-z0-9_.:-]+$")


def _decode_fact(raw: bytes) -> OmsFactInputV1:
    """Decode and bound a synthetic fact without Pydantic response overhead."""

    try:
        wire = msgspec.json.decode(raw, type=_FactWireV1)
        for name, value, maximum in (
            ("scope_id", wire.scope_id, 128),
            ("execution_id", wire.execution_id, 256),
            ("canonical_client_order_ref", wire.canonical_client_order_ref, 256),
        ):
            if not value or len(value) > maximum or _TOKEN.fullmatch(value) is None:
                raise ValueError(f"{name} is invalid")
        if wire.external_order_id is not None and (
            len(wire.external_order_id) > 256 or _TOKEN.fullmatch(wire.external_order_id) is None
        ):
            raise ValueError("external_order_id is invalid")
        quantities = (wire.state_version, wire.cumulative_quantity, wire.leaves_quantity)
        if any(
            value is not None and (type(value) is not int or not 0 <= value <= 2_000_000_000)
            for value in quantities
        ):
            raise ValueError("fact version or quantity is invalid")
        return OmsFactInputV1(
            scope_id=wire.scope_id,
            execution_id=wire.execution_id,
            canonical_client_order_ref=wire.canonical_client_order_ref,
            kind=OmsSidecarFactKind(wire.kind),
            order_state=OmsSidecarOrderState(wire.order_state),
            external_order_id=wire.external_order_id,
            state_version=wire.state_version,
            cumulative_quantity=wire.cumulative_quantity,
            leaves_quantity=wire.leaves_quantity,
        )
    except (msgspec.ValidationError, ValueError) as error:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(error)
        ) from error


def create_app(
    settings: OmsLoadPlatformSettingsV1 | None = None,
    *,
    enable_faults: bool = False,
    fault_key: str | None = None,
) -> FastAPI:
    """Build an app whose lifecycle owns only the standalone sidecar harness."""

    runtime = OmsLoadPlatformV1(settings)

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        await runtime.start()
        try:
            yield
        finally:
            await runtime.stop()

    app = FastAPI(title="TradeCore OMS Load Platform", version="v1", lifespan=lifespan)
    app.state.runtime = runtime
    app.state.enable_faults = enable_faults
    app.state.fault_key = fault_key

    @app.post("/perf/v1/oms/facts", status_code=status.HTTP_201_CREATED)
    async def ingest_fact(
        request: Request,
        acknowledgement: Annotated[str | None, Header(alias="X-TradeCore-Perf-Ack")] = None,
    ) -> Response:
        try:
            receipt = await runtime.ingest(_decode_fact(await request.body()))
        except OmsProjectorPausedError as error:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(error)
            ) from error
        except OmsIngressOverloadedError as error:
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS, detail=str(error)
            ) from error
        except OmsLoadPlatformError as error:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(error)
            ) from error
        if acknowledgement == "minimal":
            return Response(status_code=status.HTTP_201_CREATED)
        payload = {
            "status": "PROJECTED",
            "scope_id": receipt.scope_id,
            "position": receipt.position,
            "snapshot_generation": receipt.snapshot_generation,
            "admitted_monotonic_ns": receipt.admitted_monotonic_ns,
            "projected_monotonic_ns": receipt.projected_monotonic_ns,
        }
        return Response(
            content=msgspec.json.encode(payload),
            media_type="application/json",
            status_code=status.HTTP_201_CREATED,
        )

    @app.get("/perf/v1/oms/orders/{execution_id}")
    async def query_order(execution_id: str) -> dict[str, object]:
        result = runtime.order(execution_id)
        if result is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="order not found")
        return dict(result)

    @app.get("/perf/v1/oms/snapshot")
    async def snapshot(require_fresh: bool = Query(default=False)) -> dict[str, object]:
        try:
            return runtime.snapshot_summary(require_fresh=require_fresh)
        except OmsSnapshotStaleError as error:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(error)
            ) from error

    @app.get("/perf/v1/oms/health")
    async def health() -> dict[str, object]:
        return runtime.health()

    @app.get("/perf/v1/metrics/hdr")
    async def hdr_metrics() -> dict[str, object]:
        return runtime.metrics.summary()

    @app.post("/perf/v1/metrics/reset", status_code=status.HTTP_204_NO_CONTENT)
    async def reset_metrics() -> Response:
        runtime.reset_metrics()
        return Response(status_code=status.HTTP_204_NO_CONTENT)

    @app.get("/metrics", response_class=PlainTextResponse)
    async def prometheus_metrics() -> str:
        return runtime.metrics.prometheus()

    @app.post("/perf/v1/admin/fault/{action}")
    async def fault(
        action: str,
        request: FaultRequestV1,
        supplied_key: Annotated[str | None, Header(alias="X-TradeCore-Perf-Fault-Key")] = None,
    ) -> dict[str, object]:
        if (
            not app.state.enable_faults
            or not app.state.fault_key
            or supplied_key != app.state.fault_key
        ):
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="fault endpoint disabled"
            )
        if action == "pause-projector":
            runtime.pause_projector(request.scope_id)
            return {"status": "PAUSED", "scope_id": request.scope_id}
        if action == "resume-projector":
            runtime.resume_projector(request.scope_id)
            return {"status": "CURRENT", "scope_id": request.scope_id}
        if action == "replay-verify":
            try:
                return await runtime.verify_replay()
            except OmsLoadPlatformError as error:
                raise HTTPException(
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(error)
                ) from error
        if action == "reset-benchmark":
            try:
                return await runtime.reset_benchmark_state()
            except OmsLoadPlatformError as error:
                raise HTTPException(
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(error)
                ) from error
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="unknown fault action")

    return app


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the isolated TradeCore OMS load platform")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=18080)
    parser.add_argument("--scopes", type=int, default=4)
    parser.add_argument(
        "--backlog",
        type=int,
        default=8_192,
        help=(
            "TCP accept backlog for the isolated sidecar; the W3/W7 k6 profile can "
            "establish thousands of concurrent loopback connections"
        ),
    )
    parser.add_argument("--queue-capacity", type=int, default=8_192)
    parser.add_argument("--snapshot-stale-after-ms", type=int, default=250)
    parser.add_argument("--seed-orders", type=int, default=1_024)
    parser.add_argument("--enable-faults", action="store_true")
    parser.add_argument("--fault-key", default=os.environ.get("TRADECORE_OMS_PERF_FAULT_KEY"))
    return parser


def main() -> None:
    args = _parser().parse_args()
    if args.enable_faults and not args.fault_key:
        raise SystemExit("--enable-faults requires --fault-key or TRADECORE_OMS_PERF_FAULT_KEY")
    if not 128 <= args.backlog <= 65_535:
        raise SystemExit("--backlog must be in [128, 65535]")
    settings = OmsLoadPlatformSettingsV1(
        scope_count=args.scopes,
        queue_capacity=args.queue_capacity,
        snapshot_stale_after_ms=args.snapshot_stale_after_ms,
        seed_orders=args.seed_orders,
    )
    uvicorn.run(
        create_app(settings, enable_faults=args.enable_faults, fault_key=args.fault_key),
        host=args.host,
        port=args.port,
        backlog=args.backlog,
        log_level="warning",
        access_log=False,
    )


if __name__ == "__main__":
    main()
