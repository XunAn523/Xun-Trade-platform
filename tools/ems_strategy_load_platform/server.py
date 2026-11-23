"""HTTP server for the isolated EMS and strategy-consumer load contracts."""

from __future__ import annotations

import argparse
import os
from contextlib import asynccontextmanager
from typing import Annotated, AsyncIterator

import uvicorn
from fastapi import FastAPI, Header, HTTPException, Response, status
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel, Field

from .service_v1 import (
    EmsAdmissionOverloadedError,
    EmsAdmissionPausedError,
    EmsPlanInputV1,
    EmsStrategyLoadPlatformError,
    EmsStrategyLoadPlatformV1,
    EmsStrategyLoadSettingsV1,
    StrategyConsumerPausedError,
    StrategyEventGapError,
    StrategyEventInputV1,
)


class PlanWireV1(BaseModel):
    request_id: str = Field(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9_.:-]+$")
    wait_for_child: bool = False


class StrategyEventWireV1(BaseModel):
    consumer_id: str = Field(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9_.:-]+$")
    scope_id: str = Field(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9_.:-]+$")
    sequence: int = Field(ge=1, le=2_000_000_000)
    event_id: str = Field(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9_.:-]+$")


class ControlWireV1(BaseModel):
    plan_id: str | None = Field(default=None, min_length=1, max_length=128)
    consumer_id: str | None = Field(default=None, min_length=1, max_length=128)


def create_app(
    settings: EmsStrategyLoadSettingsV1 | None = None,
    *,
    enable_faults: bool = False,
    fault_key: str | None = None,
) -> FastAPI:
    """Create a loopback-only performance app with no broker dependencies."""

    runtime = EmsStrategyLoadPlatformV1(settings)

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        await runtime.start()
        try:
            yield
        finally:
            await runtime.stop()

    app = FastAPI(title="TradeCore EMS/Strategy Load Platform", version="v1", lifespan=lifespan)
    app.state.runtime = runtime
    app.state.enable_faults = enable_faults
    app.state.fault_key = fault_key

    async def _submit(item: EmsPlanInputV1) -> Response:
        try:
            result = await runtime.submit_plan(item)
        except EmsAdmissionPausedError as error:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(error)
            ) from error
        except EmsAdmissionOverloadedError as error:
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS, detail=str(error)
            ) from error
        except EmsStrategyLoadPlatformError as error:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(error)
            ) from error
        response_status = (
            status.HTTP_201_CREATED if item.wait_for_child else status.HTTP_202_ACCEPTED
        )
        return Response(
            content=_encode(result), media_type="application/json", status_code=response_status
        )

    @app.post("/perf/v1/ems/plans")
    async def submit_ems_plan(payload: PlanWireV1) -> Response:
        return await _submit(EmsPlanInputV1(payload.request_id, "ems", payload.wait_for_child))

    @app.post("/perf/v1/strategy/signals")
    async def submit_strategy_signal(payload: PlanWireV1) -> Response:
        try:
            result = await runtime.submit_strategy_signal(
                payload.request_id, wait_for_child=payload.wait_for_child
            )
        except EmsAdmissionPausedError as error:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(error)
            ) from error
        except EmsAdmissionOverloadedError as error:
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS, detail=str(error)
            ) from error
        except EmsStrategyLoadPlatformError as error:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(error)
            ) from error
        response_status = (
            status.HTTP_201_CREATED if payload.wait_for_child else status.HTTP_202_ACCEPTED
        )
        return Response(
            content=_encode(result), media_type="application/json", status_code=response_status
        )

    @app.get("/perf/v1/ems/plans/{plan_id}")
    async def ems_plan(plan_id: str) -> dict[str, object]:
        result = runtime.plan(plan_id)
        if result is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="plan not found")
        return dict(result)

    @app.get("/perf/v1/ems/snapshot")
    async def ems_snapshot() -> dict[str, object]:
        return dict(runtime.plan_snapshot())

    @app.get("/perf/v1/strategy/snapshot")
    async def strategy_snapshot() -> dict[str, object]:
        return dict(runtime.strategy_snapshot())

    @app.post("/perf/v1/strategy/events")
    async def consume_strategy_event(payload: StrategyEventWireV1) -> Response:
        try:
            result = runtime.consume_strategy_event(
                StrategyEventInputV1(
                    payload.consumer_id, payload.scope_id, payload.sequence, payload.event_id
                )
            )
        except StrategyConsumerPausedError as error:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(error)
            ) from error
        except StrategyEventGapError as error:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(error)) from error
        return Response(
            content=_encode(result),
            media_type="application/json",
            status_code=status.HTTP_201_CREATED,
        )

    @app.get("/perf/v1/ems/health")
    @app.get("/perf/v1/strategy/health")
    async def health() -> dict[str, object]:
        return dict(runtime.health())

    @app.get("/perf/v1/metrics/hdr")
    async def hdr_metrics() -> dict[str, object]:
        return runtime.metrics.summary()

    @app.post("/perf/v1/metrics/reset", status_code=status.HTTP_204_NO_CONTENT)
    async def reset_metrics() -> Response:
        runtime.metrics.reset()
        return Response(status_code=status.HTTP_204_NO_CONTENT)

    @app.get("/metrics", response_class=PlainTextResponse)
    async def prometheus_metrics() -> str:
        return runtime.metrics.prometheus()

    @app.post("/perf/v1/admin/{family}/{action}")
    async def control(
        family: str,
        action: str,
        payload: ControlWireV1,
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
        try:
            if family == "ems":
                return dict(runtime.control_ems(action, plan_id=payload.plan_id))
            if family == "strategy":
                return dict(runtime.control_strategy(action, consumer_id=payload.consumer_id))
            if family == "benchmark" and action == "reset":
                return dict(await runtime.reset_benchmark_state())
        except EmsStrategyLoadPlatformError as error:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(error)) from error
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="unknown control action")

    return app


def _encode(value: object) -> bytes:
    """Use the standard library encoder; benchmark requests ask for no body."""

    import json

    return json.dumps(value, separators=(",", ":"), sort_keys=True).encode("utf-8")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the TradeCore EMS/strategy load platform")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=18180)
    parser.add_argument("--scopes", type=int, default=4)
    parser.add_argument("--backlog", type=int, default=8_192)
    parser.add_argument("--plan-queue-capacity", type=int, default=8_192)
    parser.add_argument("--seed-plans", type=int, default=2_048)
    parser.add_argument("--enable-faults", action="store_true")
    parser.add_argument("--fault-key", default=os.environ.get("TRADECORE_EMS_STR_PERF_FAULT_KEY"))
    return parser


def main() -> None:
    args = _parser().parse_args()
    if args.enable_faults and not args.fault_key:
        raise SystemExit("--enable-faults requires --fault-key or TRADECORE_EMS_STR_PERF_FAULT_KEY")
    if not 128 <= args.backlog <= 65_535:
        raise SystemExit("--backlog must be in [128, 65535]")
    settings = EmsStrategyLoadSettingsV1(
        scope_count=args.scopes,
        plan_queue_capacity=args.plan_queue_capacity,
        seed_plans=args.seed_plans,
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
