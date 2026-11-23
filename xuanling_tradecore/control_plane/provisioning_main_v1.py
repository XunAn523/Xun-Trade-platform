"""Process entry point for a deployment-owned Provisioning Control listener.

The repository intentionally does not contain a production recipient private
key, certificate, principal allow-list, SecretProvider, or broker endpoint.
Those components are assembled by one protected deployment factory supplied at
runtime.  This entry point validates that the resulting listener is the
strict V1 HTTP/2/mTLS service and owns only its lifecycle.
"""

from __future__ import annotations

import argparse
import asyncio
import importlib
import os
import signal
from collections.abc import Callable
from types import ModuleType
from typing import cast

from .provisioning_contracts_v1 import ProvisioningErrorV1
from .provisioning_server_v1 import ProvisioningHttp2ServerV1


def _factory_from_name(name: str) -> Callable[[], object]:
    module_name, separator, attribute_name = name.partition(":")
    if not separator or not module_name or not attribute_name:
        raise ProvisioningErrorV1(
            "CONTROL_RUNTIME", "control listener factory must be module:callable"
        )
    module: ModuleType = importlib.import_module(module_name)
    candidate = getattr(module, attribute_name, None)
    if not callable(candidate):
        raise ProvisioningErrorV1("CONTROL_RUNTIME", "control listener factory is unavailable")
    return cast(Callable[[], object], candidate)


async def _serve(server: ProvisioningHttp2ServerV1) -> None:
    stopped = asyncio.Event()
    loop = asyncio.get_running_loop()
    for item in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(item, stopped.set)
    await server.start()
    try:
        await stopped.wait()
    finally:
        await server.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="TradeCore Provisioning Control HTTP/2 service")
    parser.add_argument(
        "--factory",
        default=os.environ.get("TRADECORE_PROVISIONING_FACTORY"),
        help="protected deployment factory as module:callable",
    )
    arguments = parser.parse_args(argv)
    if not isinstance(arguments.factory, str) or not arguments.factory:
        parser.error("--factory or TRADECORE_PROVISIONING_FACTORY is required")
    factory = _factory_from_name(arguments.factory)
    created = factory()
    if not isinstance(created, ProvisioningHttp2ServerV1):
        raise ProvisioningErrorV1("CONTROL_RUNTIME", "control factory returned an invalid listener")
    asyncio.run(_serve(created))
    return 0


if __name__ == "__main__":  # pragma: no cover - process entry point
    raise SystemExit(main())
