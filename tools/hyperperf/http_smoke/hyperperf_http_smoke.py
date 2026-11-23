"""HyperPerf HTTP fixture connectivity only; never a TradeCore/EMT benchmark.

Upload this file as the Locust entrypoint. HyperPerf supplies the origin,
allowlist, proxy, concurrency, request rate, phases and output collection.
The request shape matches HyperPerf 3e442a9's isolated HTTP target contract.
Actual deployment, preflight and authorized execution are verified separately.
"""

from locust import HttpUser, task

OPERATION_NAME = "GET /work"
REQUEST_TIMEOUT = (2.0, 5.0)  # Connect and read budgets, not a whole-call deadline.


class HyperPerfHttpSmokeUser(HttpUser):
    """Exactly one measured request per task, without retries or setup traffic."""

    @task
    def work(self) -> None:
        with self.client.get(
            "/work",
            name=OPERATION_NAME,
            headers={"Accept": "application/json"},
            timeout=REQUEST_TIMEOUT,
            allow_redirects=False,
            catch_response=True,
        ) as response:
            if response.error is not None:
                # Preserve the transport exception for the platform's own
                # classification; wrapped timeouts are not always TIMEOUT.
                response.failure(response.error)
                return

            if response.status_code != 200:
                response.failure(f"HTTP_STATUS expected=200 actual={response.status_code}")
                return

            media_type = response.headers.get("Content-Type", "").split(";", 1)[0]
            if media_type.strip().lower() != "application/json":
                response.failure("PROTOCOL_ERROR expected application/json")
                return

            try:
                payload = response.json()
            except ValueError:
                # Do not include the raw response body in error messages.
                response.failure("PROTOCOL_ERROR invalid JSON")
                return

            if (
                type(payload) is not dict
                or set(payload) != {"accepted"}
                or payload["accepted"] is not True
            ):
                response.failure("BUSINESS_CONTRACT expected exactly accepted=true")
                return

            response.success()
