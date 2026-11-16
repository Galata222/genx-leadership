"""Shared HTTP client that retries transient downstream failures."""

import asyncio
import logging
import os

import httpx

logger = logging.getLogger(__name__)

DOWNSTREAM_URL = os.getenv("DOWNSTREAM_URL", "http://127.0.0.1:9001/events")
RETRYABLE_STATUS = {408, 425, 429, 500, 502, 503, 504}
MAX_ATTEMPTS = 5


class RetryableDownstreamError(Exception):
    """A downstream response or transport error that is safe to try again."""


class PermanentDeliveryError(Exception):
    """A downstream response that will not succeed on retry."""


class DownstreamClient:
    def __init__(self) -> None:
        self._client: httpx.AsyncClient | None = None
        self._url = os.getenv("DOWNSTREAM_URL", DOWNSTREAM_URL)

    async def start(self) -> None:
        self._url = os.getenv("DOWNSTREAM_URL", DOWNSTREAM_URL)
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(2.0, connect=0.5),
            limits=httpx.Limits(max_connections=40, max_keepalive_connections=20),
        )

    async def aclose(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def forward(self, event_id: str, cleaned_text: str, fingerprint: str) -> dict:
        """POST one event, retrying only transient failures.

        Attempts are sequential for a single event so a retry cannot race
        itself. Callers must not run two forwards for the same event at once.
        """
        if self._client is None:
            raise RuntimeError("downstream client is not started")

        delay = 0.05
        last_error: Exception | None = None
        payload = {
            "event_id": event_id,
            "text": cleaned_text,
            "fingerprint": fingerprint,
        }

        for attempt in range(1, MAX_ATTEMPTS + 1):
            try:
                response = await self._client.post(self._url, json=payload)
            except (httpx.TimeoutException, httpx.TransportError) as exc:
                last_error = exc
                logger.debug("downstream transport error for %s: %s", event_id, exc)
            else:
                if response.status_code in RETRYABLE_STATUS:
                    await response.aread()
                    last_error = RetryableDownstreamError(
                        f"downstream returned {response.status_code} for {event_id}"
                    )
                    logger.debug(
                        "downstream %s for %s (attempt %s)",
                        response.status_code,
                        event_id,
                        attempt,
                    )
                elif response.status_code >= 400:
                    await response.aread()
                    raise PermanentDeliveryError(
                        f"downstream returned {response.status_code} for {event_id}"
                    )
                else:
                    return response.json()

            if attempt < MAX_ATTEMPTS:
                await asyncio.sleep(delay)
                delay = min(delay * 2, 0.4)

        assert last_error is not None
        raise last_error
