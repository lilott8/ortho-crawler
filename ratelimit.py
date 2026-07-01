"""Async rate limiting primitives.

Combines a token-bucket limiter (sustained requests/sec with a burst
allowance) and a concurrency cap, so the scraper stays polite to the
MediaWiki API no matter how many workers are running.
"""

from __future__ import annotations

import asyncio
import time


class TokenBucket:
    """A simple asyncio-safe token bucket.

    Refills at ``rate`` tokens per second up to ``capacity`` tokens. Each call
    to :meth:`acquire` consumes one token, sleeping if none are available.
    """

    def __init__(self, rate: float, capacity: float):
        if rate <= 0:
            raise ValueError("rate must be > 0")
        self._rate = rate
        self._capacity = max(capacity, 1.0)
        self._tokens = self._capacity
        self._updated = time.monotonic()
        self._lock = asyncio.Lock()

    def _refill(self) -> None:
        now = time.monotonic()
        elapsed = now - self._updated
        if elapsed > 0:
            self._tokens = min(self._capacity, self._tokens + elapsed * self._rate)
            self._updated = now

    async def acquire(self) -> None:
        while True:
            async with self._lock:
                self._refill()
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return
                # How long until at least one token is available?
                deficit = 1.0 - self._tokens
                wait = deficit / self._rate
            await asyncio.sleep(wait)


class RateLimiter:
    """One or more token buckets + a concurrency semaphore, as an async context
    manager.

    A single bucket is the common case (sustained rate + burst). Pass ``buckets``
    to enforce *several* limits at once — every bucket must yield a token before a
    request proceeds — e.g. WikiArt's "4/s **and** 400/hour":
    ``RateLimiter(buckets=[TokenBucket(4, 4), TokenBucket(400/3600, 400)])``.
    """

    def __init__(self, requests_per_second: float = None, burst: int = None,
                 max_concurrency: int = 4, *, buckets=None):
        if buckets is None:
            if requests_per_second is None or burst is None:
                raise ValueError("provide requests_per_second+burst, or buckets")
            buckets = [TokenBucket(requests_per_second, burst)]
        self._buckets = list(buckets)
        self._sem = asyncio.Semaphore(max(1, max_concurrency))

    async def __aenter__(self):
        await self._sem.acquire()
        try:
            # Satisfy every limit. Acquiring in series is fine: a bucket consumes
            # a token only when it grants, so a later bucket's wait can't waste an
            # earlier token (the earlier token is already "spent" on this request).
            for bucket in self._buckets:
                await bucket.acquire()
        except BaseException:
            self._sem.release()
            raise
        return self

    async def __aexit__(self, exc_type, exc, tb):
        self._sem.release()
        return False
