from __future__ import annotations

import asyncio
import time


class TokenBucketRateLimiter:
    def __init__(
        self,
        calls_per_minute: int,
        *,
        clock=time.monotonic,
        sleep=asyncio.sleep,
    ) -> None:
        if calls_per_minute <= 0:
            raise ValueError("RATE_LIMIT_MUST_BE_POSITIVE")
        self.capacity = float(calls_per_minute)
        self.refill_per_second = self.capacity / 60.0
        self.tokens = self.capacity
        self.clock = clock
        self.sleep = sleep
        self.updated_at = clock()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        while True:
            async with self._lock:
                now = self.clock()
                elapsed = max(0.0, now - self.updated_at)
                self.tokens = min(
                    self.capacity,
                    self.tokens + elapsed * self.refill_per_second,
                )
                self.updated_at = now
                if self.tokens + 1e-9 >= 1.0:
                    self.tokens = max(0.0, self.tokens - 1.0)
                    return
                wait_seconds = (1.0 - self.tokens) / self.refill_per_second
            await self.sleep(wait_seconds)

