"""Adaptive rate limiting utilities for Telegram API interactions."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Optional


LOGGER = logging.getLogger(__name__)


@dataclass
class AdaptiveRateLimiter:
    """Simple adaptive rate limiter with exponential backoff support."""

    base_delay: float = 6.0
    max_delay: float = 300.0
    min_delay: float = 0.5
    backoff_factor: float = 1.5
    recovery_factor: float = 0.85
    _current_delay: float = field(default=0.0, init=False)
    _consecutive_success: int = field(default=0, init=False)
    _last_error_delay: Optional[float] = field(default=None, init=False)

    async def throttle(self) -> None:
        """Sleep for the currently configured delay before making a request."""

        delay = max(self.min_delay, self._current_delay or self.base_delay)
        if delay <= 0:
            return

        LOGGER.debug("Throttling for %.2f seconds", delay)
        await asyncio.sleep(delay)

    def record_success(self) -> None:
        """Record a successful API call and gradually relax throttling."""

        self._consecutive_success += 1
        if self._consecutive_success < 5:
            return

        self._consecutive_success = 5
        if self._current_delay <= self.base_delay:
            self._current_delay = max(self.min_delay, self.base_delay)
            return

        self._current_delay = max(
            self.min_delay,
            self._current_delay * self.recovery_factor,
        )

    def record_flood_wait(self, flood_seconds: int) -> float:
        """Handle a Telegram flood wait response and return the suggested delay."""

        wait_seconds = float(flood_seconds)
        self._consecutive_success = 0
        self._current_delay = min(wait_seconds * self.backoff_factor, self.max_delay)
        self._last_error_delay = self._current_delay
        LOGGER.warning(
            "Telegram flood wait encountered (%.0fs); throttling for %.2fs", wait_seconds, self._current_delay
        )
        return self._current_delay

    def record_error(self) -> float:
        """Apply backoff after non-flood RPC errors and return the delay."""

        self._consecutive_success = 0
        if self._last_error_delay is None:
            self._current_delay = min(
                max(self.base_delay, self.min_delay) * self.backoff_factor, self.max_delay
            )
        else:
            self._current_delay = min(self._last_error_delay * self.backoff_factor, self.max_delay)

        self._last_error_delay = self._current_delay
        return self._current_delay

