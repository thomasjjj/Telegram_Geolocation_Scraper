"""Utilities for caching Telegram entities between API calls."""
from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, Iterable, List, Optional

from telethon import TelegramClient
from telethon.errors import FloodWaitError, RPCError
from telethon.tl.types import PeerChannel, PeerChat, PeerUser

from src.database import CoordinatesDatabase
from src.rate_limiter import AdaptiveRateLimiter
from src.telethon_session import ensure_connected


LOGGER = logging.getLogger(__name__)


class EntityCache:
    """Cache helper that combines in-memory and persistent storage."""

    def __init__(
        self,
        client: TelegramClient,
        database: Optional[CoordinatesDatabase] = None,
        *,
        rate_limiter: Optional[AdaptiveRateLimiter] = None,
        max_age_hours: Optional[int] = None,
    ) -> None:
        self.client = client
        self.database = database
        self._cache: Dict[str, Any] = {}
        self.rate_limiter = rate_limiter
        self.max_age_hours = max_age_hours

    async def get_entity(self, identifier: Any) -> Any:
        """Return the entity for *identifier*, caching the result when possible."""

        lookup = self._prepare_lookup(identifier)
        keys = lookup["cache_keys"] or list(self._normalise_identifiers(identifier))
        for key in keys:
            if key in self._cache:
                return self._cache[key]

        entity = None
        if self.database:
            ttl = self._effective_ttl
            for key in keys:
                record = self.database.get_cached_entity(key, max_age_hours=ttl)
                if record:
                    entity = record.entity
                    LOGGER.debug("Loaded entity %s from database cache", key)
                    self._store_entity(entity, keys, persist=False)
                    return entity

        entity = await self._fetch_entity(lookup)
        self._store_entity(entity, keys)
        return entity

    async def get_entities_batch(
        self,
        identifiers: Iterable[Any],
        *,
        rate_limiter: Optional[AdaptiveRateLimiter] = None,
    ) -> Dict[Any, Any]:
        """Resolve multiple entities using cache and adaptive throttling."""

        results: Dict[Any, Any] = {}
        limiter = rate_limiter or self.rate_limiter

        for identifier in identifiers:
            try:
                entity = await self.get_entity(identifier)
                if entity is not None:
                    results[identifier] = entity
                    if limiter:
                        limiter.record_success()
            except FloodWaitError as exc:
                wait_time = limiter.record_flood_wait(exc.seconds) if limiter else float(exc.seconds)
                await asyncio.sleep(wait_time)
            except RPCError as exc:
                LOGGER.debug("RPC error while fetching entity %s: %s", identifier, exc)
                if limiter:
                    delay = limiter.record_error()
                    await asyncio.sleep(delay)
            except ValueError as exc:
                LOGGER.debug("Failed to resolve entity %s: %s", identifier, exc)
        return results

    @property
    def _effective_ttl(self) -> Optional[int]:
        if self.max_age_hours is not None:
            return self.max_age_hours
        if self.database is not None:
            return self.database.entity_cache_max_age_hours
        return None

    async def _throttle(self, limiter: Optional[AdaptiveRateLimiter] = None) -> None:
        limiter = limiter or self.rate_limiter
        if limiter:
            await limiter.throttle()

    def _store_entity(self, entity: Any, keys: Iterable[str], *, persist: bool = True) -> None:
        for key in keys:
            self._cache[key] = entity
            if self.database and persist:
                self.database.cache_entity(key, entity)

        extra_keys = set()
        entity_id = getattr(entity, "id", None)
        if entity_id is not None:
            extra_keys.add(str(entity_id))
        username = getattr(entity, "username", None)
        if username:
            extra_keys.add(username)

        for key in extra_keys:
            self._cache[key] = entity
            if self.database and persist:
                self.database.cache_entity(key, entity)

    @staticmethod
    def _normalise_identifiers(identifier: Any) -> Iterable[str]:
        value = str(identifier)
        return [value]

    @staticmethod
    def _normalise_peer_type(peer_type: Optional[str]) -> Optional[str]:
        if not peer_type:
            return None

        value = str(peer_type).lower()
        if value in {"peerchannel", "channel"}:
            return "channel"
        if value in {"peerchat", "chat", "supergroup", "megagroup", "group"}:
            return "chat"
        if value in {"peeruser", "user"}:
            return "user"
        return value

    @classmethod
    def _prepare_lookup(cls, identifier: Any) -> Dict[str, Any]:
        peer_type: Optional[str] = None
        entity_id: Optional[int] = None
        username: Optional[str] = None
        query: Any = identifier

        if isinstance(identifier, dict):
            peer_type = identifier.get("peer_type") or identifier.get("entity_type")
            entity_id = identifier.get("id") or identifier.get("channel_id")
            username = identifier.get("username")
            if entity_id is not None:
                query = entity_id
            elif username:
                query = username
        elif isinstance(identifier, tuple) and len(identifier) >= 2:
            entity_id = identifier[0]
            peer_type = identifier[1]
            query = entity_id
        else:
            if isinstance(identifier, int):
                entity_id = identifier
            elif isinstance(identifier, str) and identifier.isdigit():
                try:
                    entity_id = int(identifier)
                except ValueError:
                    entity_id = None

        normalised_peer = cls._normalise_peer_type(peer_type)

        cache_keys: List[str] = []
        if entity_id is not None:
            if normalised_peer:
                cache_keys.append(f"{normalised_peer}:{entity_id}")
            cache_keys.append(str(entity_id))
        if username:
            cache_keys.append(str(username))
        cache_keys.append(str(identifier))

        # Preserve order while removing duplicates
        seen = set()
        ordered_keys: List[str] = []
        for key in cache_keys:
            if key is None:
                continue
            if key in seen:
                continue
            seen.add(key)
            ordered_keys.append(key)

        return {
            "cache_keys": ordered_keys,
            "peer_type": normalised_peer,
            "entity_id": entity_id,
            "username": username,
            "query": query,
        }

    @staticmethod
    def _build_peer_reference(peer_type: Optional[str], entity_id: Optional[int]):
        if peer_type is None or entity_id is None:
            return None

        try:
            numeric_id = int(entity_id)
        except (TypeError, ValueError):
            return None

        if peer_type == "channel":
            return PeerChannel(channel_id=numeric_id)
        if peer_type == "chat":
            return PeerChat(chat_id=numeric_id)
        if peer_type == "user":
            return PeerUser(user_id=numeric_id)
        return None

    async def _find_in_dialogs(self, entity_id: Optional[int]) -> Any:
        if entity_id is None:
            return None

        try:
            target_id = int(entity_id)
        except (TypeError, ValueError):
            return None

        async for dialog in self.client.iter_dialogs():
            dialog_entity = getattr(dialog, "entity", None)
            if getattr(dialog_entity, "id", None) == target_id:
                return dialog_entity
        return None

    async def _fetch_entity(self, lookup: Dict[str, Any]) -> Any:
        await ensure_connected(self.client)
        limiter = self.rate_limiter

        peer_reference = self._build_peer_reference(
            lookup.get("peer_type"), lookup.get("entity_id")
        )

        if peer_reference is not None:
            try:
                return await self._request_with_retry(
                    lambda: self.client.get_entity(peer_reference), limiter
                )
            except ValueError:
                LOGGER.debug(
                    "Entity resolution failed for %s using peer %s",
                    lookup.get("entity_id"),
                    lookup.get("peer_type"),
                )

        username = lookup.get("username")
        if username:
            try:
                return await self._request_with_retry(
                    lambda: self.client.get_entity(username), limiter
                )
            except ValueError:
                LOGGER.debug("Entity resolution via username %s failed", username)

        query = lookup.get("query")
        try:
            return await self._request_with_retry(
                lambda: self.client.get_entity(query), limiter
            )
        except (RPCError, ValueError, TypeError):
            LOGGER.debug("Entity resolution failed for query %s", query)

        entity_id = lookup.get("entity_id")
        if entity_id is not None:
            try:
                return await self._request_with_retry(
                    lambda: self.client.get_entity(int(entity_id)), limiter
                )
            except (RPCError, ValueError):
                LOGGER.debug("Entity resolution failed for id %s", entity_id)

        entity = await self._find_in_dialogs(entity_id)
        if entity:
            return entity

        raise ValueError(f"Unable to resolve entity for identifier {query}")

    async def _request_with_retry(
        self,
        func,
        limiter: Optional[AdaptiveRateLimiter],
        *,
        max_attempts: int = 5,
    ):
        attempts = 0
        while True:
            attempts += 1
            await self._throttle(limiter)
            try:
                result = await func()
                if limiter:
                    limiter.record_success()
                return result
            except FloodWaitError as exc:
                wait_time = limiter.record_flood_wait(exc.seconds) if limiter else float(exc.seconds)
                await asyncio.sleep(wait_time)
            except RPCError as exc:
                LOGGER.debug("RPC error during entity fetch: %s", exc)
                if attempts >= max_attempts:
                    raise
                delay = limiter.record_error() if limiter else min(5 * attempts, 30)
                await asyncio.sleep(delay)

