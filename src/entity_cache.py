"""Utilities for caching Telegram entities between API calls."""
from __future__ import annotations

import logging
from typing import Any, Dict, Iterable, List, Optional

from telethon import TelegramClient
from telethon.errors import RPCError
from telethon.tl.types import PeerChannel, PeerChat, PeerUser

from src.database import CoordinatesDatabase


LOGGER = logging.getLogger(__name__)


class EntityCache:
    """Cache helper that combines in-memory and persistent storage."""

    def __init__(
        self,
        client: TelegramClient,
        database: Optional[CoordinatesDatabase] = None,
    ) -> None:
        self.client = client
        self.database = database
        self._cache: Dict[str, Any] = {}

    async def get_entity(self, identifier: Any) -> Any:
        """Return the entity for *identifier*, caching the result when possible."""

        lookup = self._prepare_lookup(identifier)
        keys = lookup["cache_keys"] or list(self._normalise_identifiers(identifier))
        for key in keys:
            if key in self._cache:
                return self._cache[key]

        entity = None
        if self.database:
            for key in keys:
                entity = self.database.get_cached_entity(key)
                if entity:
                    LOGGER.debug("Loaded entity %s from database cache", key)
                    self._store_entity(entity, keys)
                    return entity

        entity = await self._fetch_entity(lookup)
        self._store_entity(entity, keys)
        return entity

    def _store_entity(self, entity: Any, keys: Iterable[str]) -> None:
        for key in keys:
            self._cache[key] = entity
            if self.database:
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
            if self.database:
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
        peer_reference = self._build_peer_reference(
            lookup.get("peer_type"), lookup.get("entity_id")
        )

        if peer_reference is not None:
            try:
                return await self.client.get_entity(peer_reference)
            except (RPCError, ValueError):
                LOGGER.debug(
                    "Entity resolution failed for %s using peer %s",
                    lookup.get("entity_id"),
                    lookup.get("peer_type"),
                )

        username = lookup.get("username")
        if username:
            try:
                return await self.client.get_entity(username)
            except (RPCError, ValueError):
                LOGGER.debug("Entity resolution via username %s failed", username)

        query = lookup.get("query")
        try:
            return await self.client.get_entity(query)
        except (RPCError, ValueError, TypeError):
            LOGGER.debug("Entity resolution failed for query %s", query)

        entity_id = lookup.get("entity_id")
        if entity_id is not None:
            try:
                return await self.client.get_entity(int(entity_id))
            except (RPCError, ValueError):
                LOGGER.debug("Entity resolution failed for id %s", entity_id)

        entity = await self._find_in_dialogs(entity_id)
        if entity:
            return entity

        raise ValueError(f"Unable to resolve entity for identifier {query}")
