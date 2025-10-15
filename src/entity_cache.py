"""Utilities for caching Telegram entities between API calls."""

from __future__ import annotations

import logging
from typing import Any, Dict, Iterable, Optional

from telethon import TelegramClient

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

        keys = self._normalise_identifiers(identifier)
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

        entity = await self.client.get_entity(identifier)
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
