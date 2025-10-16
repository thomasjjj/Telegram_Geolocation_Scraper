"""Utilities for discovering and managing recommended Telegram channels."""

from __future__ import annotations

import json
import logging
import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from telethon import TelegramClient
from telethon.errors import RPCError
from telethon.tl.functions.channels import GetChannelRecommendationsRequest
from telethon.tl.types import Channel, Chat, PeerChannel, PeerChat, PeerUser

from src.database import CoordinatesDatabase

LOGGER = logging.getLogger(__name__)


@dataclass
class RecommendationSettings:
    """Configuration options for the recommendation system."""

    enabled: bool = True
    min_score: float = 30.0
    min_hit_rate: float = 5.0
    show_at_startup: bool = True
    auto_enrich: bool = False
    max_display: int = 5
    telegram_recs_enabled: bool = True
    telegram_min_source_density: float = 5.0
    telegram_auto_harvest: bool = False
    telegram_harvest_after_scrape: bool = False
    telegram_max_source_channels: Optional[int] = None
    quality_weight: float = 0.6
    trust_weight: float = 0.4
    penalize_low_sample: bool = True
    hide_zero_coordinates: bool = False

    @classmethod
    def from_environment(cls) -> "RecommendationSettings":
        """Load settings from environment variables."""

        def _as_bool(value: Optional[str], default: bool) -> bool:
            if value is None:
                return default
            return value.lower() in {"1", "true", "yes", "on"}

        enabled = _as_bool(os.environ.get("RECOMMENDATIONS_ENABLED"), True)
        min_score = float(os.environ.get("RECOMMENDATIONS_MIN_SCORE", 30.0))
        min_hit_rate = float(os.environ.get("RECOMMENDATIONS_MIN_HIT_RATE", 5.0))
        show_at_startup = _as_bool(os.environ.get("RECOMMENDATIONS_SHOW_AT_STARTUP"), True)
        auto_enrich = _as_bool(os.environ.get("RECOMMENDATIONS_AUTO_ENRICH"), False)
        max_display = int(os.environ.get("RECOMMENDATIONS_MAX_DISPLAY", 5))

        telegram_recs_enabled = _as_bool(os.environ.get("TELEGRAM_RECS_ENABLED"), True)
        telegram_min_source_density = float(
            os.environ.get("TELEGRAM_RECS_MIN_SOURCE_DENSITY", 5.0)
        )
        telegram_auto_harvest = _as_bool(
            os.environ.get("TELEGRAM_RECS_AUTO_HARVEST"), False
        )
        telegram_harvest_after_scrape = _as_bool(
            os.environ.get("TELEGRAM_RECS_HARVEST_AFTER_SCRAPE"), False
        )
        max_sources_value = os.environ.get("TELEGRAM_RECS_MAX_SOURCE_CHANNELS")
        try:
            telegram_max_source_channels = (
                int(max_sources_value) if max_sources_value else None
            )
        except (TypeError, ValueError):
            telegram_max_source_channels = None

        return cls(
            enabled=enabled,
            min_score=min_score,
            show_at_startup=show_at_startup,
            auto_enrich=auto_enrich,
            max_display=max_display,
            telegram_recs_enabled=telegram_recs_enabled,
            telegram_min_source_density=telegram_min_source_density,
            telegram_auto_harvest=telegram_auto_harvest,
            telegram_harvest_after_scrape=telegram_harvest_after_scrape,
            telegram_max_source_channels=telegram_max_source_channels,
            min_hit_rate=min_hit_rate,
            quality_weight=float(os.environ.get("RECOMMENDATIONS_QUALITY_WEIGHT", 0.6)),
            trust_weight=float(os.environ.get("RECOMMENDATIONS_TRUST_WEIGHT", 0.4)),
            penalize_low_sample=_as_bool(
                os.environ.get("RECOMMENDATIONS_PENALTY_LOW_SAMPLE"), True
            ),
            hide_zero_coordinates=_as_bool(
                os.environ.get("RECOMMENDATIONS_HIDE_ZERO_COORDS"), False
            ),
        )


class RecommendationManager:
    """Manages channel recommendations derived from forwarded messages."""

    def __init__(
        self, database: CoordinatesDatabase, settings: Optional[RecommendationSettings] = None
    ) -> None:
        self.db = database
        self.settings = settings or RecommendationSettings.from_environment()

    # ------------------------------------------------------------------
    # Forward processing
    def process_forwarded_message(
        self,
        message,
        current_channel_id: int,
        has_coordinates: bool,
        message_row_id: Optional[int] = None,
    ) -> Optional[int]:
        """Process a forwarded message and update recommendation records.

        Returns the channel ID of a newly discovered recommendation, or ``None``
        when no new recommendation was created.
        """

        if not self.settings.enabled or not message or not getattr(message, "forward", None):
            return None

        forward_info = self._extract_forward_info(message)
        if not forward_info:
            return None

        source_channel_id = forward_info["channel_id"]
        if not self._is_valid_channel_id(source_channel_id, forward_info):
            LOGGER.debug(
                "Skipping forward from invalid entity type: %s", source_channel_id
            )
            return None
        if self._is_already_followed(source_channel_id):
            return None

        existing = self.db.get_recommended_channel(source_channel_id)
        now_iso = datetime.now(timezone.utc).isoformat()
        discovered_from = self._merge_sources(existing, current_channel_id)

        if existing:
            forward_count = int(existing.get("forward_count") or 0) + 1
            coordinate_count = int(existing.get("coordinate_forward_count") or 0)
            if has_coordinates:
                coordinate_count += 1

            update_data: Dict[str, Any] = {
                "forward_count": forward_count,
                "coordinate_forward_count": coordinate_count,
                "last_seen": now_iso,
                "discovered_from_channels": json.dumps(discovered_from),
            }

            if forward_info.get("title") and not existing.get("title"):
                update_data["title"] = forward_info["title"]
            if forward_info.get("username") and not existing.get("username"):
                update_data["username"] = forward_info["username"]
            if forward_info.get("entity_type") and not existing.get("entity_type"):
                update_data["entity_type"] = forward_info["entity_type"]
            if forward_info.get("peer_type") and not existing.get("peer_type"):
                update_data["peer_type"] = forward_info["peer_type"]

            self.db.update_recommended_channel(source_channel_id, update_data)
            created = False
        else:
            payload = {
                "username": forward_info.get("username"),
                "title": forward_info.get("title"),
                "channel_type": forward_info.get("channel_type"),
                "peer_type": forward_info.get("peer_type"),
                "entity_type": forward_info.get("entity_type"),
                "first_seen": now_iso,
                "last_seen": now_iso,
                "discovered_from_channels": json.dumps(discovered_from),
                "forward_count": 1,
                "coordinate_forward_count": 1 if has_coordinates else 0,
                "user_status": "pending",
            }
            self.db.add_recommended_channel(source_channel_id, payload)
            self.db.add_recommendation_event(
                source_channel_id,
                "discovered",
                {
                    "discovered_from": current_channel_id,
                    "message_id": getattr(message, "id", None),
                    "has_coordinates": has_coordinates,
                },
            )
            created = True

        if message_row_id is not None:
            self.db.add_channel_forward(
                message_ref=message_row_id,
                from_channel_id=source_channel_id,
                to_channel_id=current_channel_id,
                had_coordinates=has_coordinates,
                forward_date=forward_info.get("forward_date"),
                forward_signature=forward_info.get("forward_signature"),
            )

        self._recalculate_score(source_channel_id)
        return source_channel_id if created else None

    # ------------------------------------------------------------------
    # Telegram API recommendation harvesting
    async def fetch_telegram_recommendations(
        self,
        client: TelegramClient,
        channel_id: int,
    ) -> List[Dict[str, Any]]:
        """Fetch Telegram's native channel recommendations for *channel_id*."""

        try:
            entity = await client.get_entity(channel_id)
        except (RPCError, ValueError) as exc:
            LOGGER.warning("Unable to resolve entity for channel %s: %s", channel_id, exc)
            return []

        try:
            result = await client(
                GetChannelRecommendationsRequest(channel=entity)
            )
        except RPCError as exc:  # pragma: no cover - Telethon RPC errors are network driven
            error_text = str(exc)
            if "CHANNEL_INVALID" in error_text:
                LOGGER.warning("Channel %s is invalid or inaccessible", channel_id)
            elif "CHANNEL_PRIVATE" in error_text:
                LOGGER.warning("Channel %s is private and cannot provide recommendations", channel_id)
            else:
                LOGGER.error(
                    "Failed to fetch Telegram recommendations for channel %s: %s",
                    channel_id,
                    exc,
                )
            return []
        except Exception as exc:  # pragma: no cover - defensive fallback
            LOGGER.error(
                "Unexpected error fetching Telegram recommendations for channel %s: %s",
                channel_id,
                exc,
            )
            return []

        recommendations: List[Dict[str, Any]] = []
        for chat in getattr(result, "chats", []) or []:
            if not hasattr(chat, "id"):
                continue

            recommendations.append(
                {
                    "channel_id": chat.id,
                    "username": getattr(chat, "username", None),
                    "title": getattr(chat, "title", None),
                    "peer_type": "channel",
                    "participants_count": getattr(chat, "participants_count", None),
                    "verified": getattr(chat, "verified", False),
                    "scam": getattr(chat, "scam", False),
                    "fake": getattr(chat, "fake", False),
                    "has_geo": getattr(chat, "has_geo", False),
                    "restricted": getattr(chat, "restricted", False),
                    "source_channel_id": channel_id,
                }
            )

        LOGGER.info(
            "Fetched %s Telegram recommendations for channel %s",
            len(recommendations),
            channel_id,
        )
        return recommendations

    async def harvest_telegram_recommendations(
        self,
        client: TelegramClient,
        min_coordinate_density: Optional[float] = None,
        max_source_channels: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Harvest Telegram recommendations from high-quality coordinate sources."""

        if not self.settings.enabled or not self.settings.telegram_recs_enabled:
            LOGGER.warning("Telegram recommendation harvesting is disabled")
            return {}

        density_threshold = (
            self.settings.telegram_min_source_density
            if min_coordinate_density is None
            else min_coordinate_density
        )
        channel_limit = (
            self.settings.telegram_max_source_channels
            if max_source_channels is None
            else max_source_channels
        )

        source_channels = self.db.get_channels_with_coordinates(
            min_density=density_threshold,
            limit=channel_limit,
        )

        if not source_channels:
            LOGGER.warning(
                "No source channels found with coordinate density >= %.1f%%",
                density_threshold,
            )
            return {
                "source_channels_checked": 0,
                "new_recommendations": 0,
                "updated_recommendations": 0,
                "total_telegram_suggestions": 0,
                "already_tracked": 0,
                "errors": 0,
            }

        print(
            f"\n🔍 Harvesting Telegram recommendations from {len(source_channels)} high-quality channels..."
        )
        print(
            f"   Using channels with coordinate density >= {density_threshold:.1f}%\n"
        )

        stats = {
            "source_channels_checked": 0,
            "new_recommendations": 0,
            "updated_recommendations": 0,
            "total_telegram_suggestions": 0,
            "already_tracked": 0,
            "errors": 0,
        }

        for idx, source_channel in enumerate(source_channels, 1):
            channel_id = source_channel["id"]
            channel_name = (
                source_channel.get("title")
                or source_channel.get("username")
                or f"ID:{channel_id}"
            )
            density = float(source_channel.get("coordinate_density") or 0.0)

            print(
                f"[{idx}/{len(source_channels)}] Checking: {channel_name} (density: {density:.1f}%)..."
            )

            telegram_recs = await self.fetch_telegram_recommendations(client, channel_id)
            stats["source_channels_checked"] += 1
            stats["total_telegram_suggestions"] += len(telegram_recs)

            if not telegram_recs:
                print("   ⚠️  No recommendations available")
                continue

            print(f"   📥 Found {len(telegram_recs)} Telegram suggestions")

            for rec in telegram_recs:
                rec_channel_id = rec["channel_id"]

                if self._is_already_followed(rec_channel_id):
                    stats["already_tracked"] += 1
                    continue

                existing = self.db.get_recommended_channel(rec_channel_id)

                try:
                    if existing:
                        self._update_telegram_recommendation(
                            rec_channel_id,
                            rec,
                            channel_id,
                            density,
                        )
                        stats["updated_recommendations"] += 1
                    else:
                        self._add_telegram_recommendation(
                            rec,
                            channel_id,
                            density,
                        )
                        stats["new_recommendations"] += 1
                        if self.settings.auto_enrich:
                            try:
                                await self.enrich_recommendation(client, rec_channel_id)
                            except Exception as exc:  # pragma: no cover - defensive logging
                                LOGGER.debug(
                                    "Auto-enrichment failed for Telegram recommendation %s: %s",
                                    rec_channel_id,
                                    exc,
                                )

                        rec_name = (
                            rec.get("title")
                            or rec.get("username")
                            or f"ID:{rec_channel_id}"
                        )
                        print(f"   ✨ New: {rec_name}")
                except sqlite3.DatabaseError as exc:  # pragma: no cover - sqlite errors
                    stats["errors"] += 1
                    LOGGER.error(
                        "Failed to process Telegram recommendation %s: %s",
                        rec_channel_id,
                        exc,
                    )

        print("\n" + "=" * 60)
        print("✅ HARVEST COMPLETE")
        print("=" * 60)
        print(f"Source channels checked:     {stats['source_channels_checked']}")
        print(f"Telegram suggestions found:  {stats['total_telegram_suggestions']}")
        print(f"New recommendations:         {stats['new_recommendations']}")
        print(f"Updated recommendations:     {stats['updated_recommendations']}")
        print(f"Already tracked (skipped):   {stats['already_tracked']}")
        if stats["errors"]:
            print(f"Errors encountered:          {stats['errors']}")
        print("=" * 60 + "\n")

        LOGGER.info(
            "Telegram recommendation harvest complete: checked=%s, new=%s, updated=%s",
            stats["source_channels_checked"],
            stats["new_recommendations"],
            stats["updated_recommendations"],
        )

        return stats

    def _add_telegram_recommendation(
        self,
        rec: Dict[str, Any],
        source_channel_id: int,
        source_density: float,
    ) -> None:
        """Create a recommendation entry for a Telegram API discovery."""

        now_iso = datetime.now(timezone.utc).isoformat()

        payload = {
            "username": rec.get("username"),
            "title": rec.get("title"),
            "channel_type": "channel",
            "peer_type": rec.get("peer_type", "channel"),
            "first_seen": now_iso,
            "last_seen": now_iso,
            "discovered_from_channels": json.dumps([source_channel_id]),
            "discovery_method": "telegram_api",
            "forward_count": 0,
            "coordinate_forward_count": 0,
            "telegram_recommendation_count": 1,
            "telegram_rec_source_density": float(source_density),
            "user_status": "pending",
            "subscriber_count": rec.get("participants_count"),
            "is_verified": rec.get("verified", False),
            "is_scam": rec.get("scam", False),
            "is_fake": rec.get("fake", False),
            "is_accessible": not rec.get("restricted", False),
            "last_harvest_date": now_iso,
        }

        self.db.add_recommended_channel(rec["channel_id"], payload)
        self._recalculate_score(rec["channel_id"])
        self.db.add_recommendation_event(
            rec["channel_id"],
            "discovered_telegram_api",
            {
                "source_channel": source_channel_id,
                "source_density": source_density,
                "telegram_metadata": rec,
            },
        )

    def _update_telegram_recommendation(
        self,
        rec_channel_id: int,
        rec: Dict[str, Any],
        source_channel_id: int,
        source_density: float,
    ) -> None:
        """Update an existing recommendation with Telegram API metadata."""

        existing = self.db.get_recommended_channel(rec_channel_id)
        if not existing:
            return

        now_iso = datetime.now(timezone.utc).isoformat()
        sources = self._merge_sources(existing, source_channel_id)

        telegram_count = int(existing.get("telegram_recommendation_count", 0) or 0) + 1
        old_avg = float(existing.get("telegram_rec_source_density", 0.0) or 0.0)
        new_avg = ((old_avg * (telegram_count - 1)) + float(source_density)) / telegram_count

        update_data: Dict[str, Any] = {
            "last_seen": now_iso,
            "discovered_from_channels": json.dumps(sources),
            "telegram_recommendation_count": telegram_count,
            "telegram_rec_source_density": new_avg,
            "last_harvest_date": now_iso,
        }

        if not existing.get("title") and rec.get("title"):
            update_data["title"] = rec["title"]
        if not existing.get("username") and rec.get("username"):
            update_data["username"] = rec["username"]
        if not existing.get("peer_type") and rec.get("peer_type"):
            update_data["peer_type"] = rec["peer_type"]
        if not existing.get("subscriber_count") and rec.get("participants_count"):
            update_data["subscriber_count"] = rec["participants_count"]

        self.db.update_recommended_channel(rec_channel_id, update_data)
        self._recalculate_score(rec_channel_id)

    # ------------------------------------------------------------------
    # Recommendation retrieval helpers
    def get_top_recommendations(
        self,
        limit: int = 10,
        min_score: Optional[float] = None,
        min_hit_rate: Optional[float] = None,
        status: Optional[str] = "pending",
    ) -> List[Dict[str, Any]]:
        """Return highest scoring recommended channels with optional hit rate filter."""

        if not self.settings.enabled:
            return []

        min_score_value = self.settings.min_score if min_score is None else min_score
        min_hit_rate_value = (
            self.settings.min_hit_rate if min_hit_rate is None else min_hit_rate
        )

        recommendations = self.list_recommendations(
            status=status,
            order_by="recommendation_score DESC, coordinate_forward_count DESC",
            limit=None,
        )

        filtered: List[Dict[str, Any]] = []
        for rec in recommendations:
            score_value = float(rec.get("recommendation_score") or 0.0)
            if min_score_value is not None and score_value < min_score_value:
                continue

            forward_count = int(rec.get("forward_count") or 0)
            coord_count = int(rec.get("coordinate_forward_count") or 0)

            if self.settings.hide_zero_coordinates and coord_count == 0:
                continue

            if min_hit_rate_value is not None:
                if forward_count > 0:
                    hit_rate = (coord_count / forward_count) * 100
                    if hit_rate < min_hit_rate_value:
                        continue
                else:
                    # No sample yet – keep unless explicitly hidden
                    if self.settings.hide_zero_coordinates:
                        continue

            filtered.append(rec)

            if len(filtered) >= limit:
                break

        return filtered

    def list_recommendations(
        self,
        status: Optional[str] = None,
        order_by: str = "recommendation_score DESC, coordinate_forward_count DESC",
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        params: List[Any] = []
        conditions: List[str] = []
        if status:
            conditions.append("user_status = ?")
            params.append(status)
        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        limit_clause = ""
        if limit is not None:
            limit_clause = " LIMIT ?"
            params.append(limit)
        rows = self.db.query(
            f"SELECT * FROM recommended_channels {where_clause} ORDER BY {order_by}{limit_clause}",
            params,
        )
        return [dict(row) for row in rows]

    def recalculate_all_scores(self, verbose: bool = False) -> int:
        """Recalculate recommendation scores for every channel.

        Returns the number of records whose score changed.
        """

        rows = self.db.query("SELECT * FROM recommended_channels ORDER BY channel_id")
        updated = 0

        for row in rows:
            row_dict = dict(row)
            channel_id = row_dict["channel_id"]
            old_score = float(row_dict.get("recommendation_score") or 0.0)
            new_score = self.calculate_recommendation_score(row_dict)

            if abs(new_score - old_score) <= 0.1:
                continue

            self.db.update_recommended_channel(
                channel_id,
                {"recommendation_score": new_score},
            )
            updated += 1

            if verbose:
                forward_count = int(row_dict.get("forward_count") or 0)
                coord_count = int(row_dict.get("coordinate_forward_count") or 0)
                hit_rate = (coord_count / forward_count * 100) if forward_count else 0.0
                LOGGER.info(
                    "Recalculated score for %s: %.1f -> %.1f (hit rate %.1f%%)",
                    row_dict.get("username") or channel_id,
                    old_score,
                    new_score,
                    hit_rate,
                )

        return updated

    def search_recommendations(self, term: str, status: Optional[str] = None) -> List[Dict[str, Any]]:
        term_like = f"%{term.lower()}%"
        params: List[Any] = [term_like, term_like, term_like]
        condition = (
            "(LOWER(COALESCE(username, '')) LIKE ? "
            "OR LOWER(COALESCE(title, '')) LIKE ? "
            "OR CAST(channel_id AS TEXT) LIKE ?)"
        )
        if status:
            condition += " AND user_status = ?"
            params.append(status)
        sql = (
            "SELECT * FROM recommended_channels WHERE "
            + condition
            + " ORDER BY recommendation_score DESC"
        )
        rows = self.db.query(sql, params)
        return [dict(row) for row in rows]

    def get_recommendation_statistics(self) -> Dict[str, Any]:
        if not self.settings.enabled:
            return {
                "total_recommended": 0,
                "pending": 0,
                "accepted": 0,
                "rejected": 0,
                "inaccessible": 0,
                "top_score": 0,
                "total_forwards_tracked": 0,
                "coordinate_forwards": 0,
            }

        top_score_row = self.db.query_one(
            "SELECT MAX(recommendation_score) AS max_score FROM recommended_channels"
        )
        return {
            "total_recommended": self.db.count("recommended_channels"),
            "pending": self.db.count("recommended_channels", "user_status='pending'"),
            "accepted": self.db.count("recommended_channels", "user_status='accepted'"),
            "rejected": self.db.count("recommended_channels", "user_status='rejected'"),
            "inaccessible": self.db.count("recommended_channels", "user_status='inaccessible'"),
            "top_score": float(top_score_row["max_score"]) if top_score_row and top_score_row["max_score"] is not None else 0.0,
            "total_forwards_tracked": self.db.count("channel_forwards"),
            "coordinate_forwards": self.db.count("channel_forwards", "had_coordinates=1"),
        }

    # ------------------------------------------------------------------
    # User actions
    def mark_recommendation_status(self, channel_id: int, status: str, notes: Optional[str] = None) -> None:
        now_iso = datetime.now(timezone.utc).isoformat()
        update_payload: Dict[str, Any] = {
            "user_status": status,
            "user_notes": notes,
        }
        if status == "accepted":
            update_payload["added_to_scrape_list"] = now_iso
        elif status == "scraped":
            update_payload["last_scraped"] = now_iso

        self.db.update_recommended_channel(channel_id, update_payload)
        self.db.add_recommendation_event(
            channel_id=channel_id,
            event_type=status,
            details={"notes": notes},
        )

    async def enrich_recommendation(self, client: TelegramClient, channel_id: int) -> bool:
        record = self.db.get_recommended_channel(channel_id) if self.db else None
        peer_type = record.get("peer_type") if record else None
        username = record.get("username") if record else None

        try:
            entity = await self._resolve_entity(
                client,
                channel_id,
                peer_type=peer_type,
                username=username,
            )

            if not self._is_channel_entity(entity):
                entity_type = type(entity).__name__
                LOGGER.warning(
                    "Entity %s is not a channel (type: %s)", channel_id, entity_type
                )
                self.db.update_recommended_channel(
                    channel_id,
                    {
                        "is_accessible": False,
                        "user_status": "invalid_entity_type",
                        "user_notes": f"Not a channel (type: {entity_type})",
                        "entity_type": entity_type.lower(),
                    },
                )
                self.db.add_recommendation_event(
                    channel_id,
                    "enrichment_failed",
                    {"reason": "invalid_entity_type", "type": entity_type},
                )
                return False
        except (RPCError, ValueError) as exc:  # pragma: no cover - Telethon RPC errors
            error_text = str(exc)
            LOGGER.warning("Failed to fetch entity for channel %s: %s", channel_id, exc)

            update_payload: Dict[str, Any] = {
                "is_accessible": False,
            }

            if "PeerUser" in error_text or "USER_ID_INVALID" in error_text:
                update_payload.update(
                    {
                        "user_status": "invalid_entity_type",
                        "user_notes": "This is a user ID, not a channel",
                        "entity_type": "user",
                    }
                )
            elif "CHANNEL_PRIVATE" in error_text:
                update_payload.update(
                    {
                        "user_status": "private",
                        "user_notes": "Private channel - requires invitation",
                        "requires_join": True,
                    }
                )
            elif "CHANNEL_INVALID" in error_text:
                update_payload.update(
                    {
                        "user_status": "inaccessible",
                        "user_notes": "Channel invalid or deleted",
                    }
                )
            else:
                update_payload.setdefault("user_status", "inaccessible")

            self.db.update_recommended_channel(channel_id, update_payload)
            if update_payload.get("user_status") == "invalid_entity_type":
                self.db.add_recommendation_event(
                    channel_id,
                    "enrichment_failed",
                    {"reason": "invalid_entity_type"},
                )
            return False
        except Exception as exc:  # pragma: no cover - defensive fallback
            LOGGER.error(
                "Unexpected error enriching channel %s: %s", channel_id, exc
            )
            return False

        entity_type = self._map_entity_type(entity)
        enrichment_data = {
            "title": getattr(entity, "title", None),
            "username": getattr(entity, "username", None),
            "is_verified": getattr(entity, "verified", False),
            "is_scam": getattr(entity, "scam", False),
            "is_fake": getattr(entity, "fake", False),
            "subscriber_count": getattr(entity, "participants_count", None),
            "is_accessible": True,
            "requires_join": not bool(getattr(entity, "username", None)),
            "entity_type": entity_type,
        }
        self.db.update_recommended_channel(channel_id, enrichment_data)
        self._recalculate_score(channel_id)
        return True

    # ------------------------------------------------------------------
    # Export helpers
    def export_recommendations(self, status: Optional[str] = None) -> List[Dict[str, Any]]:
        return self.list_recommendations(status=status, order_by="recommendation_score DESC")

    # ------------------------------------------------------------------
    # Internal helpers
    def _is_already_followed(self, channel_id: int) -> bool:
        return self.db.get_channel_info(channel_id) is not None

    def _extract_forward_info(self, message) -> Optional[Dict[str, Any]]:
        header = getattr(message, "forward", None)
        if not header:
            return None

        peer_details = self._parse_forward_peer(getattr(header, "from_id", None))
        if not peer_details:
            return None

        channel_id = peer_details["id"]

        return {
            "channel_id": int(channel_id),
            "peer_type": peer_details.get("peer_type"),
            "entity_type": peer_details.get("entity_type"),
            "channel_type": peer_details.get("channel_type"),
            "forward_date": getattr(header, "date", None),
            "forward_signature": getattr(header, "post_author", None),
            "title": getattr(header, "from_name", None),
        }

    @staticmethod
    def _parse_forward_peer(peer) -> Optional[Dict[str, Any]]:
        if peer is None:
            return None

        if isinstance(peer, PeerChannel):
            return {
                "id": int(peer.channel_id),
                "peer_type": "channel",
                "entity_type": "channel",
                "channel_type": "channel",
            }

        if isinstance(peer, PeerChat):
            return {
                "id": int(peer.chat_id),
                "peer_type": "chat",
                "entity_type": "supergroup",
                "channel_type": "supergroup",
            }

        if isinstance(peer, PeerUser):
            return {
                "id": int(peer.user_id),
                "peer_type": "user",
                "entity_type": "user",
                "channel_type": None,
            }

        return None

    def _is_valid_channel_id(
        self, channel_id: int, forward_info: Dict[str, Any]
    ) -> bool:
        peer_type = self._normalise_peer_type(forward_info.get("peer_type"))
        if peer_type == "user":
            return False

        if peer_type in {"channel", "chat"}:
            return True

        entity_type = forward_info.get("entity_type")

        if entity_type == "user":
            return False

        if entity_type in {"channel", "supergroup", "megagroup"}:
            return True

        if channel_id < 1_000_000_000:
            LOGGER.debug(
                "Rejecting potential user ID %s (below channel ID threshold)",
                channel_id,
            )
            return False

        return True

    def _is_channel_entity(self, entity: Any) -> bool:
        return isinstance(entity, (Channel, Chat))

    def _map_entity_type(self, entity: Any) -> str:
        if isinstance(entity, Channel):
            if getattr(entity, "megagroup", False):
                return "supergroup"
            return "channel"
        if isinstance(entity, Chat):
            return "group"
        return type(entity).__name__.lower()

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

    async def _find_entity_in_dialogs(
        self, client: TelegramClient, entity_id: Optional[int]
    ) -> Optional[Any]:
        if entity_id is None:
            return None

        try:
            target_id = int(entity_id)
        except (TypeError, ValueError):
            return None

        async for dialog in client.iter_dialogs():
            dialog_entity = getattr(dialog, "entity", None)
            if getattr(dialog_entity, "id", None) == target_id:
                return dialog_entity
        return None

    async def _resolve_entity(
        self,
        client: TelegramClient,
        channel_id: int,
        *,
        peer_type: Optional[str] = None,
        username: Optional[str] = None,
    ):
        normalised_peer = self._normalise_peer_type(peer_type)
        peer_reference = self._build_peer_reference(normalised_peer, channel_id)

        if peer_reference is not None:
            try:
                return await client.get_entity(peer_reference)
            except (RPCError, ValueError):
                LOGGER.debug(
                    "Failed to resolve entity %s using peer type %s", channel_id, normalised_peer
                )

        if username:
            try:
                return await client.get_entity(username)
            except (RPCError, ValueError):
                LOGGER.debug(
                    "Failed to resolve entity %s via username %s", channel_id, username
                )

        try:
            return await client.get_entity(channel_id)
        except (RPCError, ValueError):
            LOGGER.debug("Direct resolution by id failed for entity %s", channel_id)

        entity = await self._find_entity_in_dialogs(client, channel_id)
        if entity:
            return entity

        raise ValueError(
            f"Cannot resolve entity {channel_id} of type {normalised_peer or 'unknown'}"
        )

    def get_recommended_channel(self, channel_id: int) -> Optional[Dict[str, Any]]:
        return self.db.get_recommended_channel(channel_id)

    def _merge_sources(self, existing: Optional[Dict[str, Any]], new_source: int) -> List[int]:
        sources: List[int] = []
        if existing and existing.get("discovered_from_channels"):
            try:
                decoded = json.loads(existing["discovered_from_channels"])
                if isinstance(decoded, list):
                    sources = [int(item) for item in decoded if isinstance(item, int)]
            except (json.JSONDecodeError, TypeError, ValueError):
                LOGGER.debug("Failed to decode discovered_from_channels for %s", existing.get("channel_id"))
        if new_source not in sources:
            sources.append(new_source)
        return sources

    def _recalculate_score(self, channel_id: int) -> None:
        record = self.db.get_recommended_channel(channel_id)
        if not record:
            return
        score = self.calculate_recommendation_score(record)
        self.db.update_recommended_channel(channel_id, {"recommendation_score": score})

    # ------------------------------------------------------------------
    # Scoring
    def calculate_recommendation_score(self, channel_data: Dict[str, Any]) -> float:
        """Calculate a recommendation score that prioritises coordinate hit rate."""

        forward_count = int(channel_data.get("forward_count") or 0)
        coordinate_forward_count = int(channel_data.get("coordinate_forward_count") or 0)

        if forward_count == 0:
            # Neutral score for channels we have not yet observed.
            return 50.0

        hit_rate = coordinate_forward_count / forward_count if forward_count else 0.0

        if hit_rate >= 0.80:
            quality_score = 60.0
        elif hit_rate >= 0.60:
            quality_score = 55.0
        elif hit_rate >= 0.40:
            quality_score = 45.0
        elif hit_rate >= 0.20:
            quality_score = 30.0
        elif hit_rate >= 0.10:
            quality_score = 15.0
        elif hit_rate >= 0.05:
            quality_score = 5.0
        else:
            quality_score = 0.0

        if forward_count >= 200:
            confidence_modifier = 1.25
        elif forward_count >= 100:
            confidence_modifier = 1.2
        elif forward_count >= 50:
            confidence_modifier = 1.1
        elif forward_count >= 20:
            confidence_modifier = 1.0
        elif forward_count >= 10:
            confidence_modifier = 0.9
        else:
            confidence_modifier = 0.8

        base_score = quality_score * confidence_modifier

        if self.settings.penalize_low_sample and forward_count < 20:
            base_score *= 0.85

        trust_score = 0.0

        source_count = 0
        sources_raw = channel_data.get("discovered_from_channels")
        if sources_raw:
            if isinstance(sources_raw, str):
                try:
                    sources_list = json.loads(sources_raw)
                except json.JSONDecodeError:
                    sources_list = []
            else:
                sources_list = sources_raw
            if isinstance(sources_list, (list, tuple, set)):
                source_count = len({int(item) for item in sources_list if isinstance(item, int)})

        if source_count >= 5:
            trust_score += 15.0
        elif source_count >= 3:
            trust_score += 10.0
        elif source_count >= 2:
            trust_score += 5.0
        elif source_count >= 1:
            trust_score += 2.0

        last_seen_value = channel_data.get("last_seen")
        last_seen_dt: Optional[datetime]
        if last_seen_value:
            if isinstance(last_seen_value, str):
                try:
                    last_seen_dt = datetime.fromisoformat(last_seen_value.replace("Z", "+00:00"))
                except ValueError:
                    last_seen_dt = None
            elif isinstance(last_seen_value, datetime):
                last_seen_dt = last_seen_value
            else:
                last_seen_dt = None
            if last_seen_dt:
                days_since = (datetime.now(timezone.utc) - last_seen_dt).days
                if days_since < 7:
                    trust_score += 10.0
                elif days_since < 30:
                    trust_score += 7.0
                elif days_since < 90:
                    trust_score += 3.0
        else:
            last_seen_dt = None

        if channel_data.get("is_verified"):
            trust_score += 10.0

        subscriber_count = channel_data.get("subscriber_count") or 0
        try:
            subscriber_count = int(subscriber_count)
        except (TypeError, ValueError):
            subscriber_count = 0

        if subscriber_count > 100000:
            trust_score += 5.0
        elif subscriber_count > 10000:
            trust_score += 3.0
        elif subscriber_count > 1000:
            trust_score += 1.0

        telegram_rec_count = int(channel_data.get("telegram_recommendation_count", 0) or 0)
        if telegram_rec_count > 0:
            avg_source_density = float(channel_data.get("telegram_rec_source_density", 0.0) or 0.0)
            if avg_source_density > 10.0:
                trust_score += 10.0
            elif avg_source_density > 5.0:
                trust_score += 5.0
            elif avg_source_density > 0.0:
                trust_score += 2.0

        if channel_data.get("is_scam") or channel_data.get("is_fake"):
            return 0.0

        if channel_data.get("is_accessible") is False:
            base_score *= 0.5

        quality_weight = self.settings.quality_weight or 0.0
        trust_weight = self.settings.trust_weight or 0.0

        # Normalise weights relative to default values so existing behaviour matches defaults.
        quality_multiplier = quality_weight / 0.6 if quality_weight else 0.0
        trust_multiplier = trust_weight / 0.4 if trust_weight else 0.0

        if not quality_multiplier and not trust_multiplier:
            quality_multiplier = 1.0
            trust_multiplier = 1.0

        final_score = (base_score * quality_multiplier) + (trust_score * trust_multiplier)
        final_score = min(100.0, max(0.0, final_score))
        return final_score


__all__ = ["RecommendationManager", "RecommendationSettings"]
