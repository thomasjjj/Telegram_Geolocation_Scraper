"""Utilities for discovering and managing recommended Telegram channels."""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from telethon import TelegramClient
from telethon.tl.types import PeerChannel, PeerChat, PeerUser

from src.database import CoordinatesDatabase

LOGGER = logging.getLogger(__name__)


@dataclass
class RecommendationSettings:
    """Configuration options for the recommendation system."""

    enabled: bool = True
    min_score: float = 30.0
    show_at_startup: bool = True
    auto_enrich: bool = False
    max_display: int = 5

    @classmethod
    def from_environment(cls) -> "RecommendationSettings":
        """Load settings from environment variables."""

        def _as_bool(value: Optional[str], default: bool) -> bool:
            if value is None:
                return default
            return value.lower() in {"1", "true", "yes", "on"}

        enabled = _as_bool(os.environ.get("RECOMMENDATIONS_ENABLED"), True)
        min_score = float(os.environ.get("RECOMMENDATIONS_MIN_SCORE", 30.0))
        show_at_startup = _as_bool(os.environ.get("RECOMMENDATIONS_SHOW_AT_STARTUP"), True)
        auto_enrich = _as_bool(os.environ.get("RECOMMENDATIONS_AUTO_ENRICH"), False)
        max_display = int(os.environ.get("RECOMMENDATIONS_MAX_DISPLAY", 5))

        return cls(
            enabled=enabled,
            min_score=min_score,
            show_at_startup=show_at_startup,
            auto_enrich=auto_enrich,
            max_display=max_display,
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
    ) -> bool:
        """Process a forwarded message and update recommendation records."""

        if not self.settings.enabled or not message or not getattr(message, "forward", None):
            return False

        forward_info = self._extract_forward_info(message)
        if not forward_info:
            return False

        source_channel_id = forward_info["channel_id"]
        if self._is_already_followed(source_channel_id):
            return False

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

            self.db.update_recommended_channel(source_channel_id, update_data)
            created = False
        else:
            payload = {
                "username": forward_info.get("username"),
                "title": forward_info.get("title"),
                "channel_type": forward_info.get("channel_type"),
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
        return created

    # ------------------------------------------------------------------
    # Recommendation retrieval helpers
    def get_top_recommendations(
        self,
        limit: int = 10,
        min_score: Optional[float] = None,
        status: Optional[str] = "pending",
    ) -> List[Dict[str, Any]]:
        """Return highest scoring recommended channels."""

        if not self.settings.enabled:
            return []

        min_score_value = self.settings.min_score if min_score is None else min_score
        params: List[Any] = []
        conditions: List[str] = []

        if min_score_value is not None:
            conditions.append("recommendation_score >= ?")
            params.append(min_score_value)
        if status:
            conditions.append("user_status = ?")
            params.append(status)

        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        params.append(limit)
        rows = self.db.query(
            f"""
            SELECT * FROM recommended_channels
            {where_clause}
            ORDER BY recommendation_score DESC, coordinate_forward_count DESC
            LIMIT ?
            """,
            params,
        )
        return [dict(row) for row in rows]

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
        try:
            entity = await client.get_entity(channel_id)
        except Exception as exc:  # pragma: no cover - Telethon RPC errors
            LOGGER.warning("Failed to fetch entity for channel %s: %s", channel_id, exc)
            self.db.update_recommended_channel(
                channel_id,
                {
                    "is_accessible": False,
                    "user_status": "inaccessible",
                },
            )
            return False

        enrichment_data = {
            "title": getattr(entity, "title", None),
            "username": getattr(entity, "username", None),
            "is_verified": getattr(entity, "verified", False),
            "is_scam": getattr(entity, "scam", False),
            "is_fake": getattr(entity, "fake", False),
            "subscriber_count": getattr(entity, "participants_count", None),
            "is_accessible": True,
            "requires_join": not bool(getattr(entity, "username", None)),
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

        peer = getattr(header, "from_id", None)
        channel_id: Optional[int] = None
        if isinstance(peer, PeerChannel):
            channel_id = peer.channel_id
        elif isinstance(peer, PeerChat):
            channel_id = peer.chat_id
        elif isinstance(peer, PeerUser):
            return None

        if channel_id is None:
            return None

        return {
            "channel_id": int(channel_id),
            "forward_date": getattr(header, "date", None),
            "forward_signature": getattr(header, "post_author", None),
            "title": getattr(header, "from_name", None),
        }

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
    @staticmethod
    def calculate_recommendation_score(channel_data: Dict[str, Any]) -> float:
        score = 0.0

        forward_count = int(channel_data.get("forward_count") or 0)
        score += min(30.0, forward_count * 2.0)

        coordinate_forward_count = int(channel_data.get("coordinate_forward_count") or 0)
        if forward_count > 0:
            hit_rate = coordinate_forward_count / forward_count
            score += hit_rate * 25.0

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
        score += min(20.0, source_count * 4.0)

        last_seen_value = channel_data.get("last_seen")
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
                    score += 10.0
                elif days_since < 30:
                    score += 5.0

        if channel_data.get("is_verified"):
            score += 10.0
        if channel_data.get("is_scam") or channel_data.get("is_fake"):
            score -= 50.0

        subscriber_count = channel_data.get("subscriber_count") or 0
        try:
            subscriber_count = int(subscriber_count)
        except (TypeError, ValueError):
            subscriber_count = 0
        if subscriber_count > 10000:
            score += 5.0
        elif subscriber_count > 1000:
            score += 2.0

        return max(0.0, min(100.0, score))


__all__ = ["RecommendationManager", "RecommendationSettings"]
