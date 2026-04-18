from __future__ import annotations

import json
import logging
import sqlite3
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable, Optional


LOGGER = logging.getLogger(__name__)


def utc_now_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


@dataclass
class AssetRecord:
    source_name: str
    source_asset_id: str
    source_url: str
    title: str
    creator: str = ""
    license_type: str = ""
    license_url: str = ""
    duration_seconds: Optional[float] = None
    tags: list[str] = field(default_factory=list)
    thumbnail_url: str = ""
    media_url: str = ""
    local_path: str = ""
    processed_path: str = ""
    attribution_required: bool = False
    attribution_text: str = ""
    rights_status: str = "unknown"
    rights_reason: str = ""
    proposed_caption: str = ""
    proposed_hashtags: str = ""
    source_attribution: str = ""
    manual_approved: bool = False
    review_notes: str = ""
    scheduled_for: str = ""
    media_hash: str = ""
    posted_at: str = ""
    subtitle_file: str = ""
    asset_id: Optional[int] = None
    created_at: str = field(default_factory=utc_now_iso)
    updated_at: str = field(default_factory=utc_now_iso)

    @property
    def is_posted(self) -> bool:
        return bool(self.posted_at)

    @property
    def has_allowed_rights(self) -> bool:
        return self.rights_status in {
            "public_domain",
            "cc_commercial",
            "licensed_stock",
            "owned",
            "ai_owned",
        }


class Storage:
    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(self.db_path)
        self.connection.row_factory = sqlite3.Row
        self.initialize()

    def initialize(self) -> None:
        with self.connection:
            self.connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS assets (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_name TEXT NOT NULL,
                    source_asset_id TEXT NOT NULL,
                    source_url TEXT NOT NULL,
                    title TEXT NOT NULL,
                    creator TEXT NOT NULL DEFAULT '',
                    license_type TEXT NOT NULL DEFAULT '',
                    license_url TEXT NOT NULL DEFAULT '',
                    duration_seconds REAL,
                    tags_json TEXT NOT NULL DEFAULT '[]',
                    thumbnail_url TEXT NOT NULL DEFAULT '',
                    media_url TEXT NOT NULL DEFAULT '',
                    local_path TEXT NOT NULL DEFAULT '',
                    processed_path TEXT NOT NULL DEFAULT '',
                    attribution_required INTEGER NOT NULL DEFAULT 0,
                    attribution_text TEXT NOT NULL DEFAULT '',
                    rights_status TEXT NOT NULL DEFAULT 'unknown',
                    rights_reason TEXT NOT NULL DEFAULT '',
                    proposed_caption TEXT NOT NULL DEFAULT '',
                    proposed_hashtags TEXT NOT NULL DEFAULT '',
                    source_attribution TEXT NOT NULL DEFAULT '',
                    manual_approved INTEGER NOT NULL DEFAULT 0,
                    review_notes TEXT NOT NULL DEFAULT '',
                    scheduled_for TEXT NOT NULL DEFAULT '',
                    media_hash TEXT NOT NULL DEFAULT '',
                    posted_at TEXT NOT NULL DEFAULT '',
                    subtitle_file TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(source_name, source_asset_id)
                );

                CREATE TABLE IF NOT EXISTS posts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    asset_id INTEGER NOT NULL,
                    uploader_name TEXT NOT NULL,
                    status TEXT NOT NULL,
                    external_post_id TEXT NOT NULL DEFAULT '',
                    dry_run INTEGER NOT NULL DEFAULT 0,
                    media_hash TEXT NOT NULL DEFAULT '',
                    error_message TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(asset_id) REFERENCES assets(id)
                );

                CREATE TABLE IF NOT EXISTS logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    level TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    message TEXT NOT NULL,
                    asset_id INTEGER,
                    details_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_assets_rights_status ON assets(rights_status);
                CREATE INDEX IF NOT EXISTS idx_assets_manual_approved ON assets(manual_approved);
                CREATE INDEX IF NOT EXISTS idx_assets_scheduled_for ON assets(scheduled_for);
                CREATE INDEX IF NOT EXISTS idx_assets_posted_at ON assets(posted_at);
                CREATE INDEX IF NOT EXISTS idx_posts_asset_id ON posts(asset_id);
                """
            )

    def close(self) -> None:
        self.connection.close()

    def upsert_asset(self, asset: AssetRecord) -> AssetRecord:
        now = utc_now_iso()
        asset.updated_at = now
        with self.connection:
            self.connection.execute(
                """
                INSERT INTO assets (
                    source_name, source_asset_id, source_url, title, creator,
                    license_type, license_url, duration_seconds, tags_json,
                    thumbnail_url, media_url, local_path, processed_path,
                    attribution_required, attribution_text, rights_status,
                    rights_reason, proposed_caption, proposed_hashtags,
                    source_attribution, manual_approved, review_notes,
                    scheduled_for, media_hash, posted_at, subtitle_file,
                    created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(source_name, source_asset_id) DO UPDATE SET
                    source_url = excluded.source_url,
                    title = excluded.title,
                    creator = excluded.creator,
                    license_type = excluded.license_type,
                    license_url = excluded.license_url,
                    duration_seconds = excluded.duration_seconds,
                    tags_json = excluded.tags_json,
                    thumbnail_url = excluded.thumbnail_url,
                    media_url = excluded.media_url,
                    local_path = CASE
                        WHEN excluded.local_path != '' THEN excluded.local_path
                        ELSE assets.local_path
                    END,
                    processed_path = CASE
                        WHEN excluded.processed_path != '' THEN excluded.processed_path
                        ELSE assets.processed_path
                    END,
                    attribution_required = excluded.attribution_required,
                    attribution_text = excluded.attribution_text,
                    rights_status = excluded.rights_status,
                    rights_reason = excluded.rights_reason,
                    proposed_caption = excluded.proposed_caption,
                    proposed_hashtags = excluded.proposed_hashtags,
                    source_attribution = excluded.source_attribution,
                    media_hash = CASE
                        WHEN excluded.media_hash != '' THEN excluded.media_hash
                        ELSE assets.media_hash
                    END,
                    subtitle_file = CASE
                        WHEN excluded.subtitle_file != '' THEN excluded.subtitle_file
                        ELSE assets.subtitle_file
                    END,
                    updated_at = excluded.updated_at
                """,
                (
                    asset.source_name,
                    asset.source_asset_id,
                    asset.source_url,
                    asset.title,
                    asset.creator,
                    asset.license_type,
                    asset.license_url,
                    asset.duration_seconds,
                    json.dumps(asset.tags),
                    asset.thumbnail_url,
                    asset.media_url,
                    asset.local_path,
                    asset.processed_path,
                    int(asset.attribution_required),
                    asset.attribution_text,
                    asset.rights_status,
                    asset.rights_reason,
                    asset.proposed_caption,
                    asset.proposed_hashtags,
                    asset.source_attribution,
                    int(asset.manual_approved),
                    asset.review_notes,
                    asset.scheduled_for,
                    asset.media_hash,
                    asset.posted_at,
                    asset.subtitle_file,
                    asset.created_at,
                    asset.updated_at,
                ),
            )
        stored = self.get_asset_by_source(asset.source_name, asset.source_asset_id)
        if stored is None:
            raise RuntimeError("Asset upsert failed unexpectedly.")
        return stored

    def get_asset_by_source(self, source_name: str, source_asset_id: str) -> Optional[AssetRecord]:
        row = self.connection.execute(
            """
            SELECT * FROM assets
            WHERE source_name = ? AND source_asset_id = ?
            """,
            (source_name, source_asset_id),
        ).fetchone()
        return self._row_to_asset(row) if row else None

    def get_asset(self, asset_id: int) -> Optional[AssetRecord]:
        row = self.connection.execute(
            "SELECT * FROM assets WHERE id = ?",
            (asset_id,),
        ).fetchone()
        return self._row_to_asset(row) if row else None

    def list_assets(
        self,
        *,
        manual_approved: Optional[bool] = None,
        rights_status: Optional[str] = None,
        scheduled_only: bool = False,
        include_posted: bool = True,
    ) -> list[AssetRecord]:
        query = "SELECT * FROM assets WHERE 1=1"
        params: list[Any] = []
        if manual_approved is not None:
            query += " AND manual_approved = ?"
            params.append(int(manual_approved))
        if rights_status:
            query += " AND rights_status = ?"
            params.append(rights_status)
        if scheduled_only:
            query += " AND scheduled_for != ''"
        if not include_posted:
            query += " AND posted_at = ''"
        query += " ORDER BY updated_at DESC"
        rows = self.connection.execute(query, params).fetchall()
        return [self._row_to_asset(row) for row in rows]

    def list_unscheduled_approved_assets(self) -> list[AssetRecord]:
        rows = self.connection.execute(
            """
            SELECT * FROM assets
            WHERE manual_approved = 1
              AND posted_at = ''
              AND scheduled_for = ''
              AND rights_status IN ('public_domain', 'cc_commercial', 'licensed_stock', 'owned', 'ai_owned')
            ORDER BY created_at ASC
            """
        ).fetchall()
        return [self._row_to_asset(row) for row in rows]

    def list_due_assets(self, now_iso: str) -> list[AssetRecord]:
        rows = self.connection.execute(
            """
            SELECT * FROM assets
            WHERE manual_approved = 1
              AND posted_at = ''
              AND scheduled_for != ''
              AND scheduled_for <= ?
              AND rights_status IN ('public_domain', 'cc_commercial', 'licensed_stock', 'owned', 'ai_owned')
            ORDER BY scheduled_for ASC
            """,
            (now_iso,),
        ).fetchall()
        return [self._row_to_asset(row) for row in rows]

    def update_asset(self, asset_id: int, **fields: Any) -> None:
        if not fields:
            return
        fields["updated_at"] = utc_now_iso()
        assignments = ", ".join(f"{key} = ?" for key in fields)
        values = list(fields.values()) + [asset_id]
        with self.connection:
            self.connection.execute(
                f"UPDATE assets SET {assignments} WHERE id = ?",
                values,
            )

    def approve_asset(self, asset_id: int, review_notes: str = "") -> None:
        self.update_asset(asset_id, manual_approved=1, review_notes=review_notes)

    def reject_asset(self, asset_id: int, review_notes: str) -> None:
        self.update_asset(
            asset_id,
            manual_approved=0,
            rights_status="rejected",
            rights_reason=review_notes,
            review_notes=review_notes,
            scheduled_for="",
        )

    def schedule_asset(self, asset_id: int, scheduled_for: str) -> None:
        self.update_asset(asset_id, scheduled_for=scheduled_for)

    def mark_processed(self, asset_id: int, processed_path: str, media_hash: str = "") -> None:
        self.update_asset(asset_id, processed_path=processed_path, media_hash=media_hash)

    def mark_posted(self, asset_id: int, posted_at: str, media_hash: str, external_post_id: str = "") -> None:
        self.update_asset(asset_id, posted_at=posted_at, media_hash=media_hash)

    def record_post_attempt(
        self,
        *,
        asset_id: int,
        uploader_name: str,
        status: str,
        external_post_id: str,
        dry_run: bool,
        media_hash: str,
        error_message: str,
    ) -> None:
        with self.connection:
            self.connection.execute(
                """
                INSERT INTO posts (
                    asset_id, uploader_name, status, external_post_id, dry_run,
                    media_hash, error_message, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    asset_id,
                    uploader_name,
                    status,
                    external_post_id,
                    int(dry_run),
                    media_hash,
                    error_message,
                    utc_now_iso(),
                ),
            )

    def has_posted_hash(self, media_hash: str) -> bool:
        if not media_hash:
            return False
        row = self.connection.execute(
            """
            SELECT 1
            FROM posts
            WHERE media_hash = ? AND status = 'posted'
            LIMIT 1
            """,
            (media_hash,),
        ).fetchone()
        return row is not None

    def has_posted_source(self, source_name: str, source_asset_id: str) -> bool:
        row = self.connection.execute(
            """
            SELECT 1
            FROM assets
            WHERE source_name = ? AND source_asset_id = ? AND posted_at != ''
            LIMIT 1
            """,
            (source_name, source_asset_id),
        ).fetchone()
        return row is not None

    def count_posts_on_date(self, day: date) -> int:
        prefix = day.isoformat()
        row = self.connection.execute(
            """
            SELECT COUNT(*) AS count
            FROM assets
            WHERE posted_at LIKE ?
            """,
            (f"{prefix}%",),
        ).fetchone()
        return int(row["count"]) if row else 0

    def count_scheduled_on_date(self, day: date) -> int:
        prefix = day.isoformat()
        row = self.connection.execute(
            """
            SELECT COUNT(*) AS count
            FROM assets
            WHERE scheduled_for LIKE ? AND posted_at = ''
            """,
            (f"{prefix}%",),
        ).fetchone()
        return int(row["count"]) if row else 0

    def log_event(
        self,
        level: str,
        event_type: str,
        message: str,
        *,
        asset_id: Optional[int] = None,
        details: Optional[dict[str, Any]] = None,
    ) -> None:
        payload = json.dumps(details or {})
        with self.connection:
            self.connection.execute(
                """
                INSERT INTO logs (level, event_type, message, asset_id, details_json, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (level, event_type, message, asset_id, payload, utc_now_iso()),
            )
        log_method = getattr(LOGGER, level.lower(), LOGGER.info)
        log_method("%s: %s", event_type, message)

    def export_review_rows(self) -> list[dict[str, Any]]:
        rows = self.connection.execute(
            """
            SELECT * FROM assets
            ORDER BY
                CASE rights_status
                    WHEN 'unknown' THEN 0
                    WHEN 'rejected' THEN 1
                    ELSE 2
                END,
                updated_at DESC
            """
        ).fetchall()
        return [self._row_to_export_dict(row) for row in rows]

    def _row_to_asset(self, row: sqlite3.Row) -> AssetRecord:
        return AssetRecord(
            asset_id=row["id"],
            source_name=row["source_name"],
            source_asset_id=row["source_asset_id"],
            source_url=row["source_url"],
            title=row["title"],
            creator=row["creator"],
            license_type=row["license_type"],
            license_url=row["license_url"],
            duration_seconds=row["duration_seconds"],
            tags=json.loads(row["tags_json"] or "[]"),
            thumbnail_url=row["thumbnail_url"],
            media_url=row["media_url"],
            local_path=row["local_path"],
            processed_path=row["processed_path"],
            attribution_required=bool(row["attribution_required"]),
            attribution_text=row["attribution_text"],
            rights_status=row["rights_status"],
            rights_reason=row["rights_reason"],
            proposed_caption=row["proposed_caption"],
            proposed_hashtags=row["proposed_hashtags"],
            source_attribution=row["source_attribution"],
            manual_approved=bool(row["manual_approved"]),
            review_notes=row["review_notes"],
            scheduled_for=row["scheduled_for"],
            media_hash=row["media_hash"],
            posted_at=row["posted_at"],
            subtitle_file=row["subtitle_file"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    def _row_to_export_dict(self, row: sqlite3.Row) -> dict[str, Any]:
        data = dict(row)
        data["tags"] = ", ".join(json.loads(row["tags_json"] or "[]"))
        data["manual_approved"] = bool(row["manual_approved"])
        data["attribution_required"] = bool(row["attribution_required"])
        data.pop("tags_json", None)
        return data


def write_csv_report(rows: Iterable[dict[str, Any]], output_path: str | Path) -> Path:
    import csv

    destination = Path(output_path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    rows = list(rows)
    headers = sorted({key for row in rows for key in row.keys()}) if rows else []
    with destination.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    return destination


def write_html_report(rows: Iterable[dict[str, Any]], output_path: str | Path) -> Path:
    import html

    destination = Path(output_path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    rows = list(rows)
    cards: list[str] = []
    for row in rows:
        thumb = row.get("thumbnail_url") or ""
        thumb_html = (
            f'<img src="{html.escape(thumb)}" alt="thumbnail" class="thumb" />'
            if thumb
            else '<div class="thumb placeholder">No preview</div>'
        )
        cards.append(
            f"""
            <article class="card">
                {thumb_html}
                <div class="meta">
                    <h2>{html.escape(str(row.get("title", "")))}</h2>
                    <p><strong>Source:</strong> {html.escape(str(row.get("source_name", "")))} |
                    <a href="{html.escape(str(row.get("source_url", "")))}">Open</a></p>
                    <p><strong>Creator:</strong> {html.escape(str(row.get("creator", "")))}</p>
                    <p><strong>License:</strong> {html.escape(str(row.get("license_type", "")))} |
                    <a href="{html.escape(str(row.get("license_url", "")))}">License</a></p>
                    <p><strong>Rights Status:</strong> {html.escape(str(row.get("rights_status", "")))}</p>
                    <p><strong>Caption:</strong> {html.escape(str(row.get("proposed_caption", "")))}</p>
                    <p><strong>Hashtags:</strong> {html.escape(str(row.get("proposed_hashtags", "")))}</p>
                    <p><strong>Attribution:</strong> {html.escape(str(row.get("source_attribution", "")))}</p>
                    <p><strong>Approved:</strong> {html.escape(str(row.get("manual_approved", "")))}</p>
                    <p><strong>Scheduled:</strong> {html.escape(str(row.get("scheduled_for", "")))}</p>
                    <p><strong>Notes:</strong> {html.escape(str(row.get("review_notes", "")))}</p>
                </div>
            </article>
            """
        )
    html_text = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="utf-8" />
        <title>TikTok Review Dashboard</title>
        <style>
            body {{
                font-family: Arial, sans-serif;
                margin: 2rem;
                background: #f6f6f6;
                color: #111;
            }}
            .card {{
                display: grid;
                grid-template-columns: 260px 1fr;
                gap: 1rem;
                background: #fff;
                border-radius: 12px;
                padding: 1rem;
                margin-bottom: 1rem;
                box-shadow: 0 2px 8px rgba(0, 0, 0, 0.08);
            }}
            .thumb {{
                width: 100%;
                aspect-ratio: 9 / 16;
                object-fit: cover;
                border-radius: 8px;
                background: #ddd;
            }}
            .placeholder {{
                display: flex;
                align-items: center;
                justify-content: center;
            }}
            .meta h2 {{
                margin-top: 0;
            }}
        </style>
    </head>
    <body>
        <h1>TikTok Review Dashboard</h1>
        <p>Manual approval is required before posting. Unknown or rejected rights statuses must stay blocked.</p>
        {''.join(cards) if cards else '<p>No assets found yet.</p>'}
    </body>
    </html>
    """
    destination.write_text(html_text, encoding="utf-8")
    return destination
