from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from media_processor import MediaProcessor
from rights_validator import ALLOWED_UPLOAD_STATUSES
from storage import Storage
from uploader import UploadRequest, Uploader


LOGGER = logging.getLogger(__name__)


@dataclass
class SchedulerResult:
    scheduled_count: int = 0
    posted_count: int = 0
    skipped_count: int = 0


class Scheduler:
    def __init__(
        self,
        config: dict[str, Any],
        storage: Storage,
        media_processor: MediaProcessor,
        uploader: Uploader,
    ) -> None:
        self.config = config
        self.storage = storage
        self.media_processor = media_processor
        self.uploader = uploader
        timezone_name = config["schedule"].get("timezone", "UTC")
        try:
            self.timezone = ZoneInfo(timezone_name)
        except ZoneInfoNotFoundError:
            LOGGER.warning(
                "Timezone %s was not available in this Python runtime; falling back to UTC for scheduling.",
                timezone_name,
            )
            self.timezone = timezone.utc

    def auto_schedule(self, *, days_ahead: int = 14) -> SchedulerResult:
        result = SchedulerResult()
        assets = self.storage.list_unscheduled_approved_assets()
        slots = self._future_slots(days_ahead=days_ahead)
        for asset in assets:
            slot = self._next_open_slot(slots)
            if slot is None:
                self.storage.log_event(
                    "warning",
                    "schedule_full",
                    "No schedule slot available within the configured horizon.",
                    asset_id=asset.asset_id,
                )
                result.skipped_count += 1
                continue
            slot["used"] = True
            self.storage.schedule_asset(asset.asset_id or 0, slot["dt"].isoformat())
            self.storage.log_event(
                "info",
                "scheduled",
                f"Scheduled asset {asset.title} for {slot['dt'].isoformat()}",
                asset_id=asset.asset_id,
            )
            result.scheduled_count += 1
        return result

    def run_due_posts(self, *, dry_run: bool = False, now: datetime | None = None) -> SchedulerResult:
        result = SchedulerResult()
        if now is None:
            current_time = datetime.now(self.timezone)
        elif now.tzinfo is None:
            current_time = now.replace(tzinfo=self.timezone)
        else:
            current_time = now.astimezone(self.timezone)
        daily_limit = int(self.config["schedule"].get("daily_post_limit", 1))
        already_posted_today = self.storage.count_posts_on_date(current_time.date())
        due_assets = self.storage.list_due_assets(current_time.isoformat())
        for asset in due_assets:
            if already_posted_today >= daily_limit:
                self.storage.log_event(
                    "info",
                    "daily_limit_reached",
                    f"Daily posting limit of {daily_limit} reached.",
                    asset_id=asset.asset_id,
                )
                break

            if asset.rights_status not in ALLOWED_UPLOAD_STATUSES:
                self.storage.log_event(
                    "info",
                    "post_blocked",
                    "Asset skipped because rights status is not approved for upload.",
                    asset_id=asset.asset_id,
                    details={"rights_status": asset.rights_status},
                )
                result.skipped_count += 1
                continue

            if not asset.manual_approved:
                self.storage.log_event(
                    "info",
                    "post_blocked",
                    "Asset skipped because manual approval is missing.",
                    asset_id=asset.asset_id,
                )
                result.skipped_count += 1
                continue

            if self.storage.has_posted_source(asset.source_name, asset.source_asset_id):
                self.storage.log_event(
                    "info",
                    "duplicate_source",
                    "Asset skipped because the same source item was already posted.",
                    asset_id=asset.asset_id,
                )
                result.skipped_count += 1
                continue

            try:
                prepared_path = self.media_processor.prepare_asset(
                    asset,
                    subtitle_file=asset.subtitle_file,
                    dry_run=dry_run,
                )
                media_hash = asset.media_hash or (
                    self.media_processor.compute_sha256(prepared_path)
                    if prepared_path.exists()
                    else ""
                )
                if media_hash and self.storage.has_posted_hash(media_hash):
                    self.storage.log_event(
                        "info",
                        "duplicate_hash",
                        "Asset skipped because an identical media hash was already posted.",
                        asset_id=asset.asset_id,
                    )
                    result.skipped_count += 1
                    continue

                request = UploadRequest(
                    asset=asset,
                    video_path=prepared_path,
                    caption=asset.proposed_caption,
                    hashtags=asset.proposed_hashtags,
                    attribution=asset.source_attribution,
                    dry_run=dry_run,
                )
                upload_result = self.uploader.upload(request)
                status = "posted" if upload_result.success and not dry_run else "dry_run"
                if not upload_result.success:
                    status = "failed"

                self.storage.record_post_attempt(
                    asset_id=asset.asset_id or 0,
                    uploader_name=upload_result.uploader_name,
                    status=status,
                    external_post_id=upload_result.external_post_id,
                    dry_run=dry_run,
                    media_hash=media_hash,
                    error_message=upload_result.error_message,
                )

                if upload_result.success and not dry_run:
                    self.storage.mark_posted(
                        asset.asset_id or 0,
                        posted_at=current_time.isoformat(),
                        media_hash=media_hash,
                        external_post_id=upload_result.external_post_id,
                    )
                    already_posted_today += 1
                    result.posted_count += 1
                elif dry_run:
                    self.storage.log_event(
                        "info",
                        "dry_run_post",
                        f"Dry-run upload simulated for asset {asset.title}",
                        asset_id=asset.asset_id,
                    )
                    result.posted_count += 1
                else:
                    self.storage.log_event(
                        "error",
                        "upload_failed",
                        upload_result.error_message or "Uploader returned failure.",
                        asset_id=asset.asset_id,
                    )
                    result.skipped_count += 1
            except Exception as exc:
                self.storage.record_post_attempt(
                    asset_id=asset.asset_id or 0,
                    uploader_name=self.uploader.name,
                    status="failed",
                    external_post_id="",
                    dry_run=dry_run,
                    media_hash=asset.media_hash,
                    error_message=str(exc),
                )
                self.storage.log_event(
                    "error",
                    "post_exception",
                    f"Posting failed: {exc}",
                    asset_id=asset.asset_id,
                )
                result.skipped_count += 1
        return result

    def _future_slots(self, *, days_ahead: int) -> list[dict[str, Any]]:
        posting_times = self.config["schedule"].get("posting_times", ["09:00"])
        daily_limit = int(self.config["schedule"].get("daily_post_limit", 1))
        now = datetime.now(self.timezone)
        slots: list[dict[str, Any]] = []
        for offset in range(days_ahead + 1):
            day = (now + timedelta(days=offset)).date()
            existing_count = self.storage.count_scheduled_on_date(day) + self.storage.count_posts_on_date(day)
            remaining = max(0, daily_limit - existing_count)
            if remaining == 0:
                continue
            day_slots: list[dict[str, Any]] = []
            for posting_time in posting_times[:daily_limit]:
                scheduled_dt = datetime.combine(day, parse_time(posting_time), tzinfo=self.timezone)
                if scheduled_dt <= now:
                    continue
                day_slots.append({"dt": scheduled_dt, "used": False})
            slots.extend(day_slots[:remaining])
        return sorted(slots, key=lambda item: item["dt"])

    @staticmethod
    def _next_open_slot(slots: list[dict[str, Any]]) -> dict[str, Any] | None:
        for slot in slots:
            if not slot["used"]:
                return slot
        return None


def parse_time(value: str) -> time:
    hour, minute = value.split(":")
    return time(hour=int(hour), minute=int(minute))
