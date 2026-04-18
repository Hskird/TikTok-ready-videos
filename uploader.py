from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from storage import AssetRecord


@dataclass
class UploadRequest:
    asset: AssetRecord
    video_path: Path
    caption: str
    hashtags: str
    attribution: str
    dry_run: bool = False


@dataclass
class UploadResult:
    success: bool
    uploader_name: str
    external_post_id: str = ""
    error_message: str = ""


class Uploader(ABC):
    name: str

    @abstractmethod
    def upload(self, request: UploadRequest) -> UploadResult:
        raise NotImplementedError


class MockUploader(Uploader):
    name = "mock"

    def upload(self, request: UploadRequest) -> UploadResult:
        label = "dryrun" if request.dry_run else "mock"
        return UploadResult(
            success=True,
            uploader_name=self.name,
            external_post_id=f"{label}-{request.asset.asset_id or request.asset.source_asset_id}",
        )


class TikTokOfficialUploader(Uploader):
    name = "tiktok_official"

    def __init__(self, config: dict[str, Any]) -> None:
        self.config = config

    def upload(self, request: UploadRequest) -> UploadResult:
        return UploadResult(
            success=False,
            uploader_name=self.name,
            error_message=(
                "Official TikTok integration is a placeholder only. "
                "Implement an approved official API client before enabling uploads."
            ),
        )


def build_uploader(config: dict[str, Any]) -> Uploader:
    uploader_name = config.get("uploader", {}).get("name", "mock")
    if uploader_name == "mock":
        return MockUploader()
    if uploader_name == "tiktok_official":
        return TikTokOfficialUploader(config)
    raise ValueError(f"Unsupported uploader: {uploader_name}")
