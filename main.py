from __future__ import annotations

import argparse
import json
import logging
import os
import re
from pathlib import Path
from typing import Any

try:
    import yaml
except ImportError as exc:  # pragma: no cover - dependency failure path
    raise RuntimeError("PyYAML is required. Install it with `pip install pyyaml`.") from exc

from media_processor import MediaProcessor
from rights_validator import RightsValidator
from scheduler import Scheduler
from search_sources import discover_assets
from storage import AssetRecord, Storage, write_csv_report, write_html_report
from uploader import UploadRequest, build_uploader
from kids_story_generator import KidsStoryGenerator


LOGGER = logging.getLogger(__name__)


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    config = load_config(args.config)
    configure_logging(config)

    storage = Storage(config["database"]["path"])
    try:
        validator = RightsValidator(config)
        media_processor = MediaProcessor(config, storage)
        return args.handler(args, config, storage, validator, media_processor)
    finally:
        storage.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Rights-safe TikTok content pipeline.")
    parser.add_argument(
        "--config",
        default="config.yaml",
        help="Path to the YAML config file.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    search_parser = subparsers.add_parser("search", help="Search approved sources and store metadata.")
    search_parser.add_argument("--query", required=True, help="Search phrase.")
    search_parser.add_argument("--limit", type=int, default=5, help="Per-source result limit.")
    search_parser.set_defaults(handler=handle_search)

    report_parser = subparsers.add_parser("review-report", help="Build CSV and HTML review reports.")
    report_parser.add_argument("--html", default="", help="Optional HTML report path override.")
    report_parser.add_argument("--csv", default="", help="Optional CSV report path override.")
    report_parser.set_defaults(handler=handle_review_report)

    list_parser = subparsers.add_parser("list-assets", help="Print discovered assets.")
    list_parser.add_argument("--rights-status", default="", help="Filter by rights status.")
    list_parser.set_defaults(handler=handle_list_assets)

    approve_parser = subparsers.add_parser("approve", help="Manually approve an asset for scheduling.")
    approve_parser.add_argument("--asset-id", required=True, type=int)
    approve_parser.add_argument("--notes", default="", help="Optional reviewer notes.")
    approve_parser.set_defaults(handler=handle_approve)

    reject_parser = subparsers.add_parser("reject", help="Reject an asset during review.")
    reject_parser.add_argument("--asset-id", required=True, type=int)
    reject_parser.add_argument("--reason", required=True, help="Reason for rejection.")
    reject_parser.set_defaults(handler=handle_reject)

    prepare_parser = subparsers.add_parser("prepare", help="Download/process a single asset.")
    prepare_parser.add_argument("--asset-id", required=True, type=int)
    prepare_parser.add_argument("--subtitle-file", default="", help="Optional subtitle text file.")
    prepare_parser.add_argument("--dry-run", action="store_true")
    prepare_parser.set_defaults(handler=handle_prepare)

    schedule_parser = subparsers.add_parser("schedule-approved", help="Assign future schedule slots to approved assets.")
    schedule_parser.add_argument("--days-ahead", type=int, default=14)
    schedule_parser.set_defaults(handler=handle_schedule)

    run_parser = subparsers.add_parser("run-scheduler", help="Post due scheduled assets.")
    run_parser.add_argument("--dry-run", action="store_true")
    run_parser.set_defaults(handler=handle_run_scheduler)

    story_parser = subparsers.add_parser("generate-kids-story", help="Create a short vertical kids story video package.")
    story_parser.add_argument("--title", default="Luna's Little Adventure")
    story_parser.add_argument("--theme", default="friendship")
    story_parser.add_argument("--character", default="Luna the little fox")
    story_parser.add_argument("--output-dir", default="./data/kids_story")
    story_parser.add_argument("--scene-dir", default="", help="Folder containing scene_01.png ... scene_04.png")
    story_parser.add_argument("--voice", default="", help="Optional Windows voice name for narration.")
    story_parser.add_argument("--rate", type=int, default=0, help="Windows narrator rate from about -10 to 10.")
    story_parser.add_argument("--create-placeholders", action="store_true", help="Create simple fallback images if AI scene art is not ready yet.")
    story_parser.add_argument("--auto-post", action="store_true", help="Post the generated story to configured platforms using official APIs.")
    story_parser.add_argument("--dry-run-post", action="store_true", help="Simulate posting without calling platform APIs.")
    story_parser.add_argument("--description", default="", help="Optional override for the social description.")
    story_parser.add_argument("--hashtags", default="", help="Optional hashtags string like '#kidsstory #shorts'.")
    story_parser.set_defaults(handler=handle_generate_kids_story)

    return parser


def handle_search(
    args: argparse.Namespace,
    config: dict[str, Any],
    storage: Storage,
    validator: RightsValidator,
    media_processor: MediaProcessor,
) -> int:
    del media_processor
    assets = discover_assets(
        config=config,
        storage=storage,
        validator=validator,
        query=args.query,
        per_source_limit=args.limit,
    )
    print(f"Stored {len(assets)} assets for query '{args.query}'.")
    return 0


def handle_review_report(
    args: argparse.Namespace,
    config: dict[str, Any],
    storage: Storage,
    validator: RightsValidator,
    media_processor: MediaProcessor,
) -> int:
    del validator, media_processor
    rows = filter_excluded_sources(storage.export_review_rows(), config)
    html_path = args.html or config["reports"]["html_path"]
    csv_path = args.csv or config["reports"]["csv_path"]
    html_output = write_html_report(rows, html_path)
    csv_output = write_csv_report(rows, csv_path)
    print(f"HTML report: {html_output}")
    print(f"CSV report: {csv_output}")
    return 0


def handle_list_assets(
    args: argparse.Namespace,
    config: dict[str, Any],
    storage: Storage,
    validator: RightsValidator,
    media_processor: MediaProcessor,
) -> int:
    del validator, media_processor
    assets = storage.list_assets(rights_status=args.rights_status or None)
    excluded_sources = excluded_source_names(config)
    for asset in assets:
        if asset.source_name.lower() in excluded_sources:
            continue
        print(
            f"[{asset.asset_id}] {asset.title} | {asset.rights_status} | "
            f"approved={asset.manual_approved} | scheduled={asset.scheduled_for or '-'}"
        )
    return 0


def handle_approve(
    args: argparse.Namespace,
    config: dict[str, Any],
    storage: Storage,
    validator: RightsValidator,
    media_processor: MediaProcessor,
) -> int:
    del config, validator, media_processor
    asset = require_asset(storage, args.asset_id)
    if not asset.has_allowed_rights:
        raise RuntimeError(
            f"Asset {asset.asset_id} cannot be approved because rights status is {asset.rights_status!r}."
        )
    storage.approve_asset(asset.asset_id or 0, review_notes=args.notes)
    storage.log_event("info", "approved", f"Asset approved: {asset.title}", asset_id=asset.asset_id)
    print(f"Approved asset {asset.asset_id}.")
    return 0


def handle_reject(
    args: argparse.Namespace,
    config: dict[str, Any],
    storage: Storage,
    validator: RightsValidator,
    media_processor: MediaProcessor,
) -> int:
    del config, validator, media_processor
    asset = require_asset(storage, args.asset_id)
    storage.reject_asset(asset.asset_id or 0, args.reason)
    storage.log_event("info", "rejected", f"Asset rejected: {asset.title}", asset_id=asset.asset_id)
    print(f"Rejected asset {asset.asset_id}.")
    return 0


def handle_prepare(
    args: argparse.Namespace,
    config: dict[str, Any],
    storage: Storage,
    validator: RightsValidator,
    media_processor: MediaProcessor,
) -> int:
    del config, validator
    asset = require_asset(storage, args.asset_id)
    if not asset.has_allowed_rights:
        raise RuntimeError(
            f"Asset {asset.asset_id} cannot be prepared because rights status is {asset.rights_status!r}."
        )
    if args.subtitle_file:
        storage.update_asset(asset.asset_id or 0, subtitle_file=args.subtitle_file)
    output = media_processor.prepare_asset(asset, subtitle_file=args.subtitle_file, dry_run=args.dry_run)
    print(f"Prepared asset output: {output}")
    return 0


def handle_schedule(
    args: argparse.Namespace,
    config: dict[str, Any],
    storage: Storage,
    validator: RightsValidator,
    media_processor: MediaProcessor,
) -> int:
    del validator
    uploader = build_uploader(config)
    scheduler = Scheduler(config, storage, media_processor, uploader)
    result = scheduler.auto_schedule(days_ahead=args.days_ahead)
    print(f"Scheduled {result.scheduled_count} assets. Skipped {result.skipped_count}.")
    return 0


def handle_run_scheduler(
    args: argparse.Namespace,
    config: dict[str, Any],
    storage: Storage,
    validator: RightsValidator,
    media_processor: MediaProcessor,
) -> int:
    del validator
    uploader = build_uploader(config)
    scheduler = Scheduler(config, storage, media_processor, uploader)
    result = scheduler.run_due_posts(dry_run=args.dry_run)
    print(
        f"Scheduler complete. posted={result.posted_count} "
        f"scheduled={result.scheduled_count} skipped={result.skipped_count}"
    )
    return 0


def handle_generate_kids_story(
    args: argparse.Namespace,
    config: dict[str, Any],
    storage: Storage,
    validator: RightsValidator,
    media_processor: MediaProcessor,
) -> int:
    del storage, validator, media_processor
    generator = KidsStoryGenerator(config)
    output_dir = Path(args.output_dir)
    story = generator.build_story(
        title=args.title,
        theme=args.theme,
        character_name=args.character,
    )
    package_files = generator.write_story_package(
        story,
        output_dir=output_dir,
        scene_dir=args.scene_dir or None,
        create_placeholder_images=args.create_placeholders,
    )
    audio_path = output_dir / "narration.wav"
    video_path = output_dir / "kids_story_short.mp4"
    generator.synthesize_narration(
        story,
        output_audio_path=audio_path,
        voice_name=args.voice,
        rate=args.rate,
    )
    generator.render_story_video(
        story,
        scene_dir=package_files["scene_dir"],
        audio_path=audio_path,
        subtitle_path=package_files["subtitles_srt"],
        output_video_path=video_path,
    )
    metadata = build_story_publish_metadata(story, config, description_override=args.description, hashtags_override=args.hashtags)
    metadata_path = output_dir / "publish_metadata.json"
    metadata_path.write_text(json.dumps(metadata, indent=2), encoding="utf-8")
    if args.auto_post:
        synthetic_asset = AssetRecord(
            source_name="local_ai",
            source_asset_id=f"kids-story-{slugify(story.title)}",
            source_url=video_path.resolve().as_uri(),
            title=story.title,
            creator="AI story generator",
            license_type="AI-generated owned media",
            rights_status="ai_owned",
            proposed_caption=metadata["title"],
            proposed_hashtags=metadata["hashtags"],
            source_attribution="",
            local_path=str(video_path.resolve()),
            processed_path=str(video_path.resolve()),
            manual_approved=True,
        )
        uploader = build_uploader(config, override_name="multi_platform")
        upload_result = uploader.upload(
            UploadRequest(
                asset=synthetic_asset,
                video_path=video_path,
                title=metadata["title"],
                caption=metadata["title"],
                description=metadata["description"],
                hashtags=metadata["hashtags"],
                attribution="",
                dry_run=args.dry_run_post,
                consent_confirmed=True,
            )
        )
        print(f"Upload result: {upload_result.external_post_id or upload_result.error_message}")
    print(f"Story package: {package_files['story_json']}")
    print(f"Prompts: {package_files['prompts_txt']}")
    print(f"Narration: {audio_path}")
    print(f"Video: {video_path}")
    print(f"Publish metadata: {metadata_path}")
    return 0


def require_asset(storage: Storage, asset_id: int):
    asset = storage.get_asset(asset_id)
    if asset is None:
        raise RuntimeError(f"Asset {asset_id} was not found.")
    return asset


def load_config(path: str) -> dict[str, Any]:
    config_path = Path(path)
    raw_config = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    if not isinstance(raw_config, dict):
        raise RuntimeError("Config file must deserialize to a mapping.")
    resolved = resolve_env_vars(raw_config)
    return absolutize_paths(resolved, config_path.parent)


def resolve_env_vars(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: resolve_env_vars(item) for key, item in value.items()}
    if isinstance(value, list):
        return [resolve_env_vars(item) for item in value]
    if isinstance(value, str):
        return re.sub(
            r"\$\{([A-Z0-9_]+)\}",
            lambda match: os.environ.get(match.group(1), ""),
            value,
        )
    return value


def absolutize_paths(config: dict[str, Any], base_dir: Path) -> dict[str, Any]:
    path_sections = ["database", "reports", "paths", "kids_story"]
    for section in path_sections:
        if section not in config:
            continue
        for key, value in list(config[section].items()):
            if isinstance(value, str) and value and not value.startswith("http"):
                config[section][key] = str((base_dir / value).resolve())
    for key in ("local_owned", "local_ai"):
        settings = config.get("sources", {}).get(key, {})
        folder = settings.get("folder")
        if isinstance(folder, str) and folder:
            settings["folder"] = str((base_dir / folder).resolve())
    youtube_settings = config.get("social_posting", {}).get("youtube", {})
    for key in ("client_secrets_file", "token_file"):
        value = youtube_settings.get(key)
        if isinstance(value, str) and value and not value.startswith("http"):
            youtube_settings[key] = str((base_dir / value).resolve())
    return config


def configure_logging(config: dict[str, Any]) -> None:
    log_path = Path(config["paths"]["log_path"])
    log_path.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        handlers=[
            logging.FileHandler(log_path, encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )


def build_story_publish_metadata(
    story,
    config: dict[str, Any],
    *,
    description_override: str = "",
    hashtags_override: str = "",
) -> dict[str, str]:
    hashtag_values = hashtags_override.strip() or " ".join(
        f"#{tag.lstrip('#')}" for tag in config.get("social_posting", {}).get("default_hashtags", [])
    )
    summary = f"{story.theme.title()} story for kids. Moral: {story.moral}"
    description_template = config.get("social_posting", {}).get(
        "description_template",
        "{title}\n\n{summary}\n\n{hashtags}",
    )
    description = description_override.strip() or description_template.format(
        title=story.title,
        summary=summary,
        hashtags=hashtag_values,
    )
    return {
        "title": story.title,
        "description": description,
        "hashtags": hashtag_values,
    }


def slugify(value: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9]+", "-", value.strip().lower()).strip("-")
    return cleaned or "story"


def excluded_source_names(config: dict[str, Any]) -> set[str]:
    return {
        value.lower()
        for value in config.get("content_filters", {}).get("excluded_sources", [])
    }


def filter_excluded_sources(rows: list[dict[str, Any]], config: dict[str, Any]) -> list[dict[str, Any]]:
    excluded_sources = excluded_source_names(config)
    return [
        row
        for row in rows
        if str(row.get("source_name", "")).lower() not in excluded_sources
    ]


if __name__ == "__main__":
    raise SystemExit(main())
