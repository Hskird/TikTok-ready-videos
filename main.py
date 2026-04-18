from __future__ import annotations

import argparse
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
from storage import Storage, write_csv_report, write_html_report
from uploader import build_uploader


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
        uploader = build_uploader(config)
        scheduler = Scheduler(config, storage, media_processor, uploader)
        return args.handler(args, config, storage, validator, media_processor, scheduler)
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

    return parser


def handle_search(
    args: argparse.Namespace,
    config: dict[str, Any],
    storage: Storage,
    validator: RightsValidator,
    media_processor: MediaProcessor,
    scheduler: Scheduler,
) -> int:
    del media_processor, scheduler
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
    scheduler: Scheduler,
) -> int:
    del validator, media_processor, scheduler
    rows = storage.export_review_rows()
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
    scheduler: Scheduler,
) -> int:
    del config, validator, media_processor, scheduler
    assets = storage.list_assets(rights_status=args.rights_status or None)
    for asset in assets:
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
    scheduler: Scheduler,
) -> int:
    del config, validator, media_processor, scheduler
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
    scheduler: Scheduler,
) -> int:
    del config, validator, media_processor, scheduler
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
    scheduler: Scheduler,
) -> int:
    del config, validator, scheduler
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
    scheduler: Scheduler,
) -> int:
    del config, validator, media_processor
    result = scheduler.auto_schedule(days_ahead=args.days_ahead)
    print(f"Scheduled {result.scheduled_count} assets. Skipped {result.skipped_count}.")
    return 0


def handle_run_scheduler(
    args: argparse.Namespace,
    config: dict[str, Any],
    storage: Storage,
    validator: RightsValidator,
    media_processor: MediaProcessor,
    scheduler: Scheduler,
) -> int:
    del config, validator, media_processor
    result = scheduler.run_due_posts(dry_run=args.dry_run)
    print(
        f"Scheduler complete. posted={result.posted_count} "
        f"scheduled={result.scheduled_count} skipped={result.skipped_count}"
    )
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
    path_sections = ["database", "reports", "paths"]
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


if __name__ == "__main__":
    raise SystemExit(main())
