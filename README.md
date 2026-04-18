# Rights-Safe TikTok Video Pipeline

This project discovers, reviews, prepares, and schedules TikTok-ready videos while enforcing a strict rights policy.

It only works with sources that are explicitly allowed for reuse:

- Public-domain libraries
- Creative Commons sources that allow commercial reuse and derivatives
- Licensed stock media providers
- Your own local media folder
- Your own AI-generated videos folder

It does not scrape random creator content, does not download from TikTok, YouTube, Instagram, or similar platforms unless you explicitly own the content and wire that source in yourself, and it will not post anything with unclear rights.

## What It Does

- Searches approved legal content sources only
- Saves source metadata to SQLite
- Classifies rights status before review
- Rejects unknown or disallowed licenses automatically
- Generates CSV and HTML review dashboards
- Requires manual approval before scheduling or posting
- Prepares vertical TikTok-friendly video outputs with optional subtitle overlay
- Enforces daily posting limits
- Prevents duplicates by source ID and media hash
- Provides a mock uploader and an official TikTok integration placeholder

## Project Files

- [main.py](C:\Users\user\Documents\Codex\2026-04-18-are-u-plus-gpt-model\main.py)
- [search_sources.py](C:\Users\user\Documents\Codex\2026-04-18-are-u-plus-gpt-model\search_sources.py)
- [rights_validator.py](C:\Users\user\Documents\Codex\2026-04-18-are-u-plus-gpt-model\rights_validator.py)
- [media_processor.py](C:\Users\user\Documents\Codex\2026-04-18-are-u-plus-gpt-model\media_processor.py)
- [scheduler.py](C:\Users\user\Documents\Codex\2026-04-18-are-u-plus-gpt-model\scheduler.py)
- [uploader.py](C:\Users\user\Documents\Codex\2026-04-18-are-u-plus-gpt-model\uploader.py)
- [storage.py](C:\Users\user\Documents\Codex\2026-04-18-are-u-plus-gpt-model\storage.py)
- [config.yaml](C:\Users\user\Documents\Codex\2026-04-18-are-u-plus-gpt-model\config.yaml)

## Rights Statuses

Each asset is classified into one of these statuses:

- `public_domain`
- `cc_commercial`
- `licensed_stock`
- `owned`
- `ai_owned`
- `rejected`
- `unknown`

Only these statuses are allowed to move toward upload:

- `public_domain`
- `cc_commercial`
- `licensed_stock`
- `owned`
- `ai_owned`

Anything marked `unknown` or `rejected` is blocked automatically.

## Approval Flow

1. Run a legal-source search.
2. Generate the review dashboard.
3. Inspect the source URL, thumbnail, license, attribution, and caption.
4. Manually approve approved-rights items only.
5. Schedule approved items.
6. Run the scheduler in dry-run mode first.
7. Enable a real uploader only after official platform integration is implemented.

Manual approval is required before any post attempt. Rights verification is required before manual approval is even allowed.

## Setup

Requirements:

- Python 3.11+ recommended
- `ffmpeg` installed and available on `PATH` if you want to process video
- `PyYAML` installed: `pip install pyyaml`
- Optional API keys for licensed providers:
  - `PEXELS_API_KEY`
  - `PIXABAY_API_KEY`
- Optional future official TikTok credentials:
  - `TIKTOK_CLIENT_ID`
  - `TIKTOK_CLIENT_SECRET`

Create local folders if you want to use owned media:

- `owned_media`
- `ai_generated_videos`

## Allowed Sources

Configured sources in `config.yaml`:

- `wikimedia_commons`
  - Intended for public-domain and Creative Commons media
  - Rights still validated per asset
- `internet_archive`
  - Queried with a public-domain / commercial-reuse-oriented license filter
  - Rights still validated per asset
- `pexels`
  - Licensed stock provider
  - Disabled by default until you add `PEXELS_API_KEY`
- `pixabay`
  - Licensed stock provider
  - Disabled by default until you add `PIXABAY_API_KEY`
- `local_owned`
  - Your own generated or recorded media folder
- `local_ai`
  - Your own AI-generated video folder

If a source does not have explicit reuse terms, do not add it.

## How To Run

Search approved sources:

```bash
python main.py search --query "city skyline" --limit 5
```

Generate review reports:

```bash
python main.py review-report
```

List discovered items:

```bash
python main.py list-assets
```

Approve an item:

```bash
python main.py approve --asset-id 12 --notes "Verified rights and brand fit."
```

Reject an item:

```bash
python main.py reject --asset-id 12 --reason "License terms unclear."
```

Prepare one asset with optional overlay text:

```bash
python main.py prepare --asset-id 12 --subtitle-file .\subtitle.txt
```

Auto-schedule approved items:

```bash
python main.py schedule-approved --days-ahead 7
```

Run posting in dry-run mode:

```bash
python main.py run-scheduler --dry-run
```

## Review Dashboard

The HTML and CSV reports include:

- Preview thumbnail when available
- Source link
- Creator
- License type and license URL
- Rights status
- Proposed caption
- Proposed hashtags
- Attribution text
- Approval status
- Schedule status

This gives you a lightweight local dashboard for manual review before posting.

## Content Preparation

The media processor:

- Downloads media only from configured approved sources
- Resizes to a TikTok-friendly vertical format
- Trims to the configured maximum duration
- Optionally burns subtitle text from a provided text file
- Stores processed output paths and hashes

If a license requires attribution, the attribution text is stored on the asset and can be passed to the uploader layer.

## Scheduler And Posting Rules

The scheduler:

- Reads approved items from SQLite
- Uses configured posting times and a daily posting limit
- Refuses to post assets without approved rights status
- Refuses to post assets without manual approval
- Supports `--dry-run`
- Skips duplicates by source ID and media hash

The uploader layer includes:

- `MockUploader` for testing
- `TikTokOfficialUploader` placeholder only

There is intentionally no browser automation to bypass platform safeguards.

## Adding Licensed Providers

To add another licensed stock provider safely:

1. Add a new source entry in `config.yaml`.
2. Implement a provider in `search_sources.py`.
3. Return explicit license metadata for each asset.
4. Mark the provider category as `licensed_stock`.
5. Keep the provider disabled until credentials and contract terms are confirmed.
6. Verify that downloading and reuse are allowed by the provider agreement.

Do not add sources with ambiguous or user-uploaded rights.

## Important Policy Constraint

Unverified, copyrighted, or unclear-rights content must not be posted.

This tool is designed to stop that workflow early by:

- auto-rejecting incompatible license terms
- blocking `unknown` rights statuses
- requiring manual approval
- keeping official posting integration separate from discovery and review
