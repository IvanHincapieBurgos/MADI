"""Bronze layer
Flow
----
1. Load centralized config (+ secrets from .env).
2. Trigger the ``knowten/finca-raiz-scraper`` actor with the configured
   ``run_input`` and wait for it to finish.
3. Validate the returned payload (non-empty list of objects).
4. Persist the raw JSON to ``data/raw/``:
     * a timestamped file  -> immutable history
     * ``finca_raiz_latest.json`` -> stable handoff for the Transform stage.
"""
from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

# --- Make `src`/`etl` importable when run as a plain file -------------
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from src.utils.apify_extractor import ApifyExtractor, ApifyExtractionError  # noqa: E402
from src.utils.config_loader import load_config, resolve_path  # noqa: E402
from src.utils.logger import get_logger  # noqa: E402
from src.validation.raw_schema import validate_raw_items  # noqa: E402

LATEST_FILENAME = "finca_raiz_latest.json"


def _timestamp() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _save_json(items: list[dict[str, Any]], raw_dir: Path, logger) -> Path:
    '''
    Write items to a timestamped file and update the 'latest' pointer.
    '''
    raw_dir.mkdir(parents=True, exist_ok=True)

    timestamped = raw_dir / f"finca_raiz_{_timestamp()}.json"
    payload = json.dumps(items, ensure_ascii=False, indent=2)
    timestamped.write_text(payload, encoding="utf-8")

    latest = raw_dir / LATEST_FILENAME
    latest.write_text(payload, encoding="utf-8")

    logger.info("Saved %d records -> %s", len(items), timestamped)
    logger.info("Updated latest pointer -> %s", latest)
    return timestamped


def run_extraction(config: dict[str, Any] | None = None) -> str:
    '''
    Execute the extraction and return the path of the timestamped file.
    '''
    cfg = config or load_config()
    log_level = cfg.get("logging", {}).get("level", "INFO")
    logger = get_logger("extract.finca_raiz", level=log_level)

    apify_cfg = cfg["apify"]
    raw_dir = resolve_path(cfg["paths"]["raw_dir"])

    logger.info("=== Fincaraíz extraction started ===")

    extractor = ApifyExtractor(
        api_token=apify_cfg["api_token"],
        actor_id=apify_cfg["actor_id"],
        logger=logger,
    )

    items = extractor.run_and_fetch(
        run_input=apify_cfg.get("run_input", {}),
        timeout_secs=apify_cfg.get("timeout_secs"),
        memory_mbytes=apify_cfg.get("memory_mbytes"),
        build=apify_cfg.get("build"),
    )

    validate_raw_items(items, logger=logger)
    output_path = _save_json(items, raw_dir, logger)

    logger.info("=== Fincaraíz extraction completed successfully ===")
    return str(output_path)


def main() -> int:
    logger = get_logger("extract.finca_raiz")
    try:
        path = run_extraction()
    except ApifyExtractionError as exc:
        logger.error("Extraction failed: %s", exc)
        return 1
    except Exception as exc:  # noqa: BLE001 - top-level guard
        logger.exception("Unexpected error during extraction: %s", exc)
        return 2
    logger.info("Raw output written to: %s", path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())