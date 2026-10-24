"""
===============================================================================
Script   : scrape_run_summary.py
Layer    : Scraper
Purpose  : Records how each shop's scrape ran and writes the run summary CSV
           that the warehouse loads next to the products CSV.
             <name>.csv               products (one row per product)
             <name>_run_summary.csv   run summary (one row per shop)
When     : Import in the scraper / compile script. Write the summary after all
           shops have run, then upload both files to the same bucket folder.
Requires : Python 3.8+, standard library only.
Warning  : Column names and order must match bronze_scrape_run_summary.
           Status must be SUCCESS, PARTIAL or FAILED.
===============================================================================

Usage:

    from scrape_run_summary import RunSummary, summary_path_for

    summary = RunSummary(scraper_version="1.0.0")

    for shop_name, scrape in SCRAPERS.items():
        with summary.shop(shop_name) as shop:
            shop.products_expected = 312          # site's own count, if shown
            for page in scrape.pages():
                shop.pages_scraped += 1
                shop.products_found += len(page.products)
            if scrape.stopped_early:
                shop.mark_partial("Stopped at page 7 of 13")
        # An exception inside the block marks the shop FAILED and the loop continues.

    summary.write(summary_path_for("MCM_All_products_2026-09-15.csv"))
"""

import csv
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Iterator, List, Optional

# Column order of bronze_scrape_run_summary (without the raw_ prefix).
COLUMNS = [
    "run_id",
    "shop_name",
    "started_at",
    "finished_at",
    "status",
    "pages_scraped",
    "products_found",
    "products_expected",
    "http_errors",
    "retries",
    "error_message",
    "scraper_version",
]


def _now() -> str:
    """Current UTC time in ISO format, readable by BigQuery."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def summary_path_for(products_csv_path: str) -> str:
    """Run summary file name for a products CSV (same folder, _run_summary suffix)."""
    if not products_csv_path.endswith(".csv"):
        raise ValueError("Products file must end in .csv")
    return products_csv_path[:-4] + "_run_summary.csv"


@dataclass
class ShopRun:
    """Counters and status for one shop's scrape. Update the counters while scraping."""

    shop_name: str
    started_at: str = field(default_factory=_now)
    finished_at: Optional[str] = None
    status: str = "SUCCESS"
    pages_scraped: int = 0
    products_found: int = 0
    products_expected: Optional[int] = None
    http_errors: int = 0
    retries: int = 0
    error_message: Optional[str] = None

    def mark_partial(self, reason: str) -> None:
        """Mark the scrape incomplete (e.g. stopped early). It loads, but is excluded from delisting."""
        self.status = "PARTIAL"
        self.error_message = reason

    def mark_failed(self, reason: str) -> None:
        """Mark the scrape failed."""
        self.status = "FAILED"
        self.error_message = reason


class RunSummary:
    """Collects one ShopRun per shop and writes them as the run summary CSV."""

    def __init__(self, scraper_version: str = "", run_id: Optional[str] = None):
        self.run_id = run_id or str(uuid.uuid4())
        self.scraper_version = scraper_version
        self.shops: List[ShopRun] = []

    @contextmanager
    def shop(self, shop_name: str, suppress_errors: bool = True) -> Iterator[ShopRun]:
        """Track one shop's scrape. Errors mark it FAILED; suppressed by default so other shops still run."""
        run = ShopRun(shop_name=shop_name)
        self.shops.append(run)
        try:
            yield run
        except Exception as exc:
            run.mark_failed(f"{type(exc).__name__}: {exc}")
            if not suppress_errors:
                raise
        finally:
            run.finished_at = _now()

    def write(self, path: str) -> None:
        """Write the run summary CSV (header row + one row per shop)."""
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
            writer.writerow(COLUMNS)
            for s in self.shops:
                writer.writerow([
                    self.run_id,
                    s.shop_name,
                    s.started_at,
                    s.finished_at or "",
                    s.status,
                    s.pages_scraped,
                    s.products_found,
                    "" if s.products_expected is None else s.products_expected,
                    s.http_errors,
                    s.retries,
                    s.error_message or "",
                    self.scraper_version,
                ])
