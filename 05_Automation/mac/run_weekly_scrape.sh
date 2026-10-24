#!/bin/bash
# =============================================================================
# Script   : run_weekly_scrape.sh
# Layer    : Automation (Mac)
# Purpose  : Runs the scraper, then uploads the run summary and the products CSV
#            to the bucket. The upload of the CSV triggers the pipeline.
#              06_Datasets/MCM_All_products_<YYYY-MM-DD>.csv
#              06_Datasets/MCM_All_products_<YYYY-MM-DD>_run_summary.csv
# When     : Weekly, by launchd (com.mcm.weekly-scrape.plist). Can be run by hand.
# Requires : gcloud CLI signed in (gcloud auth login) with write access to the bucket.
#            04_Scraper/main.py --output <csv> writes both files.
# Usage    : run_weekly_scrape.sh                    scrape, then upload
#            run_weekly_scrape.sh --upload-only <csv> upload an existing CSV (and its summary)
# Settings : SCRAPER_PYTHON  Python to run the scraper (default: <project>/.venv/bin/python)
#            UPLOAD_PREFIX   Bucket folder (default: gs://statement_competitor_data_warehouse/incoming)
# Warning  : Every upload is loaded by the pipeline. Upload each file once.
# =============================================================================

set -uo pipefail

PROJECT="$(cd "$(dirname "$0")/../.." && pwd)"
PYTHON="${SCRAPER_PYTHON:-$PROJECT/.venv/bin/python}"
UPLOAD_PREFIX="${UPLOAD_PREFIX:-gs://statement_competitor_data_warehouse/incoming}"
LOG_DIR="$PROJECT/06_Datasets/logs"
LOCK_DIR="$LOG_DIR/.run.lock"

# launchd starts with a minimal PATH; add the usual gcloud locations.
export PATH="/opt/homebrew/bin:/usr/local/bin:/usr/local/share/google-cloud-sdk/bin:$HOME/google-cloud-sdk/bin:$PATH"

mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/weekly_scrape_$(date +%Y-%m-%d).log"
exec >>"$LOG_FILE" 2>&1

log() { echo "$(date '+%Y-%m-%d %H:%M:%S') $*"; }
notify() { osascript -e "display notification \"$2\" with title \"MCM weekly scrape\" subtitle \"$1\"" >/dev/null 2>&1 || true; }
fail() { log "FAILED: $1"; notify "Failed" "$1 (log: $LOG_FILE)"; exit 1; }

# One run at a time.
mkdir "$LOCK_DIR" 2>/dev/null || fail "another run is in progress ($LOCK_DIR)"
trap 'rmdir "$LOCK_DIR"' EXIT

log "Start (project: $PROJECT)"

# Products CSV: given (--upload-only) or produced by the scraper.
if [[ "${1:-}" == "--upload-only" ]]; then
    CSV="${2:-}"
    [[ -n "$CSV" ]] || fail "--upload-only needs a CSV path"
else
    CSV="$PROJECT/06_Datasets/MCM_All_products_$(date +%Y-%m-%d).csv"
    [[ -e "$CSV" ]] && fail "$(basename "$CSV") already exists; it may have been uploaded already"
    [[ -x "$PYTHON" ]] || fail "Python not found: $PYTHON (set SCRAPER_PYTHON)"
    log "Scraping to $CSV"
    "$PYTHON" "$PROJECT/04_Scraper/main.py" --output "$CSV" || fail "scraper exited with an error"
fi

SUMMARY="${CSV%.csv}_run_summary.csv"
[[ -s "$CSV" ]] || fail "products CSV missing or empty: $CSV"
command -v gcloud >/dev/null || fail "gcloud not found (install the Google Cloud CLI)"

# Run summary first: the pipeline reads it as soon as the CSV arrives.
if [[ -s "$SUMMARY" ]]; then
    log "Uploading $(basename "$SUMMARY")"
    gcloud storage cp "$SUMMARY" "$UPLOAD_PREFIX/" || fail "upload of the run summary failed"
else
    log "No run summary found; the pipeline logs RUN_SUMMARY_MISSING"
fi

log "Uploading $(basename "$CSV")"
gcloud storage cp "$CSV" "$UPLOAD_PREFIX/" || fail "upload of the products CSV failed"

log "Done: $(basename "$CSV") uploaded; the pipeline runs in BigQuery"
notify "Uploaded" "$(basename "$CSV") sent to the pipeline"
