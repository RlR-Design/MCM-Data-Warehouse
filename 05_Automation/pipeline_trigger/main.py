"""
===============================================================================
Script   : main.py (Cloud Run function: statement-pipeline-trigger)
Layer    : Automation
Purpose  : Runs sp_run_pipeline when a products CSV lands in the bucket.
           Ignores run summary files and anything outside TRIGGER_PREFIX.
           Each object version (uri + generation) runs once only.
When     : Deployed once; triggered by google.cloud.storage.object.v1.finalized.
Requires : 01_DDL/09_create_automation_tables.sql and all procedures deployed.
Env vars : DATASET              default statement_competitor_data_warehouse
           TRIGGER_PREFIX       only objects under this prefix (default: all)
           BQ_LOCATION          dataset location, e.g. US or africa-south1
           JOB_TIMEOUT_SECONDS  wait for the pipeline (default 500)
Warning  : Deploy with --max-instances=1 and no retries. Upload the run summary
           before the products CSV; the pipeline reads it at load time.
===============================================================================
"""

import logging
import os

import functions_framework
from google.cloud import bigquery

DATASET = os.environ.get("DATASET", "statement_competitor_data_warehouse")
TRIGGER_PREFIX = os.environ.get("TRIGGER_PREFIX", "")
BQ_LOCATION = os.environ.get("BQ_LOCATION") or None
JOB_TIMEOUT_SECONDS = int(os.environ.get("JOB_TIMEOUT_SECONDS", "500"))

SUMMARY_SUFFIX = "_run_summary.csv"

# Claim the object version, then run the pipeline. Skips versions already claimed.
PIPELINE_SCRIPT = f"""
IF EXISTS (
    SELECT 1
    FROM `{DATASET}.ops_pipeline_trigger_log`
    WHERE gcs_uri = @uri AND object_generation = @generation
) THEN
    SELECT 'SKIPPED_DUPLICATE' AS outcome;
ELSE
    INSERT INTO `{DATASET}.ops_pipeline_trigger_log`
        (gcs_uri, object_generation, event_id, triggered_at)
    VALUES (@uri, @generation, @event_id, CURRENT_TIMESTAMP());

    CALL `{DATASET}.sp_run_pipeline`(@uri);

    SELECT 'COMPLETED' AS outcome;
END IF;
"""

logging.basicConfig(level=logging.INFO)
client = bigquery.Client()


def is_products_csv(name: str) -> bool:
    """True for a products CSV under the trigger prefix."""
    return (
        name.startswith(TRIGGER_PREFIX)
        and name.lower().endswith(".csv")
        and not name.lower().endswith(SUMMARY_SUFFIX)
    )


@functions_framework.cloud_event
def on_csv_uploaded(cloud_event):
    data = cloud_event.data
    name = data["name"]
    uri = f"gs://{data['bucket']}/{name}"

    if not is_products_csv(name):
        logging.info("Ignored %s", uri)
        return

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("uri", "STRING", uri),
            bigquery.ScalarQueryParameter("generation", "STRING", str(data.get("generation", ""))),
            bigquery.ScalarQueryParameter("event_id", "STRING", cloud_event["id"]),
        ],
        labels={"source": "pipeline-trigger"},
    )

    logging.info("Starting pipeline for %s", uri)
    job = client.query(PIPELINE_SCRIPT, job_config=job_config, location=BQ_LOCATION)

    try:
        rows = list(job.result(timeout=JOB_TIMEOUT_SECONDS))
    except TimeoutError:
        # The job keeps running in BigQuery; check its outcome in the logs.
        logging.warning("Pipeline still running after %ss for %s (job %s)",
                        JOB_TIMEOUT_SECONDS, uri, job.job_id)
        return
    except Exception:
        logging.exception("PIPELINE FAILED for %s (job %s)", uri, job.job_id)
        raise

    outcome = rows[0]["outcome"] if rows else "UNKNOWN"
    logging.info("Pipeline %s for %s (job %s)", outcome, uri, job.job_id)
