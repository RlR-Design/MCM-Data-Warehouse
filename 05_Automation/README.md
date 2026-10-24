# Automation

A products CSV uploaded to the bucket triggers the `statement-pipeline-trigger` Cloud Run function. The function calls `sp_run_pipeline` with that file's URI.

```
Scraper (weekly) ──► gs://<bucket>/<prefix><name>_run_summary.csv   (upload first)
                 └─► gs://<bucket>/<prefix><name>.csv               (upload last)
                          │ object finalized event (Eventarc)
                          ▼
                 statement-pipeline-trigger ──► CALL sp_run_pipeline('gs://…/<name>.csv')
```

## Rules for the scraper upload

- **Upload the run summary first, then the products CSV.** The function fires on the products CSV, and the pipeline reads the summary straight away. A summary that arrives late is logged as `SUMMARY_MISSING`.
- **Use a dated file name**, for example `MCM_All_products_20261027.csv`. A fixed name overwrites last week's raw file, and you lose the ability to replay or rebuild.
- **Upload each file once.** Every new upload, even of the same name, is a new object version and triggers a run.

## One-off deployment

Run these in Cloud Shell (console → `>_` icon) or a local terminal with the gcloud CLI. Replace `REGION` with the region the bucket and dataset are in (see step 2).

### 1. Prerequisites

```bash
PROJECT=statement-competitor-pipeline
BUCKET=statement_competitor_data_warehouse
DATASET=statement_competitor_data_warehouse
gcloud config set project $PROJECT

gcloud services enable cloudfunctions.googleapis.com run.googleapis.com \
  eventarc.googleapis.com cloudbuild.googleapis.com artifactregistry.googleapis.com \
  pubsub.googleapis.com bigquery.googleapis.com logging.googleapis.com
```

Then run `01_DDL/09_create_automation_tables.sql` in DataGrip.

### 2. Find the locations

```bash
gcloud storage buckets describe gs://$BUCKET --format="value(location)"
bq show --format=prettyjson $PROJECT:$DATASET | grep '"location"'
```

- Bucket in a single region (for example `AFRICA-SOUTH1`): use that region, in lower case, as `REGION`.
- Bucket in a multi-region (`US`, `EU`): use a region inside it, for example `us-central1`, and add `--trigger-location=us` (or `eu`) to the deploy command.
- Set `BQ_LOCATION` to the dataset location exactly as shown, for example `US`.

```bash
REGION=<region>
BQ_LOCATION=<dataset location>
```

### 3. Service account and permissions

```bash
gcloud iam service-accounts create pipeline-trigger --display-name="Statement pipeline trigger"
SA=pipeline-trigger@$PROJECT.iam.gserviceaccount.com

# Run BigQuery jobs and read/write the dataset.
gcloud projects add-iam-policy-binding $PROJECT --member=serviceAccount:$SA --role=roles/bigquery.jobUser
gcloud projects add-iam-policy-binding $PROJECT --member=serviceAccount:$SA --role=roles/bigquery.dataEditor

# Read the scrape files (LOAD DATA runs as this account).
gcloud storage buckets add-iam-policy-binding gs://$BUCKET --member=serviceAccount:$SA --role=roles/storage.objectViewer

# Receive Eventarc events.
gcloud projects add-iam-policy-binding $PROJECT --member=serviceAccount:$SA --role=roles/eventarc.eventReceiver

# Let Cloud Storage publish events.
GCS_AGENT=$(gcloud storage service-agent --project=$PROJECT)
gcloud projects add-iam-policy-binding $PROJECT --member=serviceAccount:$GCS_AGENT --role=roles/pubsub.publisher
```

IAM changes can take a few minutes to apply. If the deploy fails on permissions, wait and retry.

### 4. Deploy

From the repository root:

```bash
gcloud functions deploy statement-pipeline-trigger \
  --gen2 --runtime=python312 --region=$REGION \
  --source=05_Automation/pipeline_trigger --entry-point=on_csv_uploaded \
  --trigger-bucket=$BUCKET \
  --service-account=$SA --trigger-service-account=$SA \
  --timeout=540s --memory=256Mi --max-instances=1 --concurrency=1 \
  --set-env-vars=DATASET=$DATASET,BQ_LOCATION=$BQ_LOCATION,TRIGGER_PREFIX=

gcloud functions add-invoker-policy-binding statement-pipeline-trigger \
  --region=$REGION --member=serviceAccount:$SA
```

- Do not add `--retry`. A failed run should be investigated, not repeated automatically.
- To trigger only on a folder (recommended, for example `incoming/`), set `TRIGGER_PREFIX=incoming/`.

### 5. Alert on failure

Console → Logging → Logs Explorer, query:

```
resource.type="cloud_run_revision"
resource.labels.service_name="statement-pipeline-trigger"
severity>=ERROR
```

**Create alert** → notification channel: your email. This catches failed pipeline runs and function crashes.

### 6. Test

```bash
gcloud storage cp datasets/<summary>_run_summary.csv gs://$BUCKET/
gcloud storage cp datasets/<file>.csv gs://$BUCKET/
gcloud functions logs read statement-pipeline-trigger --region=$REGION --limit=20
```

Expect `Starting pipeline for …`, then `Pipeline COMPLETED for …`. Then run the after-run checks (Manual 12.1) and the Tests block of `09_create_automation_tables.sql`.

## Operation

| Task | How |
|---|---|
| See what ran | `ops_pipeline_trigger_log`, function logs |
| Re-run a file | `CALL sp_run_pipeline('gs://…')` in DataGrip. The trigger never re-runs a claimed file. |
| Pause automation | `gcloud functions delete statement-pipeline-trigger --region=$REGION`, or upload outside `TRIGGER_PREFIX` |
| Update the function | Re-run the deploy command |

## Limits

- The function waits up to `JOB_TIMEOUT_SECONDS` (default 500). A longer pipeline keeps running in BigQuery; the function logs a warning, and the outcome is in the pipeline logs.
- One run at a time (`--max-instances=1`). Steps 2 to 8 work on the latest batch, so two runs must never overlap. Upload one products CSV per scrape.
