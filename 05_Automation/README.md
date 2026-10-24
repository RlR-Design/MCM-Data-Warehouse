# Automation

Every week the Mac runs the scraper and uploads its two files to the bucket folder `incoming/`. The upload of the products CSV triggers the `statement-pipeline-trigger` Cloud Run function, which calls `sp_run_pipeline` with that file.

```
Mac, launchd (Monday 06:00)
  run_weekly_scrape.sh
    1. 04_Scraper/main.py --output 06_Datasets/MCM_All_products_<YYYY-MM-DD>.csv
    2. upload <name>_run_summary.csv ──► gs://statement_competitor_data_warehouse/incoming/   (first)
    3. upload <name>.csv             ──► gs://statement_competitor_data_warehouse/incoming/   (last)
                                              │ object finalized event (Eventarc)
                                              ▼
                              statement-pipeline-trigger ──► CALL sp_run_pipeline('gs://…/incoming/<name>.csv')
```

| Setting | Value |
|---|---|
| Project | `statement-competitor-pipeline` |
| Bucket | `statement_competitor_data_warehouse` |
| Trigger folder | `incoming/` (files elsewhere in the bucket never trigger a run) |
| Region / BigQuery location | `us-east1` |

## Rules for uploads

- **Run summary first, products CSV last.** The function fires on the products CSV and the pipeline reads the summary straight away; a summary that arrives later is logged as `RUN_SUMMARY_MISSING`.
- **Dated file names**, e.g. `MCM_All_products_2026-10-27.csv`. A fixed name overwrites last week's raw file.
- **Upload each file once.** Every new upload, even of the same name, is a new object version and runs the pipeline.
- **Manual or test files** go outside `incoming/` (e.g. the bucket root) and are run with `CALL sp_run_pipeline(...)`.

---

## Part A: Deploy the trigger function (one-off)

Run in **Cloud Shell**: Google Cloud console → `>_` (top right). Nothing needs installing.

### A1. Get the code and set variables

```bash
git clone https://github.com/RlR-Design/MCM-Data-Warehouse.git
cd MCM-Data-Warehouse

PROJECT=statement-competitor-pipeline
BUCKET=statement_competitor_data_warehouse
DATASET=statement_competitor_data_warehouse
REGION=us-east1
BQ_LOCATION=us-east1
gcloud config set project $PROJECT
```

Cloud Shell forgets variables when it restarts. If a later command complains about an empty value, set them again.

### A2. Enable the services

```bash
gcloud services enable cloudfunctions.googleapis.com run.googleapis.com \
  eventarc.googleapis.com cloudbuild.googleapis.com artifactregistry.googleapis.com \
  pubsub.googleapis.com bigquery.googleapis.com logging.googleapis.com
```

Takes a minute or two. `01_DDL/09_create_automation_tables.sql` must have been run.

### A3. Check the bucket location

```bash
gcloud storage buckets describe gs://$BUCKET --format="value(location)"
```

- `US-EAST1`: continue.
- `US` (multi-region): continue, and add `--trigger-location=us` to the deploy command in A5.
- Anything else: set `REGION` to that region in lower case. `BQ_LOCATION` stays `us-east1`.

### A4. Service account and permissions

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

If a command asks to choose a condition, choose `None`. Permissions can take a few minutes to apply: if A5 fails on permissions, wait and run it again.

### A5. Deploy

```bash
gcloud functions deploy statement-pipeline-trigger \
  --gen2 --runtime=python312 --region=$REGION \
  --source=05_Automation/pipeline_trigger --entry-point=on_csv_uploaded \
  --trigger-bucket=$BUCKET \
  --service-account=$SA --trigger-service-account=$SA \
  --timeout=540s --memory=256Mi --max-instances=1 --concurrency=1 \
  --set-env-vars=DATASET=$DATASET,BQ_LOCATION=$BQ_LOCATION,TRIGGER_PREFIX=incoming/

gcloud functions add-invoker-policy-binding statement-pipeline-trigger \
  --region=$REGION --member=serviceAccount:$SA
```

The first deploy takes 2 to 4 minutes. Do not add `--retry`: a failed run must be investigated, not repeated.

### A6. Email alert on failure

Console → **Logging → Logs Explorer**, paste:

```
resource.type="cloud_run_revision"
resource.labels.service_name="statement-pipeline-trigger"
severity>=ERROR
```

Run the query, then **Actions → Create log alert**: name `Pipeline failed`, notification channel = your email (create it when asked). Catches failed pipeline runs and function crashes.

### A7. Check it is live

```bash
gcloud functions describe statement-pipeline-trigger --region=$REGION --format="value(state)"
```

Expect `ACTIVE`. The real test is the first upload (Part B, step B3).

---

## Part B: Weekly scrape on the Mac (one-off)

### B1. Install and sign in to the Google Cloud CLI

In Terminal:

```bash
brew install --cask google-cloud-sdk
gcloud auth login
gcloud config set project statement-competitor-pipeline
```

`gcloud auth login` opens the browser; sign in with the Google account that owns the project. The sign-in is kept, so the weekly job runs unattended.

### B2. Make the script executable and create the log folder

```bash
cd ~/DataGripProjects/Statement-Competitor-Pipeline
chmod +x 05_Automation/mac/run_weekly_scrape.sh
mkdir -p 06_Datasets/logs
```

### B3. Test the upload (this runs the pipeline)

Uploads an existing CSV (and its run summary, if present) to `incoming/`, exactly as the weekly job does:

```bash
05_Automation/mac/run_weekly_scrape.sh --upload-only 06_Datasets/MCM_All_products_<YYYY-MM-DD>.csv
```

Then:

- Notification "Uploaded" appears; the log is `06_Datasets/logs/weekly_scrape_<today>.log`.
- Within a minute: `gcloud functions logs read statement-pipeline-trigger --region=us-east1 --limit=20` shows `Starting pipeline for …` and then `Pipeline COMPLETED for …`.
- In DataGrip: `SELECT * FROM statement_competitor_data_warehouse.ops_pipeline_trigger_log;` has one row, and the after-run checks pass.

### B4. Switch on the weekly schedule

Only once `04_Scraper/main.py --output <csv>` produces both files (scraper rewrite). Until then the job runs, finds no CSV and shows a "Failed" notification.

```bash
cp 05_Automation/mac/com.mcm.weekly-scrape.plist ~/Library/LaunchAgents/
launchctl load -w ~/Library/LaunchAgents/com.mcm.weekly-scrape.plist
launchctl list | grep com.mcm.weekly-scrape      # listed = scheduled
```

Run it once now, without waiting for Monday:

```bash
launchctl start com.mcm.weekly-scrape
```

### B5. Make sure the Mac is awake

launchd runs a missed job when the Mac wakes from sleep, but not if it was switched off. To wake (or power on) the Mac every Monday at 05:55:

```bash
sudo pmset repeat wakeorpoweron M 05:55:00
pmset -g sched        # shows the schedule
```

Keep the Mac plugged in on Monday mornings.

### Change the day or time

Edit `Weekday` (0 = Sunday, 1 = Monday ... 6 = Saturday), `Hour` and `Minute` in `05_Automation/mac/com.mcm.weekly-scrape.plist`, then reload:

```bash
cp 05_Automation/mac/com.mcm.weekly-scrape.plist ~/Library/LaunchAgents/
launchctl unload ~/Library/LaunchAgents/com.mcm.weekly-scrape.plist
launchctl load -w ~/Library/LaunchAgents/com.mcm.weekly-scrape.plist
```

Update the `pmset` wake time to match.

---

## Operation

| Task | How |
|---|---|
| See what ran | Mac: `06_Datasets/logs/`. Cloud: `ops_pipeline_trigger_log`, `gcloud functions logs read statement-pipeline-trigger --region=us-east1` |
| A run failed | Email alert (cloud) or "Failed" notification (Mac). Read the log, fix, then re-run (below) |
| Re-run a file already uploaded | `CALL sp_run_pipeline('gs://statement_competitor_data_warehouse/incoming/<file>.csv')` in DataGrip. The trigger never re-runs a file it has handled |
| Upload by hand | `05_Automation/mac/run_weekly_scrape.sh --upload-only <csv>` |
| Pause the Mac schedule | `launchctl unload ~/Library/LaunchAgents/com.mcm.weekly-scrape.plist` |
| Pause the trigger | `gcloud functions delete statement-pipeline-trigger --region=us-east1` (redeploy with A5) |
| Update the function | `git pull` in Cloud Shell, then re-run A5 |

## Limits

- The function waits up to 500 seconds for the pipeline. A longer run carries on in BigQuery; the function logs a warning and the outcome is in the pipeline logs.
- One run at a time (`--max-instances=1`). Steps 2 to 8 work on the latest batch, so runs must never overlap: upload one products CSV per scrape.
- The Mac script refuses to scrape twice on the same day (the dated CSV already exists), so a repeated start never uploads a second copy.
