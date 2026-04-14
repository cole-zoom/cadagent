# Deploying trace-ca to GCP

A step-by-step guide to go from a fresh GCP project to a running data pipeline and query API. Written for a developer who has billing enabled but nothing else set up.

This is not a "shortest path to prod" guide. It prioritizes a workflow where you can iterate locally, push via git, build containers with a single command, and tail logs from your terminal. No copy-pasting into console UIs.

---

## Prerequisites

Install these on your machine before starting:

| Tool | Version | Install |
|------|---------|---------|
| `gcloud` CLI | latest | `brew install google-cloud-sdk` |
| `terraform` | >= 1.5 | `brew install terraform` |
| `docker` | latest | Docker Desktop or `brew install docker` |
| `python` | 3.12+ | `brew install python@3.12` |
| `git` | latest | already installed on macOS |

---

## Section 1: GCP Project Setup

### 1.1 Pick your project ID

Decide on a project ID. It must be globally unique. This guide uses `trace-ca-dev` as an example -- replace it everywhere with yours.

```bash
export PROJECT_ID="trace-ca-dev"
export REGION="us-east1"
```

Put these in your shell profile (`~/.zshrc`) so they persist across terminal sessions.

### 1.2 Authenticate gcloud

```bash
gcloud auth login
gcloud auth application-default login
```

The first command authenticates the CLI. The second creates Application Default Credentials (ADC) that Python libraries use when running locally. You will use these for local development instead of downloading service account keys.

### 1.3 Set the active project

```bash
gcloud config set project $PROJECT_ID
```

### 1.4 Enable required APIs

These are the GCP APIs the platform uses. Enable them all at once:

```bash
gcloud services enable \
  cloudbuild.googleapis.com \
  run.googleapis.com \
  artifactregistry.googleapis.com \
  bigquery.googleapis.com \
  storage.googleapis.com \
  iam.googleapis.com \
  cloudresourcemanager.googleapis.com \
  secretmanager.googleapis.com
```

This takes about 30 seconds. If any fail, your billing might not be linked -- check the console.

### 1.5 Verify

```bash
gcloud config get-value project
# Should print: trace-ca-dev

gcloud services list --enabled --filter="name:bigquery OR name:run OR name:storage" --format="value(name)"
# Should list the APIs you just enabled
```

---

## Section 2: Git Repository

### 2.1 Initialize git

From the project root:

```bash
cd /path/to/trace-ca
git init
git add .
git commit -m "Initial commit: full monorepo scaffold"
```

### 2.2 Create remote repository

Pick your git host. GitHub example:

```bash
gh repo create trace-ca --private --source=. --remote=origin --push
```

Or for an existing repo:

```bash
git remote add origin git@github.com:YOUR_USER/trace-ca.git
git push -u origin main
```

### 2.3 Branch strategy

For a small team / solo:

- `main` -- always deployable
- `dev` -- integration branch, deploy to dev environment
- Feature branches off `dev`

Don't overthink this at the start. A single `main` branch with direct pushes is fine until you have collaborators.

---

## Section 3: Terraform -- Provision Infrastructure

Terraform creates everything: buckets, BigQuery datasets, service accounts, Artifact Registry, and Cloud Run jobs/services. You run it once and it's all there.

### 3.1 Create a tfvars file

```bash
cat > infra/terraform/terraform.tfvars <<EOF
project_id  = "${PROJECT_ID}"
region      = "${REGION}"
environment = "dev"
EOF
```

Add `terraform.tfvars` to `.gitignore` (it already ignores `*.tfstate` but not tfvars with project-specific values):

```bash
echo "infra/terraform/terraform.tfvars" >> .gitignore
```

### 3.2 Initialize Terraform

```bash
cd infra/terraform
terraform init
```

This downloads the Google provider plugin. You should see "Terraform has been successfully initialized."

### 3.3 Plan (dry run)

```bash
terraform plan
```

Review the output. It will create approximately 15 resources:
- 2 GCS buckets (raw, processed)
- 4 BigQuery datasets (raw, stg, cur, quality)
- 1 service account + 6 IAM bindings
- 1 Artifact Registry repository
- 3 Cloud Run jobs (ingest, extract, normalize)
- 1 Cloud Run service (agent-api)

The Cloud Run resources will fail on first apply because the container images don't exist yet. That's expected -- we'll handle it in Section 5.

### 3.4 Apply (create resources)

For the first apply, we want everything except the Cloud Run resources (which need images). Target just the foundational infra:

```bash
terraform apply \
  -target=google_storage_bucket.raw \
  -target=google_storage_bucket.processed \
  -target=google_bigquery_dataset.raw \
  -target=google_bigquery_dataset.stg \
  -target=google_bigquery_dataset.cur \
  -target=google_bigquery_dataset.quality \
  -target=google_service_account.pipeline \
  -target=google_project_iam_member.pipeline_bq_editor \
  -target=google_project_iam_member.pipeline_bq_user \
  -target=google_storage_bucket_iam_member.pipeline_raw_writer \
  -target=google_storage_bucket_iam_member.pipeline_raw_reader \
  -target=google_storage_bucket_iam_member.pipeline_processed_writer \
  -target=google_storage_bucket_iam_member.pipeline_processed_reader \
  -target=google_artifact_registry_repository.containers
```

Type `yes` to confirm. Should take 30-60 seconds.

### 3.5 Verify infrastructure

```bash
# Buckets
gsutil ls | grep $PROJECT_ID
# gs://trace-ca-dev-raw/
# gs://trace-ca-dev-processed/

# BigQuery datasets
bq ls --project_id=$PROJECT_ID
# raw
# stg
# cur
# quality

# Artifact Registry
gcloud artifacts repositories list --location=$REGION
# trace-ca   DOCKER   ...

# Service account
gcloud iam service-accounts list --filter="email:trace-pipeline"
# trace-pipeline@trace-ca-dev.iam.gserviceaccount.com
```

### 3.6 Go back to the repo root

```bash
cd ../..
```

---

## Section 4: Create BigQuery Tables

The DDL files define the schema. Run them against your project.

### 4.1 Run all DDL files

```bash
# Raw layer
for f in sql/raw/*.sql; do
  echo "Running $f..."
  sed "s/\${PROJECT_ID}/$PROJECT_ID/g" "$f" | bq query --use_legacy_sql=false
done

# Staging layer
for f in sql/staging/*.sql; do
  echo "Running $f..."
  sed "s/\${PROJECT_ID}/$PROJECT_ID/g" "$f" | bq query --use_legacy_sql=false
done

# Curated layer
for f in sql/curated/*.sql; do
  echo "Running $f..."
  sed "s/\${PROJECT_ID}/$PROJECT_ID/g" "$f" | bq query --use_legacy_sql=false
done
```

### 4.2 Verify tables

```bash
bq ls ${PROJECT_ID}:raw
# documents   extracted_tables   extracted_cells

bq ls ${PROJECT_ID}:stg
# headers   row_values_long

bq ls ${PROJECT_ID}:cur
# dim_department   dim_document   dim_metric   dim_time   dim_geography
# dim_scenario   dim_attribute_type   dim_attribute_value
# fact_observation   bridge_observation_attribute

bq ls ${PROJECT_ID}:quality
# observation_quality
```

### 4.3 Seed dimension tables

This populates the curated dimension tables with data from the YAML mapping dictionaries (departments, provinces, scenarios, initial metrics, attribute types).

```bash
# Set up local env
cp .env.example .env
```

Edit `.env` and set your actual values:

```
GCP_PROJECT_ID=trace-ca-dev
GCP_REGION=us-east1
GCS_RAW_BUCKET=trace-ca-dev-raw
GCS_PROCESSED_BUCKET=trace-ca-dev-processed
```

Then run the seed script:

```bash
source .venv/bin/activate
python scripts/seed_mappings.py
```

You should see:
```
Seeded 3 departments
Seeded 22 geographies
Seeded 2 scenarios
Seeded 50 metrics
Seeded 6 attribute types and 24 attribute values
```

### 4.4 Verify seeded data

```bash
bq query --use_legacy_sql=false "SELECT * FROM ${PROJECT_ID}.cur.dim_department"
# fin, statcan, tbs-sct

bq query --use_legacy_sql=false "SELECT COUNT(*) FROM ${PROJECT_ID}.cur.dim_geography"
# 22

bq query --use_legacy_sql=false "SELECT COUNT(*) FROM ${PROJECT_ID}.cur.dim_metric"
# 50
```

---

## Section 5: Build and Push Container Images

Each pipeline stage has its own Docker image. We use Artifact Registry (not Docker Hub) so images stay in the same GCP project.

### 5.1 Configure Docker for Artifact Registry

```bash
gcloud auth configure-docker ${REGION}-docker.pkg.dev
```

This adds the Artifact Registry domain to your Docker config so `docker push` works without separate credentials.

### 5.2 Set the registry URL

```bash
export REGISTRY="${REGION}-docker.pkg.dev/${PROJECT_ID}/trace-ca"
```

### 5.3 Build and push all images

Run from the repo root. Each Dockerfile uses `COPY` paths relative to the repo root, so the build context must be `.`:

```bash
# Ingest
docker build -f services/ingest/Dockerfile -t ${REGISTRY}/ingest:latest .
docker push ${REGISTRY}/ingest:latest

# Extract
docker build -f services/extract/Dockerfile -t ${REGISTRY}/extract:latest .
docker push ${REGISTRY}/extract:latest

# Normalize
docker build -f services/normalize/Dockerfile -t ${REGISTRY}/normalize:latest .
docker push ${REGISTRY}/normalize:latest

# Agent API
docker build -f services/agent_api/Dockerfile -t ${REGISTRY}/agent-api:latest .
docker push ${REGISTRY}/agent-api:latest
```

Each build takes 30-60 seconds. The pushes depend on your upload speed.

### 5.4 Verify images in Artifact Registry

```bash
gcloud artifacts docker images list ${REGISTRY} --format="table(package,version)"
```

You should see all 4 images.

### 5.5 Shortcut: Build script

To avoid typing all this every time, create a build script:

```bash
cat > scripts/build_and_push.sh <<'SCRIPT'
#!/bin/bash
set -euo pipefail

PROJECT_ID="${PROJECT_ID:?Set PROJECT_ID}"
REGION="${REGION:-us-east1}"
REGISTRY="${REGION}-docker.pkg.dev/${PROJECT_ID}/trace-ca"
TAG="${1:-latest}"

for service in ingest extract normalize agent-api; do
  SVC_DIR="services/${service//-/_}"
  echo "Building ${service}:${TAG}..."
  docker build -f "${SVC_DIR}/Dockerfile" -t "${REGISTRY}/${service}:${TAG}" .
  docker push "${REGISTRY}/${service}:${TAG}"
  echo "Pushed ${REGISTRY}/${service}:${TAG}"
done
SCRIPT
chmod +x scripts/build_and_push.sh
```

Usage: `./scripts/build_and_push.sh` (uses `latest` tag) or `./scripts/build_and_push.sh v0.1.0`.

---

## Section 6: Deploy Cloud Run Jobs and Services

Now that images exist, deploy the Cloud Run resources.

### 6.1 Deploy via Terraform

Go back to the Terraform directory and apply the full plan:

```bash
cd infra/terraform
terraform apply
```

This time it will create the Cloud Run jobs and service. Type `yes`.

### 6.2 Alternative: Deploy without Terraform

If you want to iterate faster on a single service without running the full Terraform plan:

```bash
# Deploy a Cloud Run job
gcloud run jobs deploy ingest-dev \
  --image=${REGISTRY}/ingest:latest \
  --region=$REGION \
  --service-account=trace-pipeline@${PROJECT_ID}.iam.gserviceaccount.com \
  --set-env-vars="GCP_PROJECT_ID=${PROJECT_ID},GCS_RAW_BUCKET=${PROJECT_ID}-raw,GCS_PROCESSED_BUCKET=${PROJECT_ID}-processed" \
  --memory=1Gi \
  --cpu=1 \
  --task-timeout=3600

# Deploy the Agent API service
gcloud run deploy agent-api-dev \
  --image=${REGISTRY}/agent-api:latest \
  --region=$REGION \
  --service-account=trace-pipeline@${PROJECT_ID}.iam.gserviceaccount.com \
  --set-env-vars="GCP_PROJECT_ID=${PROJECT_ID}" \
  --memory=2Gi \
  --cpu=1 \
  --allow-unauthenticated \
  --port=8080
```

This is useful when you're iterating on one service and don't want to rebuild all Terraform state. Just remember to keep Terraform in sync afterward (`terraform import` or update the `.tf` to match).

### 6.3 Verify deployments

```bash
# List Cloud Run jobs
gcloud run jobs list --region=$REGION
# ingest-dev   extract-dev   normalize-dev

# List Cloud Run services
gcloud run services list --region=$REGION
# agent-api-dev   https://agent-api-dev-xxxxx.run.app
```

---

## Section 7: Run the Pipeline

### 7.1 Run ingestion (single department, small test)

Start with one department to verify the pipeline works end-to-end:

```bash
gcloud run jobs execute ingest-dev \
  --region=$REGION \
  --set-env-vars="DEPARTMENTS=fin,INGEST_MODE=incremental"
```

### 7.2 Watch logs

In a separate terminal:

```bash
gcloud run jobs executions list --job=ingest-dev --region=$REGION --limit=1

# Get the execution name, then stream logs:
gcloud logging read "resource.type=cloud_run_job AND resource.labels.job_name=ingest-dev" \
  --limit=50 --format="value(jsonPayload.message)" --freshness=10m
```

Or use the simpler (but less filterable) approach:

```bash
gcloud run jobs executions logs EXECUTION_NAME --region=$REGION
```

### 7.3 Verify ingestion results

```bash
# How many documents were ingested?
bq query --use_legacy_sql=false \
  "SELECT department_id, ingestion_status, COUNT(*) as cnt
   FROM ${PROJECT_ID}.raw.documents
   GROUP BY 1, 2
   ORDER BY 1, 2"

# Check GCS
gsutil ls gs://${PROJECT_ID}-raw/raw/goc/department=fin/ | head -5
```

### 7.4 Run extraction

```bash
gcloud run jobs execute extract-dev \
  --region=$REGION \
  --set-env-vars="DEPARTMENT=fin"
```

Verify:

```bash
bq query --use_legacy_sql=false \
  "SELECT COUNT(*) as tables_extracted FROM ${PROJECT_ID}.raw.extracted_tables"

bq query --use_legacy_sql=false \
  "SELECT COUNT(*) as headers FROM ${PROJECT_ID}.stg.headers"
```

### 7.5 Run normalization

```bash
gcloud run jobs execute normalize-dev \
  --region=$REGION \
  --set-env-vars="DEPARTMENT=fin"
```

Verify:

```bash
bq query --use_legacy_sql=false \
  "SELECT COUNT(*) as observations FROM ${PROJECT_ID}.cur.fact_observation"

bq query --use_legacy_sql=false \
  "SELECT dm.canonical_name, COUNT(*) as obs_count
   FROM ${PROJECT_ID}.cur.fact_observation f
   JOIN ${PROJECT_ID}.cur.dim_metric dm ON f.metric_id = dm.metric_id
   GROUP BY 1
   ORDER BY 2 DESC
   LIMIT 10"
```

### 7.6 Run all three departments

Once fin works, run the full pipeline:

```bash
# Ingest all
gcloud run jobs execute ingest-dev --region=$REGION

# Extract all
gcloud run jobs execute extract-dev --region=$REGION

# Normalize all
gcloud run jobs execute normalize-dev --region=$REGION
```

---

## Section 8: Agent API

### 8.1 Store the Anthropic API key

Use Secret Manager instead of environment variables for secrets:

```bash
echo -n "YOUR_ANTHROPIC_API_KEY" | \
  gcloud secrets create anthropic-api-key --data-file=-

# Grant the pipeline SA access
gcloud secrets add-iam-policy-binding anthropic-api-key \
  --member="serviceAccount:trace-pipeline@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/secretmanager.secretAccessor"
```

### 8.2 Deploy with the secret

```bash
gcloud run deploy agent-api-dev \
  --image=${REGISTRY}/agent-api:latest \
  --region=$REGION \
  --service-account=trace-pipeline@${PROJECT_ID}.iam.gserviceaccount.com \
  --set-env-vars="GCP_PROJECT_ID=${PROJECT_ID}" \
  --set-secrets="ANTHROPIC_API_KEY=anthropic-api-key:latest" \
  --memory=2Gi \
  --cpu=1 \
  --allow-unauthenticated \
  --port=8080
```

### 8.3 Get the service URL

```bash
export API_URL=$(gcloud run services describe agent-api-dev --region=$REGION --format="value(status.url)")
echo $API_URL
```

### 8.4 Test it

```bash
# Health check
curl ${API_URL}/health
# {"status":"ok","service":"agent_api"}

# Ask a question
curl -X POST ${API_URL}/ask \
  -H "Content-Type: application/json" \
  -d '{"question": "What was Canada real GDP growth in 2023?", "department": "fin"}'

# Get SQL without executing
curl -X POST ${API_URL}/explain \
  -H "Content-Type: application/json" \
  -d '{"question": "Compare CPI inflation across provinces for 2023-24"}'
```

---

## Section 9: Local Development Workflow

This is the day-to-day flow for iterating on the pipeline.

### 9.1 Run services locally

You don't need Docker or Cloud Run to test code changes. The services work locally with ADC:

```bash
source .venv/bin/activate

# Run ingest for a single department
DEPARTMENTS=fin INGEST_MODE=incremental python -m services.ingest.main

# Extract a specific document
DOCUMENT_ID=abc123 python -m services.extract.main

# Run normalization
DEPARTMENT=fin python -m services.normalize.main

# Start the agent API locally
uvicorn services.agent_api.main:app --reload --port 8080
```

The `--reload` flag on uvicorn restarts the server when you change code. Your local `.env` file provides the config.

### 9.2 Run tests

```bash
# Full suite
python -m pytest tests/ -v

# Single file
python -m pytest tests/shared/utils/test_time_parsing.py -v

# With coverage
python -m pytest tests/ --cov=shared --cov=services --cov-report=term-missing
```

### 9.3 Deploy a code change

The typical cycle:

```bash
# 1. Make your code change
# 2. Run tests
python -m pytest tests/ -v

# 3. Rebuild and push only the changed service
docker build -f services/ingest/Dockerfile -t ${REGISTRY}/ingest:latest .
docker push ${REGISTRY}/ingest:latest

# 4. Update the Cloud Run job to use the new image
gcloud run jobs update ingest-dev --image=${REGISTRY}/ingest:latest --region=$REGION

# 5. Execute it
gcloud run jobs execute ingest-dev --region=$REGION
```

For the agent API (always-on service), updating the image triggers a new revision automatically:

```bash
gcloud run deploy agent-api-dev --image=${REGISTRY}/agent-api:latest --region=$REGION
```

### 9.4 Tail logs during development

```bash
# Stream Cloud Run service logs (agent API)
gcloud run services logs tail agent-api-dev --region=$REGION

# Stream Cloud Run job logs
gcloud logging read "resource.type=cloud_run_job AND resource.labels.job_name=ingest-dev" \
  --limit=100 --format="value(jsonPayload.message)" --freshness=5m
```

### 9.5 Query BigQuery from terminal

```bash
# Ad-hoc queries
bq query --use_legacy_sql=false \
  "SELECT * FROM ${PROJECT_ID}.cur.fact_observation LIMIT 10"

# Interactive shell
bq shell
```

---

## Section 10: Scheduling (Optional)

Set up recurring ingestion with Cloud Scheduler.

### 10.1 Enable Cloud Scheduler

```bash
gcloud services enable cloudscheduler.googleapis.com
```

### 10.2 Create a schedule

```bash
# Run ingestion every Sunday at 3 AM ET
gcloud scheduler jobs create http ingest-weekly \
  --location=$REGION \
  --schedule="0 3 * * 0" \
  --time-zone="America/Toronto" \
  --uri="https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs/ingest-dev:run" \
  --http-method=POST \
  --oauth-service-account-email=trace-pipeline@${PROJECT_ID}.iam.gserviceaccount.com
```

### 10.3 Chain jobs

To run extract after ingest and normalize after extract, use Cloud Workflows or a simple orchestrator script:

```bash
cat > scripts/run_full_pipeline.sh <<'SCRIPT'
#!/bin/bash
set -euo pipefail
PROJECT_ID="${PROJECT_ID:?Set PROJECT_ID}"
REGION="${REGION:-us-east1}"

echo "Starting ingestion..."
gcloud run jobs execute ingest-dev --region=$REGION --wait

echo "Starting extraction..."
gcloud run jobs execute extract-dev --region=$REGION --wait

echo "Starting normalization..."
gcloud run jobs execute normalize-dev --region=$REGION --wait

echo "Pipeline complete."
SCRIPT
chmod +x scripts/run_full_pipeline.sh
```

The `--wait` flag blocks until the job finishes, so the steps run sequentially.

---

## Section 11: Debugging Checklist

When something goes wrong, check in this order:

### Pipeline job fails immediately

```bash
# Check the execution status
gcloud run jobs executions describe EXECUTION_NAME --region=$REGION

# Check container logs
gcloud run jobs executions logs EXECUTION_NAME --region=$REGION
```

Common causes: missing env vars, wrong image tag, service account missing permissions.

### No data in BigQuery after ingestion

```bash
# Check document count and status
bq query --use_legacy_sql=false \
  "SELECT ingestion_status, COUNT(*) FROM ${PROJECT_ID}.raw.documents GROUP BY 1"
```

If status is `failed`: check logs. If 0 rows: the API might have returned no results (check department codes, API availability).

### Extraction produces 0 headers

```bash
# Check what formats were ingested
bq query --use_legacy_sql=false \
  "SELECT file_format, COUNT(*) FROM ${PROJECT_ID}.raw.documents GROUP BY 1"
```

If the format is not supported by any parser (e.g., `zip`), extraction will skip it.

### Agent API returns bad SQL

The LLM might hallucinate table or column names. Check the `/explain` endpoint first:

```bash
curl -X POST ${API_URL}/explain \
  -H "Content-Type: application/json" \
  -d '{"question": "your question"}'
```

Review the generated SQL. If it references wrong columns, update `services/agent_api/prompts/system_prompt.txt` with better examples.

### Permission denied errors

```bash
# Check SA permissions
gcloud projects get-iam-policy $PROJECT_ID \
  --flatten="bindings[].members" \
  --filter="bindings.members:trace-pipeline" \
  --format="table(bindings.role)"
```

The pipeline SA needs: `bigquery.dataEditor`, `bigquery.jobUser`, `storage.objectCreator`, `storage.objectViewer`.

---

## Section 12: Cost Awareness

This setup is cheap for a dev environment. Approximate monthly costs at low volume:

| Resource | Cost |
|----------|------|
| Cloud Run jobs (3 jobs, ~1 hour/week total) | ~$0.50 |
| Cloud Run service (agent API, scales to 0) | ~$0-5 (pay per request) |
| GCS storage (a few GB of GoC files) | ~$0.50 |
| BigQuery (< 1 TB stored, < 1 TB queried) | Free tier covers it |
| Artifact Registry (4 images) | ~$0.50 |
| Anthropic API (per agent query) | $0.003-0.015 per query |
| **Total (excluding Anthropic)** | **~$2-7/month** |

The biggest cost driver will be Anthropic API usage once the agent API is active. Budget accordingly.

### Cost reduction tips

- Set Cloud Run min instances to 0 (already configured)
- Use BigQuery partitioning (already configured) to limit scan costs
- Archive raw GCS files to Nearline after 90 days (already configured)
- Monitor with `gcloud billing budgets create` to get alerts

---

## Quick Reference

```bash
# Infrastructure
cd infra/terraform && terraform apply

# Build all images
./scripts/build_and_push.sh

# Run full pipeline
./scripts/run_full_pipeline.sh

# Run tests
python -m pytest tests/ -v

# Stream agent API logs
gcloud run services logs tail agent-api-dev --region=$REGION

# Query warehouse
bq query --use_legacy_sql=false "SELECT ..."

# Local dev server
uvicorn services.agent_api.main:app --reload --port 8080
```
