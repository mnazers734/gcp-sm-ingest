#!/bin/bash
set -e

# Load config directly
CONFIG_FILE="config/config.yaml"
GCP_PROJECT=$(grep "project_id:" "$CONFIG_FILE" | cut -d'"' -f2 | xargs)
GCP_REGION=$(grep "^region:" "$CONFIG_FILE" | cut -d'"' -f2 | xargs)
JOB_NAME=$(grep "job_name:" "$CONFIG_FILE" | cut -d'"' -f2 | xargs)
STORAGE_BUCKET=$(grep "bucket_name:" "$CONFIG_FILE" | cut -d'"' -f2 | xargs)
SERVICE_ACCOUNT=$(grep "service_account:" "$CONFIG_FILE" | cut -d'"' -f2 | xargs)

echo "Deploying Cloud Run Job: $JOB_NAME"

gcloud run jobs deploy "$JOB_NAME" \
  --source src/processor \
  --region="$GCP_REGION" \
  --service-account="$SERVICE_ACCOUNT" \
  --set-env-vars="GCP_PROJECT=$GCP_PROJECT,GCP_REGION=$GCP_REGION,STORAGE_BUCKET=$STORAGE_BUCKET" \
  --cpu=2 \
  --memory=4Gi \
  --max-retries=3

echo "Deployment complete!"
