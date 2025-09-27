#!/bin/bash

set -e

# Load config directly
CONFIG_FILE="config/config.yaml"
GCP_PROJECT=$(grep "project_id:" "$CONFIG_FILE" | cut -d'"' -f2 | xargs)
GCP_REGION=$(grep "^region:" "$CONFIG_FILE" | cut -d'"' -f2 | xargs)
SERVICE_NAME=$(grep "service_name:" "$CONFIG_FILE" | cut -d'"' -f2 | xargs)
STORAGE_BUCKET=$(grep "bucket_name:" "$CONFIG_FILE" | cut -d'"' -f2 | xargs)
SERVICE_ACCOUNT=$(grep "service_account:" "$CONFIG_FILE" | cut -d'"' -f2 | xargs)
JOB_NAME=$(grep "job_name:" "$CONFIG_FILE" | cut -d'"' -f2 | xargs)

echo "DEBUG - Variables loaded:"
echo "GCP_PROJECT: '$GCP_PROJECT'"
echo "GCP_REGION: '$GCP_REGION'" 
echo "SERVICE_NAME: '$SERVICE_NAME'"
echo "SERVICE_ACCOUNT: '$SERVICE_ACCOUNT'"
echo "JOB_NAME: '$JOB_NAME'"
echo "STORAGE_BUCKET: '$STORAGE_BUCKET'"

echo "Deploying ETL Dispatcher to Cloud Run..."

gcloud run deploy "$SERVICE_NAME" \
  --source=./src/dispatcher \
  --platform=managed \
  --region="$GCP_REGION" \
  --project="$GCP_PROJECT" \
  --service-account="$SERVICE_ACCOUNT" \
  --allow-unauthenticated \
  --set-env-vars="GCP_PROJECT=$GCP_PROJECT,GCP_REGION=$GCP_REGION,STORAGE_BUCKET=$STORAGE_BUCKET,JOB_NAME=$JOB_NAME" \
  --memory=2Gi \
  --cpu=1 \
  --max-instances=10 \
  --min-instances=0 \
  --clear-vpc-connector

echo "Dispatcher deployed successfully!"

SERVICE_URL=$(gcloud run services describe $SERVICE_NAME \
    --platform=managed \
    --region="$GCP_REGION" \
    --project="$GCP_PROJECT" \
    --format="value(status.url)")

echo "Service URL: $SERVICE_URL"