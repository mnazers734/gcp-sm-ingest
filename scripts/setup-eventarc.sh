#!/bin/bash

# Eventarc Setup Script for GCS Triggers
# This script sets up Eventarc to trigger the ETL dispatcher when manifest.json is uploaded

set -e

# Load config directly
CONFIG_FILE="config/config.yaml"
GCP_PROJECT=$(grep "project_id:" "$CONFIG_FILE" | cut -d'"' -f2 | xargs)
GCP_REGION=$(grep "^region:" "$CONFIG_FILE" | cut -d'"' -f2 | xargs)
SERVICE_NAME=$(grep "service_name:" "$CONFIG_FILE" | cut -d'"' -f2 | xargs)
STORAGE_BUCKET=$(grep "bucket_name:" "$CONFIG_FILE" | cut -d'"' -f2 | xargs)
SERVICE_ACCOUNT=$(grep "service_account:" "$CONFIG_FILE" | cut -d'"' -f2 | xargs)

echo "Setting up Eventarc trigger for GCS events..."
echo "Project: $GCP_PROJECT"
echo "Region: $GCP_REGION"
echo "Service: $SERVICE_NAME"
echo "Bucket: $STORAGE_BUCKET"

# Enable Eventarc API
echo "Enabling Eventarc API..."
gcloud services enable eventarc.googleapis.com

# Delete existing trigger if it exists
# echo "Checking for existing trigger..."
# if gcloud eventarc triggers describe etl-gcs-trigger --location="$GCP_REGION" >/dev/null 2>&1; then
#     echo "Deleting existing trigger..."
#     gcloud eventarc triggers delete etl-gcs-trigger --location="$GCP_REGION" --quiet
# fi

# Create Eventarc trigger
echo "Creating Eventarc trigger..."
gcloud eventarc triggers create etl-gcs-trigger \
    --location="$GCP_REGION" \
    --destination-run-service="$SERVICE_NAME" \
    --destination-run-region="$GCP_REGION" \
    --destination-run-path="/" \
    --event-filters="type=google.cloud.storage.object.v1.finalized" \
    --event-filters="bucket=$STORAGE_BUCKET" \
    --service-account="$SERVICE_ACCOUNT"

echo "Eventarc trigger created successfully!"
echo ""
echo "Configuration:"
echo "- Trigger: etl-gcs-trigger"
echo "- Service: $SERVICE_NAME"
echo "- Bucket: gs://$STORAGE_BUCKET"
echo "- Event: Object finalized"
echo ""
echo "Test the trigger by uploading a manifest.json file to:"
echo "gs://$STORAGE_BUCKET/imports/{partner_id}/{shop_id}/{load_id}/manifest.json"
