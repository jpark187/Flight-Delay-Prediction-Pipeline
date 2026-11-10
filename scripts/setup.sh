#!/usr/bin/env bash
# setup.sh - one-time project setup: set the project, enable APIs, create the data bucket.
set -euo pipefail
cd "$(dirname "$0")/.."
[ -f env.sh ] || { echo "copy env.sh.example to env.sh and fill it in first"; exit 1; }
source env.sh

gcloud config set project "$PROJECT_ID"
gcloud config set compute/region "$REGION"
gcloud config set compute/zone "$ZONE"

echo "enabling APIs (takes a minute)..."
gcloud services enable storage.googleapis.com compute.googleapis.com dataproc.googleapis.com \
    run.googleapis.com artifactregistry.googleapis.com cloudbuild.googleapis.com

if gcloud storage buckets describe "$BUCKET" >/dev/null 2>&1; then
  echo "bucket $BUCKET already exists"
else
  gcloud storage buckets create "$BUCKET" --location="$REGION" --uniform-bucket-level-access
  scripts/log_time.sh CREATE "bucket/${BUCKET#gs://}"
fi
echo "setup done. Next: scripts/run_stage1.sh sample"
