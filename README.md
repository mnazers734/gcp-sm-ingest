# GCP Customer Data Ingestion

This repository contains the **event-driven ETL system** that ingests customer/shop management data into MySQL on Google Cloud.  
The system is built using **Cloud Run, Eventarc, Cloud Storage, Secret Manager, and VPC connectors**.

---

## ⚙️ Architecture Overview
1. Partner uploads CSV files + `manifest.json` to Cloud Storage.  
2. **Eventarc** triggers the **Dispatcher** service when `manifest.json` finalizes.  
3. Dispatcher validates the manifest and launches the **Loader** job.  
4. Loader:
   - Validates file integrity (row counts, SHA-256 checksums).  
   - Loads data into staging tables.  
   - Applies crosswalks for stable IDs.  
   - Upserts into production tables in dependency order.  
   - Records results in a `load_ledger` table for auditing.  

---

## 🚀 Key Features
- **Serverless** – runs fully on managed GCP services.  
- **Event-driven** – ingestion starts automatically when manifest is uploaded.  
- **ETL pipeline** – Extract → Transform → Load customer data.  
- **Idempotent & auditable** – safe to retry; full ledger of loads.  
- **Mono-repo** – infra, service, and job code live side by side for consistency.  

---

## 🛠️ Getting Started
1. Clone this repository.
2. Create and activate a Python virtual environment.
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
