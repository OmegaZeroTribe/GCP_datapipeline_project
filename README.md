# GCP_datapipeline_project

## 📌 Project Overview
An end-to-end serverless data processing pipeline built on Google Cloud Platform. This project ingests raw CSV files Sales records stored in **Google Cloud Storage (GCS)**, loading to **BigQuery** using **Apache Beam on Dataflow**, and transform analytics-ready data into partitioned **BigQuery** tables using **DataForm**.

### Key Objectives
* **Data Processing:** Clean, parse, and validate schema variations from GCS files.
* **Dead Letter Queue (DLQ):** Route corrupted/invalid records into a GCS DLQ bucket for debugging without breaking pipeline execution.
* **Warehouse Optimization:** Load data into BigQuery with **partitioning by event date** and **clustering by customer category** to minimize query costs.

---

## 🏗️ Architecture Diagram
![Alt text for accessibility](https://github.com/OmegaZeroTribe/GCP_datapipeline_project/blob/bb8c8b5c2be6dee34bcb4cbf2f362b170abacff5/architecture.png)

---

## 🛠️ Tech Stack & GCP Services
* **Storage:** Google Cloud Storage (GCS)
* **Loading** Apache Beam (Java SDK) , Dataflow pipeline (CSV file to BigQuery Template)
* **Processing:** BigQuery DataForm
* **Data Warehouse:** Google BigQuery
* **Sercurity&Permission:** Cloud IAM (Grant Permission Role to Service Account)

## 🔒 Security & IAM Configuration (Least Privilege)

To ensure secure execution without using full Admin privileges, a dedicated Service Account (`dataflow@<PROJECT_ID>.iam.gserviceaccount.com`) is provisioned with granular roles:
![Alt text for accessibility](https://github.com/OmegaZeroTribe/GCP_datapipeline_project/blob/1acd8de81a9687ca8078f0b56e1309b4e4e85dec/service_account.png)

| GCP Component | Role Granted | Purpose |
|---|---|---|
| **Cloud Storage** | `roles/storage.objectViewer` | Read raw CSV files & Dataflow staging scripts |
| **Dataflow** | `roles/dataflow.worker` | Execute compute tasks on worker VMs |
| **BigQuery** | `roles/bigquery.dataEditor` | Insert records into `sales_anlyst.sales_record` |
| **Dataform** | `roles/dataform.editor` | Run SQLX transformations & data assertions |

## ⚡ Pipeline Pipeline Workflow & Key Steps

### 1. Ingestion: Dataflow Template Execution (Extract & Load)
* **Generate-MockUpdate:** use Python script https://github.com/OmegaZeroTribe/GCP_datapipeline_project/blob/05701d4e022669a1c6a85f13de8655ee225b54a3/generate_scipt/mockup_data.ipynb
* Using the Google-managed **CSV Files on Cloud Storage to BigQuery** Dataflow template to read sales & customer data (.CSV file) and JSON (.schema)
* **Structure-Bucket-in-GCS:**
![Alt text for accessibility](https://github.com/OmegaZeroTribe/GCP_datapipeline_project/blob/6eb93079e229c09f58d6bbfa43696b23d9ac7560/gcs.png)

