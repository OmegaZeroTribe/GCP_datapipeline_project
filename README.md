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
