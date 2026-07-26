# GCP_datapipeline_project

## 📌 Project Overview
An end-to-end serverless data processing pipeline built on Google Cloud Platform. This project ingests raw CSV files Sales records stored in **Google Cloud Storage (GCS)**, loading to **BigQuery** using **Apache Beam on Dataflow**, and transform analytics-ready data into partitioned **BigQuery** tables using **DataForm**.

### Key Objectives
* **Data Processing:** Clean, parse, and validate schema variations from GCS files.
* **Dead Letter Queue (DLQ):** Route corrupted/invalid records into a GCS DLQ bucket for debugging without breaking pipeline execution.
* **Warehouse Optimization:** Load data into BigQuery with **partitioning by event date** and **clustering by customer category** to minimize query costs.

---

## 🏗️ Architecture Diagram
![Alt text for accessibility](https://github.com/OmegaZeroTribe/GCP_datapipeline_project/blob/c819f6f8aef625423f05570f679f379addebc76b/architecture.png)
