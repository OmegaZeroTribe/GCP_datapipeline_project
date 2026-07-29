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
* **Structure-Bucket-in-GCS:**
![Alt text for accessibility](https://github.com/OmegaZeroTribe/GCP_datapipeline_project/blob/6eb93079e229c09f58d6bbfa43696b23d9ac7560/gcs.png)
* **DataFlow Template**
![Alt text for accessibility](https://github.com/OmegaZeroTribe/GCP_datapipeline_project/blob/41799256653c93741e46dceeaeb1d2422d2df44b/dataflow_template.png)
  * Using the Google-managed **CSV Files on Cloud Storage to BigQuery** Dataflow template to read sales & customer data (.CSV file) and JSON (.schema)
  * DataFlow Execution Command: https://github.com/OmegaZeroTribe/GCP_datapipeline_project/blob/a65ed20a804ce3b4a59fac3a1f315a82d756f184/dataflow_deploy/import-sales-record.sh
* **DataFlow Pipeline**
![Alt text for accessibility](https://github.com/OmegaZeroTribe/GCP_datapipeline_project/blob/cb62ad07dab32135fc461bdda1552f550ed2fa80/dataflow_pipeline.png)
* **Result Table Loaded to Bigquery**
![Alt text for accessibility](https://github.com/OmegaZeroTribe/GCP_datapipeline_project/blob/f5066099e9675314d873a11041d2c1c2c6167f2d/result_load_bq.png)

### 2. Cleansing&TransForm: BigQuery & DataForm (TransForm)
* **DataForm SQLX:** use SQLX script to cleansing & transform data
![Alt text for accessibility](https://github.com/OmegaZeroTribe/GCP_datapipeline_project/blob/7ebe23dd0b7ae5d067ef78dbe6cf5ef71dceddc0/dataform_dept.png)
  * **SQLX sales_enriched:**
```sql
config {
  type: "table",
  schema: "sales_analyst",
  name: "sales_enriched",
  description: "Detailed sales transactions enriched with customer, product, and store master data."
}

WITH deduplicated_sales AS (
  SELECT
    transaction_id,
    transaction_date,
    customer_id,
    product_id,
    store_id,
    quantity,
    unit_price,
    gross_amount,
    discount_pct,
    discount_amount,
    net_amount,
    payment_method,
    -- Keep only the most recent entry if duplicate transaction_ids exist
    ROW_NUMBER() OVER (
      PARTITION BY transaction_id 
      ORDER BY transaction_date DESC
    ) AS row_num
  FROM
    ${ref("sales_record")}
  QUALIFY row_num = 1
)

SELECT
  -- Transaction Attributes
  s.transaction_id,
  s.transaction_date,
  s.payment_method,

  -- Customer Attributes
  s.customer_id,
  c.customer_name,
  c.email AS customer_email,
  c.city AS customer_city,
  c.state AS customer_state,

  -- Product Attributes
  s.product_id,
  p.product_name,
  p.category AS product_category,
  p.cost AS unit_cost,

  -- Store Attributes
  s.store_id,
  st.store_name,
  st.region AS store_region,
  st.store_type,

  -- Metrics
  s.quantity,
  s.unit_price,
  s.gross_amount,
  s.discount_pct,
  s.discount_amount,
  s.net_amount,
  (s.net_amount - (p.cost * s.quantity)) AS profit_amount

FROM
  deduplicated_sales AS s

LEFT JOIN
  ${ref("master_customers")} AS c
  ON s.customer_id = c.customer_id

LEFT JOIN
  ${ref("master_products")} AS p
  ON s.product_id = p.product_id

LEFT JOIN
  ${ref("customer_stores")} AS st
  ON s.store_id = st.store_id
```

  * **SQLX sales_agg:**
```sql
config {
  type: "table",
  schema: "sales_analyst",
  name: "sales_agg",
  description: "Monthly aggregation showing the top-selling product by net revenue for each month."
}

-- Step 1: Group & Aggregate raw sales data by month
WITH monthly_sales AS (
  SELECT
    DATE_TRUNC(s.transaction_date, MONTH) AS sales_month,
    s.product_id,
    s.product_name,
    s.product_category,
    COUNT(DISTINCT s.transaction_id) AS total_transactions,
    SUM(s.quantity) AS total_units_sold,
    SUM(s.net_amount) AS total_net_revenue,
    SUM(s.profit_amount) AS total_profit
  FROM
    ${ref("sales_enriched")} AS s
  GROUP BY
    1, 2, 3, 4
),

-- Step 2: Rank products per month using the aggregated result
ranked_sales AS (
  SELECT
    sales_month,
    product_id,
    product_name,
    product_category,
    total_transactions,
    total_units_sold,
    total_net_revenue,
    total_profit,
    ROW_NUMBER() OVER (
      PARTITION BY sales_month
      ORDER BY total_net_revenue DESC
    ) AS sales_rank
  FROM
    monthly_sales
)

-- Step 3: Extract the top-selling product for each month
SELECT
  sales_month,
  product_id,
  product_name,
  product_category,
  total_transactions,
  total_units_sold,
  total_net_revenue,
  total_profit
FROM
  ranked_sales
WHERE
  sales_rank = 1
ORDER BY
  sales_month DESC
```
