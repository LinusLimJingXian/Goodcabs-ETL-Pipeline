# 🚖 Goodcabs ETL Pipeline

A production-grade streaming data pipeline built on Databricks, processing ride-trip data across 10 Indian cities into a clean, analytics-ready **Medallion architecture (Bronze → Silver → Gold)**.

---

## **Overview**

**Goodcabs** is a fictional ride-hailing company operating across tier-2 cities in India.

This pipeline:
- Ingests raw trip and city data from AWS S3  
- Transforms it across three structured layers  
- Delivers an enriched **Gold layer** ready for BI and analytics  

**Data Coverage:** August 2025 – December 2025  
**Scope:** 10 cities across India  

---

## **Architecture**

**Data Flow:**

- **AWS S3 (CSV files)**  
  → Raw data source  

- **Bronze Layer**  
  → Streaming ingestion via Auto Loader  

- **Silver Layer**  
  → Cleaned, validated, and standardised (CDC + SCD Type 1)  

- **Gold Layer**  
  → Enriched SQL views for analytics and BI consumption  

---

## **Tech Stack**

| Tool                  | Purpose                                      |
|------------------------|----------------------------------------------|
| Databricks            | Pipeline orchestration and compute           |
| PySpark               | Data transformation                         |
| Delta Lake            | Storage with ACID transactions & CDC         |
| AWS S3                | Raw data source (CSV files)                 |
| Auto Loader           | Streaming ingestion from S3                 |
| SQL                   | Gold layer view definitions                 |
| Databricks Pipelines  | Declarative pipeline framework              |

---

## **Pipeline Layers**

### **Bronze — Raw Ingestion**

- Trips ingested via **Auto Loader (streaming)**  
- Automatically detects new CSV files in S3  
- City data ingested as a batch materialized view  
- Schema inferred automatically (with rescue column for malformed data)  

**Metadata Columns Added:**
- `file_name` (source S3 path)  
- `ingest_datetime`  

**Delta Lake Features:**
- Change Data Feed enabled  
- Auto-optimize & auto-compact  

---

### **Silver — Cleaning & Validation**

#### **Trips**
- Columns renamed and standardised  
- Data types cast appropriately  
- Streamed into staging view with data quality checks:

**Expectations:**
- `valid_date` → year ≥ 2025  
- `valid_driver_rating` → between 1 and 10  
- `valid_passenger_rating` → between 1 and 10  

- CDC upsert using `create_auto_cdc_flow` (SCD Type 1)  
- Prevents duplicate trips on re-ingestion  

#### **City**
- Cleaned and standardised  
- Processing timestamps tracked  

#### **Calendar Dimension**
- Generated programmatically across full date range  

**Includes:**
- Time attributes: month, quarter, week, day  
- Flags: `is_weekday`, `is_weekend`  
- Indian holidays:
  - Republic Day  
  - Independence Day  
  - Gandhi Jayanti  

---

### **Gold — Analytics-Ready Views**

- SQL views joining:
  - Trips  
  - City  
  - Calendar  

- One view per city with enriched trip data  
- Fully time-aware dataset  

**Ready for:**
- Power BI  
- Tableau  
- Looker  
- SQL-based analytics  

---

## **Data Quality**

Data quality is enforced at the **Silver layer** using Databricks expectations (`@dp.expect`).

- Invalid records are **flagged and tracked**  
- Only clean, validated data flows into the Gold layer  

---

## **Project Structure**

**transportation_pipeline/**
- **bronze/** — Raw ingestion  
  - `city_bronze.py`  
  - `trips_bronze.py`  

- **silver/** — Cleaning & validation  
  - `city_silver.py`  
  - `trips_silver.py`  
  - `calendar_silver.py`  

- **gold/** — Analytics-ready views  
  - SQL views per city  

**project_setup.ipynb** — Catalog & schema initialization

---

## **Setup**

### **Prerequisites**

- Databricks workspace (Unity Catalog enabled)  
- AWS S3 bucket with trip & city CSV files  
- Databricks cluster with PySpark runtime  

---

### **Steps**

1. Run `project_setup.ipynb`  
   → Initializes catalog and schemas (bronze, silver, gold)  

2. Configure S3 source paths  
   → Update paths in Bronze layer scripts  

3. Deploy the pipeline  
   → Use Databricks Pipelines  

4. Access Gold layer views  
   → Available after first successful run  
