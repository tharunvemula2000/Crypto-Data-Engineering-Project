# Crypto-Data-Engineering-Project


This project builds a complete end-to-end **real-time data engineering pipeline** to ingest, process, store, and visualize cryptocurrency market data.

---

## 📌 Project Overview

We collect real-time data from the **CoinGecko API**, process and transform it using **AWS Glue (Apache Spark)**, store it in a **Snowflake data warehouse**, orchestrate the entire pipeline using **Apache Airflow**, and visualize key metrics with **Power BI**.

---

## 🛠️ Tech Stack

| Tool/Platform | Purpose |
|---------------|---------|
| **Python** | API ingestion, scripting |
| **AWS S3** | Raw and processed data storage |
| **AWS Glue (Spark Jobs)** | ETL: Clean, transform, and load data |
| **Snowflake** | Cloud data warehouse |
| **Apache Airflow** | Pipeline orchestration |
| **Power BI** | Data visualization |

---

## 🔁 Pipeline Architecture

1. **Ingest**
   - Fetch crypto market data using the CoinGecko API with a Python script.
   - Store raw JSON data in AWS S3.

2. **ETL with AWS Glue**
   - **Job 1**: Clean and transform raw data into staging Parquet files (dimensions and facts) in S3.
   - **Job 2**: Load transformed Parquet data into Snowflake tables (`dim_coin`, `fact_market_data`).

3. **Snowflake Warehouse**
   - Create structured tables to support analytical queries.
   - Perform SQL-based transformations if needed.

4. **Airflow DAG**
   - Automates all steps: API fetch → S3 write → Glue job → Snowflake load.

5. **Power BI Dashboard**
   - Import mode to manually refresh data from Snowflake.
   - Visualizations: Top Coins, Market Cap, Price Trends, Volume, etc.

---

## 📂 Project Structure

