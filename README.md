PySpark & Airflow ETL Pipeline (Bronze, Silver, Gold Layers)

This project implements a robust, scalable Extract, Transform, Load (ETL) pipeline using PySpark for efficient data processing and Apache Airflow for workflow orchestration. It follows a modern data layering approach (Bronze, Silver, Gold) to manage data quality and complexity, ultimately serving business intelligence needs via PostgreSQL and Power BI.

The entire environment is containerized using Docker (via the Astro CLI) for local development and reliable execution.

🧱 Architecture Overview

The system is designed around a three-tier data lakehouse/data warehouse structure, ensuring raw data immutability, clean data standardization, and optimized business-ready aggregation.

Key Components:

Data Sources: Multiple input CSV files are consumed as raw data.

Orchestration (Apache Airflow): Manages the entire pipeline flow, ensuring tasks (PySpark jobs) execute in the correct sequence (Bronze ingestion -> Silver cleansing -> Gold aggregation).

Processing (PySpark / Spark Cluster): Performs all data transformations, cleaning, and aggregation logic.

Data Storage: A combination of Parquet files (for scalable, immutable storage in Bronze/Silver) and PostgreSQL (for query-optimized, business-ready data in Gold).

Infrastructure (Docker / Astro CLI): Provides a containerized, reproducible environment for Airflow, Spark, and PostgreSQL.

Consumption (Power BI): Connects to the Gold Layer (PostgreSQL) to build reports and dashboards.

📊 Data Layering Strategy

Layer

Purpose

Key Transformations

Storage

Bronze

Raw Data Ingestion

No transformations. Data is loaded as-is from CSV into a durable format.

Parquet Files

Silver

Cleaned & Standardized

Data cleaning, type casting, handling nulls/duplicates, and basic standardization.

Parquet Files

Gold

Business-Ready

Feature engineering, aggregations, joining dimensions, and creating summarized tables optimized for BI queries.

PostgreSQL

⚙️ Technologies Used

Orchestration: Apache Airflow

Processing: PySpark (running on a local Spark Cluster)

Database: PostgreSQL

Containerization: Docker (managed via the Astro CLI)

Infrastructure: Linux VM

Visualization: Power BI (External Tool)
