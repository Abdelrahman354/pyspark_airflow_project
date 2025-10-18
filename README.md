# PySpark Airflow Data Pipeline Project

## 🎯 Project Overview

This project implements a comprehensive **Data Lakehouse Architecture** using Apache Airflow for orchestration and Apache Spark for data processing. The pipeline processes CSV data through a medallion architecture (Bronze, Silver, Gold layers) and provides business-ready analytics via Power BI dashboards.

## 🏗️ Architecture

![Project Architecture](https://github.com/Abdelrahman354/pyspark_airflow_project/blob/main/Project_Architecture.png?raw=true)




### Architecture Overview

The project follows a **three-tier medallion architecture** pattern:

```
CSV Files → Bronze Layer → Silver Layer → Gold Layer → Power BI
              (Raw)      (Cleaned)     (Business-Ready)
```

### Architecture Components

- **Data Sources**: Multiple CSV files containing raw data
- **Bronze Layer**: Raw data ingestion with no transformations (Parquet storage)
- **Silver Layer**: Cleaned and standardized data (Parquet storage)
- **Gold Layer**: Business-ready aggregated data (PostgreSQL storage)
- **Processing Engine**: Apache Spark Cluster for distributed data processing
- **Orchestration**: Apache Airflow for workflow management
- **Visualization**: Power BI for business intelligence dashboards
- **Infrastructure**: Docker containers via Astro CLI for Apache Airflow


## 🛠️ Technology Stack

| Component | Technology |
|-----------|-----------|
| **Orchestration** | Apache Airflow |
| **Data Processing** | Apache Spark (PySpark) |
| **Storage - Bronze/Silver** | Parquet Files |
| **Storage - Gold** | PostgreSQL |
| **Container Runtime** | Docker |
| **Deployment** | Astro CLI (Astronomer) |
| **Visualization** | Power BI |
| **Infrastructure** | Linux VM |

## 📊 Data Pipeline Layers

### 🥉 Bronze Layer (Raw Data)
- **Purpose**: Ingest raw data from CSV files
- **Transformations**: None
- **Storage Format**: Parquet
- **Key Features**:
  - Preserves original data structure
  - Creates immutable raw data archive
  - Enables data reprocessing if needed

### 🥈 Silver Layer (Cleaned Data)
- **Purpose**: Clean, standardize, and validate data
- **Transformations**:
  - Data type conversions
  - Null value handling
  - Data deduplication
  - Schema standardization
  - Data quality checks
- **Storage Format**: Parquet
- **Key Features**:
  - Optimized for query performance
  - Partitioned data storage
  - Validated data quality

### 🥇 Gold Layer (Business-Ready Data)
- **Purpose**: Provide aggregated, business-ready datasets
- **Transformations**:
  - Business metrics calculation
  - Data aggregations
  - Dimension modeling
  - Feature engineering
- **Storage Format**: PostgreSQL Database
- **Key Features**:
  - Optimized for BI tools
  - Pre-calculated metrics
  - Ready for visualization

## 📁 Project Structure

```
pyspark_airflow_project/
│
├── dags/                      # Airflow DAG definitions
│   └── ETL_dag.py   
│
├── include/                      # Data storage
│   ├── raw/                   # CSV source files
│   ├── bronze/                # Bronze layer parquet files
│   ├── silver/                # Silver layer parquet files
│   └──scripts/                   # PySpark processing scripts
│       ├── bronze_layer.py   # Bronze layer transformations
│       ├── silver_layer.py   # Silver layer transformations
│       └── gold_layer.py     # Gold layer transformations
│
│
├── docker-compose.yml         # Docker services definition
├── Dockerfile                 # Airflow custom image
├── requirements.txt           # Python dependencies
└── README.md                  # This file
```

## 🔄 Pipeline Workflow

### DAG Execution Flow

```
trigger → bronze_layer_dag → silver_layer_dag → gold_layer_dag → Power BI Refresh
```

1. **Bronze Layer DAG**
   - Reads CSV files from source directory
   - Converts to Parquet format
   - Stores in Bronze layer

2. **Silver Layer DAG**
   - Reads Bronze layer Parquet files
   - Applies data cleaning transformations
   - Validates data quality
   - Stores cleaned data in Silver layer

3. **Gold Layer DAG**
   - Reads Silver layer data
   - Performs business aggregations
   - Calculates KPIs and metrics
   - Loads data into PostgreSQL

4. **Power BI**
   - Connects to PostgreSQL Gold layer
   - Visualizes business metrics
   - Provides interactive dashboards

## 📊 Data Quality Checks

The pipeline includes automated data quality validations:
- Schema validation
- Null value checks
- Data type validation
- Duplicate detection
- Business rule validation

## 🎯 Key Features

✅ **Scalable Architecture**: Distributed processing using Spark cluster
✅ **Data Quality**: Built-in validation at each layer
✅ **Reproducible**: Immutable Bronze layer enables reprocessing
✅ **Automated**: Airflow orchestrates all workflows
✅ **Containerized**: Easy deployment using Docker and Astro CLI
✅ **BI-Ready**: Optimized Gold layer for fast analytical queries

## 📖 Resources

- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [Astro CLI Documentation](https://docs.astronomer.io/astro/cli/overview)
- [Medallion Architecture Pattern](https://www.databricks.com/glossary/medallion-architecture)

## 👤 Author

**Abdelrahman**
- GitHub: [@Abdelrahman354](https://github.com/Abdelrahman354)
- Linkedin [@Abdelrahman Ahmed ](https://www.linkedin.com/in/abdelrahman-ahmed-32983a22a)

---

⭐ **Star this repository if you find it helpful!**
