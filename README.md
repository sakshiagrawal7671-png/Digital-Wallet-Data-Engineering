# Digital Wallet Transaction DB
## Data Engineering Capstone Project – 30 Advanced Tasks

![Python](https://img.shields.io/badge/Python-3.10+-blue)
![Pandas](https://img.shields.io/badge/Pandas-2.x-green)
![SQL](https://img.shields.io/badge/SQL-SQLite-orange)
![Status](https://img.shields.io/badge/Status-Complete-brightgreen)

---

## Project Overview

A complete, end-to-end Data Engineering project built around a **Digital Wallet Transaction System**.  
100,000 synthetic transactions (1,000 users · 15 merchants · 8 categories · full year 2023) demonstrate all 30 tasks across the data engineering lifecycle.

---

## Quick Start

```bash
# 1. Clone the repo
git clone https://github.com/<your-username>/digital-wallet-de-project.git
cd digital-wallet-de-project

# 2. Install dependencies
pip install -r requirements.txt

# 3. Generate the dataset (run this FIRST)
python3 data/generate_data.py

# 4. Run any individual task
python3 task06_sql_basics/sql_basics.py

# 5. Run the full end-to-end pipeline
python3 task30_final_project/end_to_end_pipeline.py
```

---

## Directory Structure

```
digital-wallet-de-project/
├── data/
│   ├── generate_data.py          ← Run first! Generates all CSV files
│   ├── transactions.csv          ← 100,000 rows (auto-generated)
│   ├── transactions_2023_01.csv  ← Monthly slices
│   ├── transactions_2023_02.csv
│   ├── transactions_2023_03.csv
│   └── users.csv                 ← 1,000 users
│
├── task01_linux/
│   └── pipeline_setup.sh         ← Bash: dirs, permissions, file movement, logging
├── task02_networking/
│   └── networking_simulation.py  ← HTTP/HTTPS/FTP protocol analysis, packet sim
├── task03_python_basics/
│   └── csv_pipeline.py           ← Read, clean, merge multiple CSVs
├── task04_advanced_python/
│   └── transformation_modules.py ← Normalizer, Aggregator, Validator classes
├── task05_pandas_numpy/
│   └── large_scale_analysis.py   ← 1M+ rows, 62% memory saving, 66× NumPy speedup
├── task06_sql_basics/
│   └── sql_basics.py             ← SQLite DB, 8 SQL query types
├── task07_advanced_sql/
│   └── advanced_sql.py           ← JOINs, subqueries, CTEs, window functions
├── task08_db_concepts/
│   └── tasks_08_to_11.py         ← OLTP/OLAP, Star Schema, ETL vs ELT, Batch Ingest
├── task12_hadoop/
│   └── hdfs_simulation.py        ← NameNode, DataNodes, 3× replication (Tasks 12-13)
├── task14_spark_basics/
│   └── spark_tasks_14_to_17.py   ← Spark API sim: jobs, DataFrames, SQL, optimisation
├── task18_streaming_concepts/
│   └── streaming_kafka_tasks_18_to_21.py  ← Streaming, Kafka, Structured Streaming
├── task22_airflow_basics/
│   └── airflow_tasks_22_to_23.py ← DAG framework, retries, scheduling, monitoring
├── task24_cloud_basics/
│   └── cloud_tasks_24_to_27.py   ← AWS/GCP/Azure, S3, EC2, BigQuery (Tasks 24-27)
├── task28_lakehouse/
│   └── lakehouse_and_dq_tasks_28_29.py  ← Delta Lake versioning + DQ (Tasks 28-29)
├── task30_final_project/
│   └── end_to_end_pipeline.py    ← Full pipeline: Ingest→Process→Store→Orch→Dashboard
│
├── requirements.txt
└── README.md
```

---

## Task Summary

| # | Task | Key Output |
|---|------|-----------|
| 01 | Linux + File System | Bash script, dirs, permissions, archive, log |
| 02 | Networking | HTTP/HTTPS/FTP analysis, packet capture sim |
| 03 | Python Basics | Multi-CSV read, clean, merge → `merged_clean.csv` |
| 04 | Advanced Python | `Normalizer`, `Aggregator`, `Validator` OOP modules |
| 05 | Pandas + NumPy | 1.1M rows · 62% memory saved · 66× NumPy speedup |
| 06 | SQL Basics | SQLite with 100K rows, 8 query types |
| 07 | Advanced SQL | JOINs, CTEs, RANK/LAG/running total windows |
| 08 | DB Concepts | OLTP vs OLAP comparison + timed queries |
| 09 | Data Warehouse | Star schema: 1 fact + 5 dimension tables |
| 10 | ETL vs ELT | Both pipelines built + benchmarked |
| 11 | Data Ingestion | Batch pipeline: 111K rows/sec, checksums, audit log |
| 12 | Hadoop | HDFS cluster sim: NameNode + 4 DataNodes |
| 13 | HDFS Arch | Block map, replication, fsck, round-trip verify |
| 14 | Spark Basics | Spark job on 100K rows via pandas API |
| 15 | Spark DataFrames | Transformations + actions (groupBy, agg) |
| 16 | Spark SQL | 4 analytical SQL queries against full dataset |
| 17 | PySpark Advanced | Partitioning, caching, broadcast join |
| 18 | Streaming Concepts | Batch vs stream, use cases, architecture |
| 19 | Kafka Basics | Producer-consumer with 4 partitions |
| 20 | Kafka Advanced | Offset management, replay, delivery semantics |
| 21 | Structured Streaming | Micro-batch processing, fraud alerts, watermarks |
| 22 | Airflow Basics | 7-task DAG with XCom + decorator pattern |
| 23 | Airflow Advanced | Scheduling, retries, monitoring, backfill |
| 24 | Cloud Basics | AWS vs GCP vs Azure service comparison |
| 25 | Cloud Storage | S3 bucket sim: PUT/GET/LS/presigned URL |
| 26 | Cloud Compute | EC2 launch + pipeline deployment walkthrough |
| 27 | Cloud DW | BigQuery/Redshift SQL: quarterly, city, KYC |
| 28 | Lakehouse | Delta Lake: ACID, versioning, time travel, vacuum |
| 29 | Data Quality | Z-score, IQR, behavioural, velocity anomalies |
| 30 | Final Project | End-to-end pipeline → live analytics dashboard |

---

## Key Results

| Metric | Value |
|--------|-------|
| Total Transactions | 100,000 |
| Total Revenue (INR) | ₹2,493.70M |
| Success Rate | 75.08% |
| Average Txn Value | ₹24,936.97 |
| Top Category | Utilities (₹237.82M) |
| Top City | Ahmedabad (₹216.06M) |
| Pipeline Throughput | 111,301 rows/sec |
| NumPy Speedup | 66× vs Python loop |
| Memory Saved (Pandas) | 62.1% |
| Data Quality Score | 100% (EXCELLENT) |
| Pipeline Duration | ~4 seconds (end-to-end) |

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Language | Python 3.10+ |
| Data Processing | Pandas, NumPy |
| Storage (OLTP) | SQLite |
| Big Data | PySpark (simulated via pandas API) |
| Streaming | Kafka simulation (Python queues) |
| Orchestration | Airflow DAG (custom Python framework) |
| Cloud | AWS S3 / EC2 / BigQuery simulation |
| Lakehouse | Delta Lake simulation |
| Shell | Bash |

---

## Requirements

```
pandas>=2.0.0
numpy>=1.24.0
```
All other imports are Python standard library. No Java or external services required.

---

## Author

**[Your Name]**  
Roll Number: [Your Roll Number]  
Batch / Program: [Your Batch]  
Submission: April 2026
