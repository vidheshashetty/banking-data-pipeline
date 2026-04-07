# Banking Data Pipeline (End-to-End Data Engineering Project)

## Project Overview

This project simulates an end-to-end banking transaction data pipeline built using modern data engineering tools.

The pipeline generates synthetic banking transactions, stores raw data in Amazon S3 (data lake), processes data using PySpark on AWS Glue, performs data validation and transformation, and loads curated data into Snowflake for analytics.

Workflow orchestration is handled using Apache Airflow, and data quality checks are implemented using both SQL validation logic and Great Expectations.

A basic CI/CD pipeline using GitHub Actions is integrated to automate deployment of workflows and pipeline components.

The project demonstrates how a production-grade ETL pipeline can be designed with data validation, scalability, observability, and cloud-native architecture.

---

## Architecture

The pipeline architecture consists of the following components:

1. Synthetic Data Generation
2. Amazon S3 (Raw Data Layer)
3. AWS Glue (PySpark Processing Layer)
4. Amazon S3 (Processed Data Layer)
5. Snowflake (Curated Data Warehouse)
6. Data Validation Layer
7. Workflow Orchestration (Apache Airflow)
8. CI/CD Pipeline (GitHub Actions)
9. Monitoring & Logging (AWS CloudWatch + Airflow)

Architecture Diagram:

![Architecture Diagram](diagrams/architecture_diagram.png)

---

## DAG Workflow

The pipeline workflow is orchestrated using Apache Airflow.

Pipeline Flow:

![DAG Flow](diagrams/DAG_flow_diagram.png)

Main DAG Tasks:

1. **load_raw**
   - Generates synthetic banking transaction data
   - Stores raw transaction data in Amazon S3 (raw zone)

2. **clean_data**
   - Triggers AWS Glue PySpark job
   - Performs:
      - Data cleaning
      - Deduplication using window functions
      - Transformation and filtering
      - Writes processed data to S3 (processed zone)

3. **move_failed**
   - Identifies invalid records
   - Stores failed records in S3 (failed zone) for debugging

4. **validate_and_log**
   - Calculates validation metrics
   - Logs pipeline execution statistics into Snowflake (PIPELINE_RUN_LOG)
   - Sends logs to AWS CloudWatch
   - Determines pipeline health status

5. **gx_checkpoint**
   - Executes Great Expectations validation
   - Validates processed data from S3/Snowflake
   - Generates data quality reports

6. **mark_processed**
   - Marks processed records to maintain pipeline consistency

---

## Data Validation

Data quality checks are implemented at two levels:

### SQL Validation

Rules applied:

- Transaction ID must not be NULL
- Amount must be greater than 0
- Transaction type must be DEBIT or CREDIT
- Transaction timestamp cannot be in the future

Invalid records are moved to the failed data layer (S3).

---

### Great Expectations Validation

Great Expectations is used to perform additional validation on processed datasets.

It provides:

- Expectation suites
- Data validation reports
- Data quality monitoring

Validation is executed via a GX checkpoint after PySpark processing.

---

## Pipeline Monitoring

Pipeline execution metrics are stored in:

PIPELINE_RUN_LOG (Snowflake)

Metrics captured:
- Total records processed
- Passed records
- Failed records
- Validation status
- Error category
- Execution time
- Great Expectations validation status

Monitoring Tools:
- AWS CloudWatch for Glue job logs
- Airflow logs for orchestration tracking

Email alerts are configured for pipeline failures.

---

## Technologies Used

- Python
- PySpark
- Apache Airflow
- AWS S3
- AWS Glue
- Snowflake
- Great Expectations
- SQL
- GitHub Actions (CI/CD)
- Linux

---

## Project Structure

banking-data-pipeline/

├── dags/
│   └── banking_pipeline_dag.py
│
├── glue_jobs/
│   └── pyspark_job.py
│
├── gx/
│   ├── great_expectations.yml
│   ├── expectations/
│   ├── checkpoints/
│
├── .github/
│   └── workflows/
│       └── ci_cd_pipeline.yml
│
├── diagrams/
│   ├── architecture_diagram.png
│   └── DAG_flow_diagram.png
│
├── add_expectations.py
│
└── README.md

---

## Key Features

- End-to-End Data Pipeline
- Data Lake Architecture using S3
- Distributed Data Processing using PySpark
- Airflow DAG Orchestration
- Data Validation Layer (SQL + Great Expectations)
- Failure Handling & Monitoring
- Pipeline Execution Logging
- CI/CD Pipeline Integration
