# Fraud Detection – End-to-End MLOps Project

## Overview
Production-grade fraud detection system built using modern MLOps practices.
The pipeline starts from a predefined AWS S3 data contract and automates
training, deployment, monitoring, and business reporting.

## Key Components
- Weekly data ingestion using Airflow
- Schema & data quality validation
- Feature engineering and model training
- Model versioning with DVC
- FastAPI inference service
- Docker + Kubernetes deployment
- Monitoring with Grafana
- Business dashboards with Power BI

## Scope
This project does NOT include upstream data engineering.
Data is assumed to be delivered weekly to an AWS S3 data lake
with a fixed schema.

## Tech Stack
Python, Airflow, DVC, MLflow, FastAPI, Docker, Kubernetes, AWS, Grafana
