# 🦠 Real-Time COVID-19 Data Processing & Analytics Platform

> End-to-End Real-Time Data Engineering Project  
> Built with Kafka, Apache NiFi, Apache Spark Structured Streaming, MySQL, Docker, and Power BI

---

## Project Overview

This project implements a real-time, scalable, and production-ready data engineering pipeline for ingesting, processing, analyzing, and visualizing COVID-19 data.

The system continuously processes COVID-19 statistics such as confirmed cases, new cases, deaths, recovery rates, and active cases, and produces real-time analytics, hotspot detection, and future trend predictions.

The processed data is stored in a relational database and exposed through Power BI dashboards for business intelligence and decision-making.

---

## Business Use Case

The platform enables:
- Real-time monitoring of COVID-19 spread
- Trend analysis across countries and continents
- Hotspot detection for critical regions
- Short-term forecasting of cases and deaths
- Executive dashboards for data-driven decision-making

---

## High-Level Architecture

CSV Data Source (OWID)  
↓  
Kafka Producer (Python)  
↓  
Kafka Topic: covid19_raw  
↓  
Apache NiFi (ETL & Enrichment)  
↓  
Kafka Topic: covid19_processed  
↓  
Apache Spark Structured Streaming  
↓  
Analytics & Predictions  
↓  
MySQL (Serving Layer)  
↓  
Power BI Dashboards

---

## Tech Stack

### Data Ingestion & Streaming
- Apache Kafka
- Zookeeper
- Kafka UI

### ETL & Data Flow
- Apache NiFi

### Real-Time Processing & Analytics
- Apache Spark 3.5 (Structured Streaming)
- PySpark

### Storage
- MySQL 8
- JDBC Integration

### Visualization
- Power BI

### Infrastructure & Deployment
- Docker
- Docker Compose

---

## Project Structure
```
.
├── docker-compose.yml  
├── init-db/  
│   └── SQL initialization scripts  
├── producer/  
│   ├── Dockerfile  
│   ├── covid_producer.py  
│   └── owid-covid-data.csv  
├── spark/  
│   ├── Dockerfile  
│   ├── covid_streaming.py  
│   ├── entrypoint.sh  
│   └── requirements.txt  
└── README.md  
```
---

## Data Pipeline Flow

### 1. Data Ingestion
- COVID-19 data is read from the OWID CSV dataset
- A Python Kafka Producer publishes records to Kafka
- Real-time behavior is simulated using controlled delays

Kafka Topic:
covid19_raw

---

### 2. ETL with Apache NiFi
NiFi is responsible for:
- Data cleaning
- Schema normalization
- Type casting
- Derived metrics generation
- Hotspot detection

Output Kafka Topic:
covid19_processed

---

### 3. Real-Time Processing with Spark Streaming

Spark Structured Streaming consumes processed data and performs:

- Data cleaning and validation
- Deduplication
- Real-time metric calculations
- Window-based analytics (7-day and 14-day rolling averages)
- Growth rate and trend direction detection
- Rule-based short-term predictions
- Prediction confidence scoring

---

### 4. Data Storage (Serving Layer)

Processed analytics are stored in MySQL tables:

- covid_realtime_stats: Country-level real-time statistics
- covid_predictions: Short-term future predictions
- continent_covid_stats: Aggregated continent-level analytics
- covid_hotspots: Detected hotspot regions
---

## How to Run the Project

1. Clone the repository  
git clone https://github.com/MohamedTarek2412/Bigdata-covid19-real-time.git  
cd Bigdata-covid19-real-time  

2. Start all services  
docker-compose up -d  

---

## Access Services

- Kafka UI: http://localhost:8080  
- NiFi UI: http://localhost:8443  
- Spark Master UI: http://localhost:8083  
- phpMyAdmin: http://localhost:8085  

---

## Default Credentials

NiFi  
Username: admin  
Password: admin1234567890  

MySQL  
Database: covid_db  
User: sa  
Password: P@ssw0rd123  

---

## Key Engineering Concepts Demonstrated

- Event-driven architecture
- Streaming ETL pipelines
- Exactly-once processing
- Window-based analytics
- Real-time forecasting
- Serving layer design
- Dockerized big-data stack
- BI-ready data modeling

---

## Project Level

Advanced / Enterprise-Grade Data Engineering Project

- Real-Time Streaming
- Scalable Architecture
- Business-Ready Analytics
- Production-Oriented Design

---


## Author

Data Engineer | Streaming & Analytics Enthusiast
