# Real-Time Data Streaming & Lakehouse Platform

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![Apache Kafka](https://img.shields.io/badge/Kafka-3.5+-231F20.svg)](https://kafka.apache.org/)
[![PySpark](https://img.shields.io/badge/PySpark-3.4+-E25A1C.svg)](https://spark.apache.org/)
[![Delta Lake](https://img.shields.io/badge/Delta_Lake-3.0-00ADD8.svg)](https://delta.io/)
[![Databricks](https://img.shields.io/badge/Databricks-Lakehouse-FF3621.svg)](https://databricks.com/)

> **High-throughput enterprise streaming platform leveraging Apache Kafka, PySpark Structured Streaming, and Databricks Delta Lake with CDC ingestion patterns, ACID transactions, automated data quality validation, and Airflow orchestration.**

---

## 🏗️ Architecture & Data Ingestion Flow

```mermaid
flowchart LR
    subgraph Sources["Operational Sources"]
        CDC["CDC Event Streams<br/>(Transactional WAL)"]
        APIs["Microservice APIs"]
    end

    subgraph Kafka["Event Bus"]
        CDC --> KT["Kafka Topic: operational_events"]
        APIs --> KT
    end

    subgraph Spark["PySpark Streaming Engine"]
        KT --> SSS["Structured Streaming Ingestion"]
        SSS --> DQ["Data Quality & Contract Validator"]
        DQ --> Bronze[("Bronze: Delta Lake Raw")]
        Bronze --> Silver[("Silver: Cleaned & Deduplicated")]
        Silver --> Gold[("Gold: Curated Aggregates")]
    end

    subgraph Serving["Downstream Consumers"]
        Gold --> BI["Real-Time Dashboards"]
        Gold --> AI["AI/ML Feature Pipelines"]
    end
```

---

## 🚀 Key Technical Features

* **CDC Pattern Ingestion**: Seamlessly consumes write-ahead change logs from operational databases into Kafka topics.
* **Medallion Lakehouse Architecture**: Bronze (raw append-only), Silver (schema enforcement & deduplicated records), and Gold (pre-computed dimensional aggregations).
* **Fault-Tolerant Checkpointing**: End-to-end exactly-once processing semantics backed by Delta Lake transaction logs.
* **Automated Data Quality Validation**: In-stream schema drift detection, null constraint validation, and quarantine routing.
* **Databricks Workflows & Airflow Orchestration**: Scheduled optimization jobs (`OPTIMIZE` + `ZORDER`) maintaining sub-second query performance.

---

## 📂 Project Structure

```
realtime-lakehouse-streaming-platform/
├── ci/
│   └── ci.yml                 # CI testing pipeline
├── dags/
│   └── lakehouse_maintenance_dag.py # Airflow Delta compaction DAG
├── docs/
│   └── architecture.md        # Technical architectural specification
├── sample_data/
│   └── events.json            # Mock operational events
├── src/
│   ├── __init__.py
│   ├── cdc_producer.py        # Mock CDC transactional producer
│   ├── data_quality.py        # In-stream data contract validator
│   ├── schema.py              # Event StructType schema definitions
│   └── streaming_pipeline.py  # PySpark Structured Streaming engine
├── tests/
│   └── test_streaming.py      # Unit tests
├── docker-compose.yml         # Local Kafka & Zookeeper environment
├── requirements.txt           # Dependencies
└── .gitignore
```

---

## ⚡ Quickstart

```bash
git clone https://github.com/masonmirtech/realtime-lakehouse-streaming-platform.git
cd realtime-lakehouse-streaming-platform
python -m venv venv
.\venv\Scripts\activate
pip install -r requirements.txt
pytest tests/ -v
```

---

## 📜 License
MIT License.
