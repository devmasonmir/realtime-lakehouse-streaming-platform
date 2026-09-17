# Real-Time Lakehouse Streaming Platform Architecture

## System Design
- **Ingestion**: Event-driven streaming from Kafka using PySpark Structured Streaming with a 10-second micro-batch trigger.
- **Medallion Pipeline**:
  - Bronze: Append-only ingestion preserving original CDC metadata.
  - Silver: Merge upserts on primary keys with schema evolution enabled.
  - Gold: Hourly sliding-window summaries partitioned by event date.
- **Compaction & Governance**: Scheduled vacuum and z-order jobs avoid small-file degradation on object storage.
