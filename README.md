# RAPID — Real-time Attack Pattern Identification & Detection

## Architecture
Lambda Architecture:
- Batch Layer (HDFS + Spark + HBase)
- Speed Layer (Kafka + Spark Streaming)
- Serving Layer (API + Dashboard)

## Sprint 1 — Batch Layer
Goal:
- Setup Docker (Hadoop, HBase, Kafka)
- HDFS partitioned storage
- Spark batch analytics
- HBase storage

## Team & Tasks
- Anass → Docker + HDFS
- Hamza → Dataset + Batch jobs
- Khalid → Detection logic
- Chawi → HBase + Aggregations

## Structure
- docker/ → infra
- hadoop/ → HDFS scripts
- spark/batch/ → batch jobs
- hbase/ → tables & config
- data/ → datasets
- docs/ → documentation