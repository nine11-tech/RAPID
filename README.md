# RAPID

### Real-time Attack Pattern Identification & Detection

RAPID is a distributed cybersecurity analytics platform built with a Lambda-style architecture. It ingests network security logs, stores raw and processed data, runs batch and streaming Spark jobs, and exposes the resulting threat intelligence through a Flask API and dashboard.

The current infrastructure is not a single-machine Docker Compose stack. It is a multi-host deployment that uses Tailscale VPN addresses for stable host-to-host communication, Docker Swarm for the shared overlay network, and Docker Compose profiles so each team member runs only the services they own.

---

## Infrastructure Architecture

```text
                         Tailscale VPN network
                 100.x host addresses shared by all nodes
                                  |
                    Docker Swarm attachable overlay
                            rapid-overlay
                                  |
        +-------------------------+--------------------------+
        |                         |                          |
   Anass node                Hamza node                 Khalid node
  100.73.216.115           100.72.34.26              100.127.99.23
        |                         |                          |
  Zookeeper                  Spark master              Spark streaming
  Kafka :9092                Spark worker              detection jobs
  HDFS NameNode              Jupyter :8888             Jupyter :8889
  HDFS DataNode              Spark UI :8080            Spark UI :4040
  Flask API :5000
        |
        +--------------------------------------------------+
                                                           |
                                                     Chawi node
                                                   100.97.208.110
                                                           |
                                                    Cassandra :9042
                                                    HBase Thrift :9090
                                                    HBase UI :16010
                                                    Dashboard :3000
```

### Data Flow

```text
Kaggle CSV / HDFS partitions
          |
          +--> Spark batch jobs ------------------> HBase historical views
          |                                         ip_reputation
          |                                         attack_patterns
          |                                         threat_timeline
          |
          +--> Kafka producer --------------------> Kafka topic
                                                    cybersecurity-logs
                                                        |
                                                  Spark streaming jobs
                                                        |
                                               Cassandra realtime views
                                               logs
                                               realtime_threats
                                               signature_alerts
                                               volume_alerts
                                               threat_scores
                                                        |
                                           Flask API + dashboard consumers
```

---

## Service Ownership

The deployment is split by Docker Compose profiles in [docker/docker-compose.yml](/mnt/c/Users/anass/Desktop/RAPID/docker/docker-compose.yml).

| Owner | Tailscale IP | Compose profile | Services |
|---|---:|---|---|
| Anass | `100.73.216.115` | `anass` | `zookeeper`, `kafka`, `namenode`, `datanode`, `flask-api` |
| Hamza | `100.72.34.26` | `hamza` | `spark-master`, `spark-worker` |
| Khalid | `100.127.99.23` | `khalid` | `spark-streaming` |
| Chawi | `100.97.208.110` | `chawi` | `cassandra`, `hbase`, `nginx-dashboard` |

The service containers communicate through two layers:

- Tailscale provides stable private host addresses between team machines.
- Docker Swarm provides the external attachable overlay network named `rapid-overlay`.

---

## Tech Stack

| Layer | Technology | Current role |
|---|---|---|
| VPN | Tailscale | Private connectivity between team machines |
| Container orchestration | Docker Swarm + Docker Compose profiles | Shared overlay network and per-owner service startup |
| Ingestion | Kafka + Zookeeper | Streaming topic `cybersecurity-logs` |
| Raw storage | HDFS | Partitioned logs under `/logs/year=2024/month=XX/` |
| Batch processing | PySpark | Historical analytics and HBase writers |
| Speed processing | Spark Structured Streaming | Real-time detection from Kafka |
| Historical serving store | HBase | Reputation, attack pattern, and timeline tables |
| Real-time serving store | Cassandra | Logs, alerts, threat scores, and active threats |
| API | Flask + Flask-CORS | Threat lookup and dashboard endpoints |
| Dashboard | Nginx static hosting | Serves dashboard assets on Chawi's node |

---

## Prerequisites

- Docker Engine or Docker Desktop with Swarm support.
- Tailscale installed and connected on every participating machine.
- Git.
- Python 3 for local helper scripts.
- Kaggle API credentials if downloading the dataset from Kaggle.

Each machine must be able to reach the other Tailscale IPs used by the stack.

---

## Environment

Create `docker/.env` on each machine. Keep Kaggle credentials private and do not commit this file.

```env
ANASS_IP=100.73.216.115
HAMZA_IP=100.72.34.26
KHALID_IP=100.127.99.23
CHAWI_IP=100.97.208.110

KAGGLE_USERNAME=your_kaggle_username
KAGGLE_KEY=your_kaggle_api_key
```

Only `ANASS_IP` and `CHAWI_IP` are currently consumed directly by the Compose services, but keeping all node addresses in one file makes the deployment easier to reason about.

---

## Swarm And Overlay Network Setup

Run this once from the Swarm manager node, normally Anass' machine:

```bash
docker swarm init --advertise-addr 100.73.216.115
docker network create --driver overlay --attachable rapid-overlay
```

On the other machines, join the Swarm using the command printed by `docker swarm init`. If you need to print it again:

```bash
docker swarm join-token worker
```

Verify that the overlay network exists:

```bash
docker network ls | grep rapid-overlay
```

The Compose file expects `rapid-overlay` to already exist because it is declared as an external network.

---

## Start The Infrastructure

Each team member starts only their profile from the `docker/` directory.

```bash
cd docker
docker compose --profile anass up -d
docker compose --profile hamza up -d
docker compose --profile khalid up -d
docker compose --profile chawi up -d
```

In normal use, each command is run on the matching owner's machine, not all on one laptop.

Useful service URLs:

| Service | URL |
|---|---|
| HDFS NameNode UI | `http://100.73.216.115:9870` |
| Kafka broker | `100.73.216.115:9092` |
| Flask API | `http://100.73.216.115:5000` |
| Spark master UI | `http://100.72.34.26:8080` |
| Hamza Jupyter | `http://100.72.34.26:8888` |
| Khalid Jupyter | `http://100.127.99.23:8889` |
| HBase UI | `http://100.97.208.110:16010` |
| HBase Thrift | `100.97.208.110:9090` |
| Cassandra | `100.97.208.110:9042` |
| Dashboard | `http://100.97.208.110:3000` |

---

## Dataset

Source: [Kaggle - Cybersecurity Threat Detection Logs](https://www.kaggle.com/datasets/aryan208/cybersecurity-threat-detection-logs)

Main fields:

| Field | Description |
|---|---|
| `timestamp` | Event date/time |
| `source_ip` | Source IP address |
| `dest_ip` | Destination IP address |
| `protocol` | Network/application protocol |
| `action` | Allowed or blocked action |
| `threat_label` | `benign`, `suspicious`, or `malicious` |
| `log_type` | Log source |
| `bytes_transferred` | Traffic volume |
| `user_agent` | Client/tool identifier |
| `request_path` | Requested path or payload indicator |

The repository includes [docker/init.sh](/mnt/c/Users/anass/Desktop/RAPID/docker/init.sh) for Kaggle download automation. The current Compose file does not define an `init` service, so run the script through a temporary Python container or download the CSV manually into `data/raw/`.

Example one-off download:

```bash
docker run --rm \
  --env-file docker/.env \
  -v "$PWD/data:/data" \
  -v "$PWD/docker/init.sh:/init.sh:ro" \
  python:3.11-slim \
  sh /init.sh
```

---

## HDFS Layout

Streaming and producer code expects monthly HDFS partitions in this shape:

```text
/logs/year=2024/month=01/data.csv
/logs/year=2024/month=02/data.csv
...
/logs/year=2024/month=12/data.csv
```

The producer reads those files through WebHDFS at:

```text
http://100.73.216.115:9870/webhdfs/v1/logs/year=2024/month=XX/data.csv?op=OPEN&user.name=root
```

Create the partition directories from the NameNode container:

```bash
docker exec namenode hdfs dfs -mkdir -p \
  /logs/year=2024/month=01 /logs/year=2024/month=02 \
  /logs/year=2024/month=03 /logs/year=2024/month=04 \
  /logs/year=2024/month=05 /logs/year=2024/month=06 \
  /logs/year=2024/month=07 /logs/year=2024/month=08 \
  /logs/year=2024/month=09 /logs/year=2024/month=10 \
  /logs/year=2024/month=11 /logs/year=2024/month=12 \
  /data/cybersecurity/batch /data/cybersecurity/streaming
```

Load `data.csv` into each month partition before running the Kafka replay producer.

---

## Running The Pipeline

### 1. Start Core Services

Anass starts Kafka, Zookeeper, HDFS, and the API:

```bash
cd docker
docker compose --profile anass up -d
```

Chawi starts Cassandra, HBase, and dashboard hosting:

```bash
cd docker
docker compose --profile chawi up -d
```

Hamza and Khalid start their Spark profiles:

```bash
cd docker
docker compose --profile hamza up -d
docker compose --profile khalid up -d
```

### 2. Create Serving Tables

Create or verify HBase tables:

```bash
python3 hbase/create_hbase_tables.py
```

Expected HBase tables include:

- `ip_reputation`
- `attack_patterns`
- `threat_timeline`

Cassandra uses the `cybersecurity` keyspace. Streaming/API code expects these tables:

- `logs`
- `realtime_threats`
- `signature_alerts`
- `volume_alerts`
- `threat_scores`

### 3. Run Batch Jobs

Batch jobs live in `spark/batch/` and write historical views to HBase and/or HDFS.

Important jobs:

- `top10_malicious_ips.py`
- `port_scan_detection.py`
- `attack_path_analysis.py`
- `hbase_threat_timeline.py`
- `multistep_attack_detection.py`
- `volume_by_threat.py`
- `ml_bonus.py`

### 4. Replay Logs Into Kafka

The producer reads monthly HDFS CSV files and publishes JSON events to `cybersecurity-logs`:

```bash
python3 spark/speed_layer/kafka_producer.py
```

It stores progress in `/tmp/kafka_producer_state.json`, so interrupted runs can resume.

### 5. Run Streaming Jobs

Base Kafka-to-Cassandra writer:

```bash
docker exec spark-streaming spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 \
  /home/jovyan/work/speed_layer/spark_streaming_writer.py
```

Detection jobs:

```bash
docker exec spark-streaming spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0 \
  /home/jovyan/work/streaming/brute_force_detection.py

docker exec spark-streaming spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0 \
  /home/jovyan/work/streaming/attack_signature_detection.py

docker exec spark-streaming spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0 \
  /home/jovyan/work/streaming/threat_score.py

docker exec spark-streaming spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0 \
  /home/jovyan/work/streaming/volume_detection.py
```

---

## API

The Flask API runs as `flask-api` on Anass' node.

Base URL:

```text
http://100.73.216.115:5000
```

Endpoints:

| Endpoint | Purpose |
|---|---|
| `GET /health` | Service health and configured backends |
| `GET /threats/ip/<ip>` | Combined Cassandra + HBase view for one source IP |
| `GET /threats/top10` | Top scored source IPs from Cassandra |
| `GET /threats/threshold` | Adaptive rolling 24h threshold |
| `GET /threats/recent` | Recent signature alerts |
| `GET /threats/volume-alerts` | Recent volume alerts |
| `GET /threats/by-protocol` | Aggregated threat counts by protocol |
| `GET /threats/timeline` | Threat counts by day |
| `GET /threats/geo/attacks` | Public-IP attack map data with geolocation |

Run the endpoint smoke test:

```bash
API_BASE=http://100.73.216.115:5000 \
ORIGIN=http://100.97.208.110:3000 \
bash scripts/script_endpoints_test.sh
```

Run the HBase/API integration test:

```bash
bash scripts/test_s3_api_hbase.sh
```

---

## Project Structure

```text
RAPID/
├── bigdata-api/
│   ├── app.py                 # Flask serving API
│   └── requirements.txt
├── cassandra/
│   └── verify_task_5_and_6.md # Cassandra/Kafka/Spark verification commands
├── data/
│   ├── raw/                   # Downloaded CSV data
│   └── processed/             # Local processed outputs
├── docker/
│   ├── docker-compose.yml     # Swarm/Tailscale profile-based infrastructure
│   ├── hadoop-config/         # Hadoop XML config
│   ├── hadoop.env
│   └── init.sh                # Kaggle dataset downloader
├── hbase/
│   └── create_hbase_tables.py # HBase table bootstrap
├── scripts/
│   ├── demo_e2e.py
│   ├── script_endpoints_test.sh
│   └── test_s3_api_hbase.sh
├── spark/
│   ├── batch/                 # Historical Spark jobs
│   ├── speed_layer/           # Kafka producer and base stream writer
│   └── streaming/             # Real-time detection jobs
└── report/
    └── sections/              # LaTeX report sections
```

---

## Current Implementation Status

Completed or present in the repository:

- Tailscale-aware distributed service layout.
- Docker Compose profiles for per-owner deployment.
- External Swarm overlay network `rapid-overlay`.
- Kafka topic flow for `cybersecurity-logs`.
- HDFS monthly partition convention.
- Spark batch jobs for malicious IPs, port scans, attack patterns, timelines, volume, multi-step attacks, and ML bonus work.
- Spark streaming jobs for raw log persistence, brute-force detection, attack signatures, volume alerts, and threat scores.
- HBase table creation and batch writers.
- Flask API with merged Cassandra/HBase threat lookup, adaptive thresholding, timeline, protocol, volume, recent alerts, and geo attack endpoints.
- Endpoint and integration test scripts.

Known documentation/implementation gaps to watch:

- `docker/init.sh` exists, but `docker-compose.yml` no longer defines an `init` service.
- The Compose file references `../dashboard` for `nginx-dashboard`; make sure that directory exists on Chawi's machine before starting the profile.
- Several scripts still hard-code the current Tailscale IPs. If IP ownership changes, update the scripts or pass environment variables where supported.

---

## Security Notes

- Do not commit `docker/.env`; it contains Kaggle credentials.
- Keep Cassandra, HBase, Kafka, and API traffic on Tailscale/private networking.
- The API enables broad CORS for dashboard/demo use. Restrict CORS origins before exposing the service beyond the private project network.
- The geo endpoint calls `ip-api.com` for public IP geolocation and caches results in `/tmp/rapid_geo_cache.json`.

---

## Git Workflow

```bash
git checkout dev
git pull
git checkout -b feature/your-task-name

git add .
git commit -m "feat(scope): short description"
git push origin feature/your-task-name
```

Rules:

- Do not push directly to `main`.
- Use one branch per task.
- Open pull requests to `dev`.
- Merge to `main` only at delivery checkpoints.
