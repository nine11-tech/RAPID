#  RAPID
### Real-time Attack Pattern Identification & Detection

A cybersecurity threat detection system built on **Lambda Architecture** using Hadoop, Spark, Kafka, HBase and Cassandra for batch and real-time streaming analysis of network logs.


---

##  Architecture

```
                        Network Logs (6M+ events)
                                  │
               ┌──────────────────┴──────────────────┐
               │                                     │
        SPEED LAYER                           BATCH LAYER
               │                                     │
         Kafka Topic                            HDFS Storage
     (cybersecurity-logs)                  (/logs/year=2024/...)
               │                                     │
      Spark Streaming                          Spark Batch
    (real-time detection)                  (historical analysis)
               │                                     │
          Cassandra                               HBase
      (active threats 24h)                  (historical views)
               │                                     │
               └──────────────────┬──────────────────┘
                                  │
                            REST API (Flask)
                                  │
                           Dashboard (HTML/JS)
```

---

##  Tech Stack

| Layer | Technology | Purpose |
|---|---|---|
| Ingestion | Apache Kafka | Real-time log streaming |
| Batch Storage | HDFS (Hadoop 3.2) | Historical log storage |
| Batch Processing | Apache Spark (PySpark) | Batch threat analysis |
| Speed Processing | Spark Streaming | Real-time threat detection |
| Batch Views | HBase | IP reputation, attack patterns |
| Speed Views | Cassandra | Active threats (last 24h) |
| Coordination | Zookeeper | Kafka cluster management |
| Serving | Flask/FastAPI | REST API |
| Visualization | HTML/JS + Chart.js | Threat dashboard |

---

##  Team

| Member | Role | Responsibilities |
|---|---|---|
| **Anass** | Scrum Master + Infra | Docker, HDFS, Kafka setup, REST API |
| **Hamza** | Lead Batch | Data ingestion, Spark batch, Kafka producer |
| **Khalid** | Lead Streaming | Spark Streaming, brute-force & signature detection |
| **Chawi** | Lead Serving | Cassandra, HBase, Dashboard |

---

##  Quick Start

### Prerequisites
- [Docker Desktop](https://www.docker.com/products/docker-desktop/) installed and running
- [Kaggle account](https://www.kaggle.com) (free)
- Git

### Step 1 — Clone the repo
```bash
git clone https://github.com/nine11-tech/RAPID.git
cd RAPID/docker
```

### Step 2 — Setup your Kaggle credentials
```bash
# Copy the example env file
cp .env.example .env
```
Open `.env` and fill in your credentials:
```env
KAGGLE_USERNAME=your_kaggle_username
KAGGLE_KEY=your_kaggle_api_key
```
> Get your key at: https://www.kaggle.com/settings → **API** → **Create New Token**

### Step 3 — Start all infrastructure
```bash
docker-compose up -d
```
Wait ~30 seconds for all services to initialize.

### Step 4 — Download the dataset
```bash
docker-compose up init
```
This downloads `cybersecurity_threat_detection_logs.csv` (~95MB, 1M+ rows) into `data/raw/`.

### Step 5 — Setup HDFS partitions + load data
```bash
docker exec namenode bash -c "
  hdfs dfs -mkdir -p \
    /logs/year=2024/month=01 /logs/year=2024/month=02 \
    /logs/year=2024/month=03 /logs/year=2024/month=04 \
    /logs/year=2024/month=05 /logs/year=2024/month=06 \
    /logs/year=2024/month=07 /logs/year=2024/month=08 \
    /logs/year=2024/month=09 /logs/year=2024/month=10 \
    /logs/year=2024/month=11 /logs/year=2024/month=12 \
    /data/cybersecurity/batch /data/cybersecurity/streaming &&
  hdfs dfs -put -f /data/raw/cybersecurity_threat_detection_logs.csv \
    /logs/year=2024/month=01/ &&
  echo 'HDFS ready!'
"
```

### Step 6 — Verify everything is running
```bash
docker ps
```

You should see all 8 containers running:

| Container | Status | URL |
|---|---|---|
| zookeeper | ✅ Up | — |
| kafka | ✅ Up | localhost:9092 |
| namenode | ✅ Up | http://localhost:9870 |
| datanode | ✅ Up | http://localhost:9864 |
| hbase | ✅ Up | http://localhost:16010 |
| spark-master | ✅ Up | http://localhost:8080 |
| spark-worker | ✅ Up | http://localhost:8888 |
| cassandra | ✅ Up | localhost:9042 |

---

##  Dataset

**Source:** [Kaggle — Cybersecurity Threat Detection Logs](https://www.kaggle.com/datasets/aryan208/cybersecurity-threat-detection-logs)

| Field | Description | Example |
|---|---|---|
| timestamp | Event date/time | 2024-05-01T00:00:00 |
| source_ip | Source IP | 192.168.1.125 |
| dest_ip | Destination IP | 192.168.1.124 |
| protocol | Protocol used | TCP, HTTP, SSH |
| action | Action taken | allowed, blocked |
| threat_label | Threat class | benign, suspicious, malicious |
| log_type | Log source | firewall, ids, application |
| bytes_transferred | Data volume (bytes) | 10889 |
| user_agent | Client used | Nmap, SQLMap, curl |
| request_path | Accessed path | /admin/login.php |

---

##  Project Structure

```
RAPID/
├── docker/
│   ├── docker-compose.yml    # Full infrastructure setup
│   ├── hadoop.env            # Hadoop configuration
│   ├── init.sh               # Kaggle dataset downloader
│   └── .env.example          # Credentials template (copy to .env)
├── data/
│   ├── raw/                  # Downloaded CSV (git-ignored)
│   └── processed/            # Processed outputs
├── spark/
│   └── batch/                # PySpark batch jobs (Sprint 1)
├── kafka/                    # Kafka producer scripts (Sprint 2)
├── hbase/                    # HBase schema & scripts (Sprint 1)
├── scripts/                  # Utility scripts
├── docs/                     # Architecture diagrams & report
└── dashboard/                # HTML/JS dashboard (Sprint 3)
```

---

##  Sprint Roadmap

###  Sprint 1 — Batch Layer (Week 1)
- [x] Docker infrastructure setup
- [x] HDFS partitioned structure (`/logs/year=2024/month=XX/`)
- [x] Dataset ingestion pipeline
- [x] Spark Batch: Top 10 malicious IPs
- [x] Spark Batch: Port scan detection
- [x] Spark Batch: SQLi/XSS pattern extraction
- [x] HBase tables: `ip_reputation`, `attack_patterns`, `threat_timeline`

###  Sprint 2 — Speed Layer (Week 2)
- [ ] Kafka topic: `cybersecurity-logs` (3 partitions)
- [ ] Custom Kafka producer (CSV replay)
- [ ] Spark Streaming: Brute-force detection
- [ ] Spark Streaming: Attack signature detection
- [ ] Spark Streaming: Abnormal volume detection
- [ ] Cassandra: `realtime_threats` schema

###  Sprint 3 — Integration & Delivery (Week 3)
- [ ] REST API: `GET /threats/ip/{ip}`
- [ ] REST API: `GET /threats/top10`
- [ ] Dashboard: Top 10 IPs + attack timeline
- [ ] Dashboard: Real-time threat map
- [ ] Technical report
- [ ] End-to-end demo

---

##  Security Notes

- **Never commit** your `.env` file — it contains your Kaggle API key
- The `.env` file is in `.gitignore` by default
- Each team member uses their own Kaggle credentials
- Use `.env.example` as the template

---

##  Git Workflow

```bash
# Start a new task
git checkout dev
git pull
git checkout -b feature/your-task-name

# Work on your task, then
git add .
git commit -m "feat(batch): top 10 malicious IPs with Spark"
git push origin feature/your-task-name
# Open PR → dev branch, assign 1 reviewer
```

**Rules:**
- Never push directly to `main`
- Every task = one branch
- PR to `dev`, merge to `main` only at sprint end

---

---


