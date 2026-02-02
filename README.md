# Real-Time Fraud Detection with Kafka & Flink

A complete real-time stream processing system for fraud detection using Apache Kafka, Apache Flink, FastAPI, and PostgreSQL.

## 🏗️ Architecture

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   FastAPI   │────▶│    Kafka    │────▶│    Flink    │────▶│    Kafka    │
│     API     │     │ transactions│     │  Fraud Job  │     │fraud-alerts │
└─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘
      │                                                              │
      │                                                              │
      ▼                                                              ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                            PostgreSQL                                    │
│                   (transactions + fraud_alerts tables)                   │
└─────────────────────────────────────────────────────────────────────────┘
                                                                    │
                                                                    ▼
                                                            ┌─────────────┐
                                                            │   Alert     │
                                                            │  Service    │
                                                            └─────────────┘
```

## 📋 Components

### 1. **API Service** (FastAPI)
- REST API to receive transaction data
- Publishes transactions to Kafka `transactions` topic
- Stores transactions in PostgreSQL

### 2. **Kafka** (Confluent Platform)
- Message broker with two topics:
  - `transactions`: Raw transaction events
  - `fraud-alerts`: Detected fraud alerts
- Zookeeper for Kafka coordination

### 3. **Flink** (Apache Flink 1.17)
- Stream processing engine
- Reads from `transactions` topic
- Applies fraud detection rules (SQL)
- Writes alerts to `fraud-alerts` topic
- Components:
  - **JobManager**: Coordinates the Flink jobs
  - **TaskManager**: Executes the tasks

### 4. **Alert Service** (Python)
- Kafka consumer for `fraud-alerts` topic
- Prints alerts to console/logs
- Stores alerts in PostgreSQL

### 5. **PostgreSQL**
- Persistent storage for:
  - Transaction records
  - Fraud alert history

## 🚀 Quick Start

### Prerequisites
- Docker & Docker Compose
- Git

### Step 1: Clone the Repository
```bash
git clone https://github.com/siddiqqulhakim/fds-stream-processing-kafka-flink
cd stream-processing-kafka-flink
```

### Step 2: Start All Services
```bash
docker-compose up -d --build
```

This will start:
- Zookeeper (port 2181)
- Kafka (port 9092)
- PostgreSQL (port 5432)
- FastAPI (port 8000)
- Flink JobManager (port 8081)
- Flink TaskManager
- Alert Service

### Step 3: Wait for Services to Initialize
```bash
# Check if all containers are running
docker ps

# Wait ~30 seconds for all services to be ready
```

### Step 4: Submit Flink Job (Automatic)

**The Flink fraud detection job is automatically submitted when the containers start!**

The `flink-sql-client` service will:
1. Wait 40 seconds for Kafka and Flink to be ready
2. Automatically submit the fraud detection job from `fraud_job.sql`
3. Keep running to maintain the job

**Verify the Job is Running:**

```bash
# Option 1: Check Flink Web UI
# Open http://localhost:8081 in your browser and look for "Running Jobs"

# Option 2: Check via CLI
docker exec stream-processing-kafka-flink-jobmanager-1 flink list
```

**Manual Submission (if needed):**

If the automatic submission fails, you can manually submit:

```bash
# Interactive mode - paste SQL commands
docker exec -it stream-processing-kafka-flink-jobmanager-1 sql-client.sh

# Or submit the file directly (PowerShell)
docker exec -it stream-processing-kafka-flink-jobmanager-1 bash -c "sql-client.sh -f /opt/flink/sql/fraud_job.sql"

# For Bash/Linux
docker exec -it stream-processing-kafka-flink-jobmanager-1 bash -c 'sql-client.sh -f /opt/flink/sql/fraud_job.sql'
```

**Note**: Wait at least 40-60 seconds after `docker-compose up` before testing to ensure all services are ready.

### Step 5: Test the System

#### Send a normal transaction (won't trigger alert):
```bash
# PowerShell
Invoke-WebRequest -Uri "http://localhost:8000/transactions?user_id=john&amount=5000000" -Method POST -UseBasicParsing

# Bash/curl
curl -X POST "http://localhost:8000/transactions?user_id=john&amount=5000000"
```

#### Send a fraudulent transaction (will trigger alert):
```bash
# PowerShell
Invoke-WebRequest -Uri "http://localhost:8000/transactions?user_id=alice&amount=20000000" -Method POST -UseBasicParsing

# Bash/curl
curl -X POST "http://localhost:8000/transactions?user_id=alice&amount=20000000"
```

### Step 6: Monitor Alerts

```bash
# Watch alert service logs in real-time
docker logs -f stream-processing-kafka-flink-alert-1

# You should see:
# 🔥 RAW MESSAGE: ConsumerRecord(topic='fraud-alerts', ...)
# 🚨 FRAUD ALERT SAVED: {'user_id': 'alice', 'reason': 'HIGH_AMOUNT', ...}
```

## 🔧 How to Add/Modify Fraud Detection Rules

### Current Rule
The system currently detects transactions with amounts greater than 10,000,000.

### Modifying the Fraud Rule

Edit the file `flink/fraud_job.sql`:

```sql
-- 🚨 JOB (THIS RUNS THE FLINK DETECTION)
INSERT INTO fraud_alerts
SELECT
  user_id,
  'HIGH_AMOUNT' AS reason,
  event_time AS detected_at
FROM transactions
WHERE amount > 10000000;  -- 👈 Change this threshold
```

### Adding New Fraud Detection Rules

#### Example 1: Detect Multiple Transactions in Short Time Window

Add this to `fraud_job.sql`:

```sql
-- Detect users with 3+ transactions within 5 minutes
INSERT INTO fraud_alerts
SELECT
  user_id,
  'MULTIPLE_TRANSACTIONS' AS reason,
  TUMBLE_END(event_time, INTERVAL '5' MINUTE) AS detected_at
FROM transactions
GROUP BY 
  user_id,
  TUMBLE(event_time, INTERVAL '5' MINUTE)
HAVING COUNT(*) >= 3;
```

#### Example 2: Detect Unusual Amount Patterns

```sql
-- Detect transactions that are 10x the user's average
INSERT INTO fraud_alerts
SELECT
  t.user_id,
  'UNUSUAL_AMOUNT' AS reason,
  t.event_time AS detected_at
FROM transactions t
WHERE t.amount > (
  SELECT AVG(amount) * 10
  FROM transactions
  WHERE user_id = t.user_id
    AND event_time BETWEEN t.event_time - INTERVAL '1' DAY AND t.event_time
);
```

#### Example 3: Velocity Check (Rapid Succession)

```sql
-- Detect transactions within 1 minute of each other
INSERT INTO fraud_alerts
SELECT
  a.user_id,
  'RAPID_SUCCESSION' AS reason,
  a.event_time AS detected_at
FROM transactions a
INNER JOIN transactions b
  ON a.user_id = b.user_id
  AND b.event_time BETWEEN a.event_time - INTERVAL '1' MINUTE AND a.event_time
  AND a.ts <> b.ts
WHERE a.amount > 1000000;
```

### Applying Rule Changes

After modifying `fraud_job.sql`:

1. **Stop the current Flink job** (if running):
   ```bash
   # Access Flink Web UI at http://localhost:8081
   # Or use CLI to cancel the job
   docker exec stream-processing-kafka-flink-jobmanager-1 flink list
   docker exec stream-processing-kafka-flink-jobmanager-1 flink cancel <job-id>
   ```

2. **Resubmit the job**:
   ```bash
   docker exec -it stream-processing-kafka-flink-jobmanager-1 sql-client.sh -f /opt/flink/sql/fraud_job.sql
   ```

## 📊 Monitoring & Debugging

### View Kafka Topics
```bash
# List all topics
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092

# Consume transactions topic
docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic transactions --from-beginning

# Consume fraud-alerts topic
docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic fraud-alerts --from-beginning
```

### Check Consumer Groups
```bash
docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group alert-service
```

### Access Flink Web UI
Open browser: http://localhost:8081

### Check Database Records
```bash
# View transactions
docker exec stream-processing-kafka-flink-postgres-1 psql -U fraud -d fraud -c "SELECT * FROM transactions ORDER BY id DESC LIMIT 10;"

# View fraud alerts
docker exec stream-processing-kafka-flink-postgres-1 psql -U fraud -d fraud -c "SELECT * FROM fraud_alerts ORDER BY id DESC LIMIT 10;"
```

### View Container Logs
```bash
# API logs
docker logs stream-processing-kafka-flink-api-1

# Alert service logs
docker logs stream-processing-kafka-flink-alert-1

# Flink JobManager logs
docker logs stream-processing-kafka-flink-jobmanager-1

# Follow logs in real-time
docker logs -f <container-name>
```

## 🗃️ Database Schema

### Transactions Table
```sql
CREATE TABLE transactions (
    id SERIAL PRIMARY KEY,
    user_id TEXT,
    amount BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### Fraud Alerts Table
```sql
CREATE TABLE fraud_alerts (
    id SERIAL PRIMARY KEY,
    user_id TEXT,
    reason TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

## 🐛 Troubleshooting

### Issue: "No alerts showing in logs"
**Solution**: Check if Python output buffering is enabled. The alert service uses `-u` flag for unbuffered output.

### Issue: "Flink job not processing messages"
**Solution**: 
1. Make sure you've submitted the Flink job: `docker exec -it stream-processing-kafka-flink-jobmanager-1 sql-client.sh -f /opt/flink/sql/fraud_job.sql`
2. Check Flink Web UI at http://localhost:8081 to see if the job is running

### Issue: "Kafka connection errors"
**Solution**: Wait 30-60 seconds after `docker-compose up` for Kafka to fully initialize.

### Issue: "Consumer lag in alert service"
**Solution**: Check consumer group status:
```bash
docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group alert-service
```

## 🧪 Testing Scenarios

### Test 1: Normal Transaction
```bash
# Should NOT trigger alert (amount < 10,000,000)
curl -X POST "http://localhost:8000/transactions?user_id=user1&amount=5000000"
```

### Test 2: High Amount Fraud
```bash
# Should trigger HIGH_AMOUNT alert
curl -X POST "http://localhost:8000/transactions?user_id=user2&amount=50000000"
```

### Test 3: Multiple Frauds
```bash
# Send 5 fraudulent transactions
for i in {1..5}; do
  curl -X POST "http://localhost:8000/transactions?user_id=user3&amount=15000000"
  sleep 1
done
```

## 🛑 Stopping the System

```bash
# Stop all services
docker-compose down

# Stop and remove volumes (clean slate)
docker-compose down -v
```

## 📁 Project Structure

```
stream-processing-kafka-flink/
├── api/
│   ├── main.py              # FastAPI application
│   ├── requirements.txt     # Python dependencies
│   └── Dockerfile           # API container config
├── alert/
│   ├── alert_service.py     # Alert consumer service
│   ├── requirements.txt     # Python dependencies
│   └── Dockerfile           # Alert service container config
├── flink/
│   ├── fraud_job.sql        # Flink SQL fraud detection rules
│   └── Dockerfile           # Flink container config
├── db/
│   └── init.sql             # PostgreSQL initialization script
├── docker-compose.yaml      # Docker orchestration
└── README.md                # This file
```

## 🔐 Security Notes

**⚠️ For Production Use:**
- Change default PostgreSQL credentials
- Use proper Kafka authentication (SASL/SSL)
- Implement API authentication/authorization
- Use secrets management (e.g., Docker secrets, HashiCorp Vault)
- Enable Flink security features
- Add rate limiting to API endpoints

## 📚 Learn More

- [Apache Flink Documentation](https://flink.apache.org/)
- [Apache Kafka Documentation](https://kafka.apache.org/)
- [FastAPI Documentation](https://fastapi.tiangolo.com/)
- [Flink SQL Connector for Kafka](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/connectors/table/kafka/)