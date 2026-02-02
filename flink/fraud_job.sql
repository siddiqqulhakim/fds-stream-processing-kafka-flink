-- SOURCE
CREATE TABLE transactions (
  user_id STRING,
  amount BIGINT,
  ts BIGINT,
  event_time AS TO_TIMESTAMP_LTZ(ts, 3),
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'transactions',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'flink-fraud-detector',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json'
);

-- SINK
CREATE TABLE fraud_alerts (
  user_id STRING,
  reason STRING,
  detected_at TIMESTAMP(3)
) WITH (
  'connector' = 'kafka',
  'topic' = 'fraud-alerts',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json'
);

-- 🚨 JOB (INI YANG MENJALANKAN FLINK)
INSERT INTO fraud_alerts
SELECT
  user_id,
  'HIGH_AMOUNT' AS reason,
  event_time AS detected_at
FROM transactions
WHERE amount > 10000000;