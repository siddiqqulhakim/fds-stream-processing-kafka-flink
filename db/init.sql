CREATE TABLE transactions (
    id SERIAL PRIMARY KEY,
    user_id TEXT,
    amount BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE fraud_alerts (
    id SERIAL PRIMARY KEY,
    user_id TEXT,
    reason TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
