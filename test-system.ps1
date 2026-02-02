# Test Script for Fraud Detection System
# Run this after starting docker-compose up -d

Write-Host "==================================" -ForegroundColor Cyan
Write-Host "Fraud Detection System Test Script" -ForegroundColor Cyan
Write-Host "==================================" -ForegroundColor Cyan
Write-Host ""

# Step 1: Check container status
Write-Host "Step 1: Checking container status..." -ForegroundColor Yellow
$containers = docker ps --format "{{.Names}}" 2>$null
if ($containers) {
    Write-Host "Running containers:" -ForegroundColor Green
    $containers | ForEach-Object { Write-Host "  - $_" -ForegroundColor White }
} else {
    Write-Host "ERROR: No containers running!" -ForegroundColor Red
    exit 1
}
Write-Host ""

# Step 2: Wait for services
Write-Host "Step 2: Waiting 10 seconds for services to stabilize..." -ForegroundColor Yellow
Start-Sleep -Seconds 10
Write-Host ""

# Step 3: Check API health
Write-Host "Step 3: Testing API endpoint..." -ForegroundColor Yellow
try {
    $response = Invoke-WebRequest -Uri "http://localhost:8000/docs" -UseBasicParsing -TimeoutSec 5 -ErrorAction Stop
    Write-Host "[OK] API is responding (Status: $($response.StatusCode))" -ForegroundColor Green
} catch {
    Write-Host "[ERROR] API is not responding!" -ForegroundColor Red
    Write-Host "  Error: $($_.Exception.Message)" -ForegroundColor Red
}
Write-Host ""

# Step 4: Check Flink Web UI
Write-Host "Step 4: Testing Flink Web UI..." -ForegroundColor Yellow
try {
    $response = Invoke-WebRequest -Uri "http://localhost:8081" -UseBasicParsing -TimeoutSec 5 -ErrorAction Stop
    Write-Host "[OK] Flink Web UI is accessible (Status: $($response.StatusCode))" -ForegroundColor Green
    Write-Host "  Visit: http://localhost:8081" -ForegroundColor Cyan
} catch {
    Write-Host "[ERROR] Flink Web UI is not responding!" -ForegroundColor Red
}
Write-Host ""

# Step 5: Check Flink job status
Write-Host "Step 5: Checking Flink job status..." -ForegroundColor Yellow
$flinkJobs = docker exec stream-processing-kafka-flink-jobmanager-1 flink list 2>&1
if ($flinkJobs -match "RUNNING") {
    Write-Host "[OK] Flink job is RUNNING" -ForegroundColor Green
} else {
    Write-Host "[WARNING] No Flink job running - needs manual submission" -ForegroundColor Yellow
    Write-Host ""
    Write-Host "To submit the job manually, run:" -ForegroundColor Cyan
    Write-Host '  docker exec -it stream-processing-kafka-flink-jobmanager-1 bash -c "sql-client.sh -f /opt/flink/sql/fraud_job.sql"' -ForegroundColor White
}
Write-Host ""

# Step 6: Check Kafka topics
Write-Host "Step 6: Checking Kafka topics..." -ForegroundColor Yellow
$topics = docker exec kafka kafka-topics --list --bootstrap-server localhost:9092 2>&1
if ($topics -match "transactions" -and $topics -match "fraud-alerts") {
    Write-Host "Kafka topics exist: transactions, fraud-alerts" -ForegroundColor Green
} else {
    Write-Host "Kafka topics not found!" -ForegroundColor Red
}
Write-Host ""

# Step 7: Test transactions
Write-Host "Step 7: Testing transactions..." -ForegroundColor Yellow
Write-Host "  Sending normal transaction (amount=5000000)..." -ForegroundColor White
try {
    $response = Invoke-WebRequest -Uri "http://localhost:8000/transactions?user_id=testuser1&amount=5000000" -Method POST -UseBasicParsing -TimeoutSec 5 -ErrorAction Stop
    Write-Host "  [OK] Normal transaction sent successfully" -ForegroundColor Green
} catch {
    Write-Host "  [ERROR] Failed to send transaction" -ForegroundColor Red
}

Write-Host ""
Write-Host "  Sending fraudulent transaction (amount=25000000)..." -ForegroundColor White
try {
    $response = Invoke-WebRequest -Uri "http://localhost:8000/transactions?user_id=testuser2&amount=25000000" -Method POST -UseBasicParsing -TimeoutSec 5 -ErrorAction Stop
    Write-Host "  [OK] Fraudulent transaction sent successfully" -ForegroundColor Green
} catch {
    Write-Host "  [ERROR] Failed to send transaction" -ForegroundColor Red
}
Write-Host ""

# Step 8: Wait and check alerts
Write-Host "Step 8: Waiting 5 seconds for Flink to process..." -ForegroundColor Yellow
Start-Sleep -Seconds 5
Write-Host ""

Write-Host "Step 9: Checking alert service logs..." -ForegroundColor Yellow
$alertLogs = docker logs stream-processing-kafka-flink-alert-1 --tail 20 2>&1
if ($alertLogs -match "FRAUD ALERT SAVED") {
    Write-Host "[OK] FRAUD ALERTS DETECTED!" -ForegroundColor Green
    $alertLogs | Select-String "FRAUD ALERT SAVED" | ForEach-Object {
        Write-Host "  $_" -ForegroundColor Cyan
    }
} else {
    Write-Host "[WARNING] No fraud alerts in logs yet" -ForegroundColor Yellow
    Write-Host "  This could mean:" -ForegroundColor White
    Write-Host "    - Flink job not running (check step 5)" -ForegroundColor White
    Write-Host "    - Job needs more time to process" -ForegroundColor White
}
Write-Host ""

# Step 10: Check database
Write-Host "Step 10: Checking PostgreSQL database..." -ForegroundColor Yellow
$transactions = docker exec stream-processing-kafka-flink-postgres-1 psql -U fraud -d fraud -c "SELECT COUNT(*) FROM transactions;" 2>&1 | Select-String -Pattern "^\s*\d+\s*$"
$alerts = docker exec stream-processing-kafka-flink-postgres-1 psql -U fraud -d fraud -c "SELECT COUNT(*) FROM fraud_alerts;" 2>&1 | Select-String -Pattern "^\s*\d+\s*$"

if ($transactions) {
    Write-Host "  [OK] Transactions in database: $($transactions.Line.Trim())" -ForegroundColor Green
}
if ($alerts) {
    Write-Host "  [OK] Fraud alerts in database: $($alerts.Line.Trim())" -ForegroundColor Green
}
Write-Host ""

# Final summary
Write-Host "==================================" -ForegroundColor Cyan
Write-Host "Test Complete!" -ForegroundColor Cyan
Write-Host "==================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Next steps:" -ForegroundColor Yellow
Write-Host "  1. Check Flink Web UI: http://localhost:8081" -ForegroundColor White
Write-Host "  2. Check API docs: http://localhost:8000/docs" -ForegroundColor White
Write-Host "  3. Monitor alerts: docker logs -f stream-processing-kafka-flink-alert-1" -ForegroundColor White
Write-Host ""

