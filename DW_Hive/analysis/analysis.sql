--------- Part 3: Analytical Queries (The "Risk Engine") -------------
---- 1. Ingestion Performance (Observability)
---- Check if your pipeline is slowing down (Network vs. Spark):

SELECT 
    dt, hr,
    AVG(source_to_kafka_lag_sec) as avg_network_lag,
    AVG(kafka_to_spark_lag_sec) as avg_spark_lag
FROM bronze_transactions
GROUP BY dt, hr
ORDER BY dt DESC, hr DESC;

---- 2. Fraud Heatmap (Business Value)
---- Find which cities are currently under attack:
SELECT tx_location, COUNT(*) as alert_count, SUM(amount) as value_at_risk
FROM silver_fraud_alerts
WHERE dt = CURRENT_DATE()
GROUP BY tx_location
ORDER BY alert_count DESC;


-- 3. DLQ Audit (Compliance)
-- Check how much money is being "rejected" from real-time processing because it's late:
SQL
SELECT dt, COUNT(*) as late_count, SUM(amount) as lost_realtime_visibility_amount
FROM dlq_transactions
GROUP BY dt;


-- 4. Top 5 High-Risk Accounts (Security Ops)
-- Find the specific accounts that triggered the most alerts in the last hour:

SELECT account_id, COUNT(*) as total_alerts, MAX(risk_score) as peak_score
FROM silver_fraud_alerts
WHERE dt = CURRENT_DATE()
GROUP BY account_id
HAVING total_alerts > 2
ORDER BY total_alerts DESC;


-- Part 4: Loading the Gold Layer (The Transformation)
-- This is the SQL code you put inside your Airflow DAG to populate your final reports:

-- Populate Location Risk
INSERT OVERWRITE TABLE gold_location_risk PARTITION (dt='2026-02-04')
SELECT tx_location, COUNT(*), SUM(amount), AVG(risk_score)
FROM silver_fraud_alerts
WHERE dt = '2026-02-04'
GROUP BY tx_location;

-- Populate Daily KPIs
INSERT OVERWRITE TABLE gold_daily_kpi_report PARTITION (dt='2026-02-04')
SELECT 
    COUNT(*), 
    SUM(CASE WHEN risk_score >= 50 THEN 1 ELSE 0 END), 
    (SUM(CASE WHEN risk_score >= 50 THEN 1 ELSE 0 END) / COUNT(*)) * 100,
    SUM(CASE WHEN risk_score >= 50 THEN amount ELSE 0 END)
FROM bronze_transactions
WHERE dt = '2026-02-04';