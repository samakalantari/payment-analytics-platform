SELECT toDate(toTimeZone(created_at, 'UTC')) AS transaction_date,
       count() AS transaction_count,
       countIf(status = 'SUCCESS') AS successful_count,
       sumIf(amount, status = 'SUCCESS') AS successful_amount
FROM mart.fact_transactions
GROUP BY transaction_date
ORDER BY transaction_date
