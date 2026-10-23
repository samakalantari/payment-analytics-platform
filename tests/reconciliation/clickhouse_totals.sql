SELECT count() AS transaction_count,
       countIf(status = 'SUCCESS') AS successful_count,
       sumIf(amount, status = 'SUCCESS') AS successful_amount
FROM mart.fact_transactions
