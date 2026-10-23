SELECT created_at::date AS transaction_date,
       count(*)::bigint AS transaction_count,
       count(*) FILTER (WHERE status = 'SUCCESS')::bigint AS successful_count,
       coalesce(sum(amount) FILTER (WHERE status = 'SUCCESS'), 0)::numeric AS successful_amount
FROM core.transactions
GROUP BY created_at::date
ORDER BY created_at::date
