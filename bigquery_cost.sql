#calculate bigquery pricing

SELECT 
      project_id,
      user_email,
      job_id,
      destination_table.dataset_id AS destination_table_dataset_id,
      destination_table.table_id AS destination_table_id,
      DateTime(creation_time) AS Date_Time,
      job_type,
      statement_type,
      priority,
      query,
      state,
      total_bytes_processed,
      total_bytes_billed,
      (total_bytes_processed / POWER(2,30)) AS processed_amount_gb,
      (total_bytes_billed / POWER(2,30)) AS billed_amount_gb,
      CAST(((total_bytes_billed / POWER(2,40)) * 6.25) AS numeric) AS cost_usd
FROM region-us.INFORMATION_SCHEMA.JOBS 
WHERE 
    Date(creation_time) >= '2023-03-09'
    AND total_bytes_billed IS NOT NULL
ORDER BY creation_time DESC, total_bytes_billed DESC
