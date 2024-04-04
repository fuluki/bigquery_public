#calculate dau/mau rate

SELECT COUNT(DISTINCT IF(_table_suffix > FORMAT_DATE('%Y%m%d',DATE_SUB('2021-01-31', INTERVAL 1 day)), user_pseudo_id, NULL)) AS DAU,
       COUNT(DISTINCT IF(_table_suffix > FORMAT_DATE('%Y%m%d',DATE_SUB('2021-01-31', INTERVAL 30 day)), user_pseudo_id, NULL)) AS MAU
FROM `your-project.events_*`
