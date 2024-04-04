#calculate the diiference between event_timestamp and real timestamp

WITH a AS (
        SELECT TIMESTAMP_DIFF(TIMESTAMP_MICROS(event_timestamp),(SELECT TIMESTAMP_MILLIS(value.int_value) FROM UNNEST(event_params) WHERE key =         
        'hit_timestamp'), MINUTE) AS minute_difference, COUNT(*) AS count
        FROM  `*****.analytics_******.events_2023*` 
        WHERE event_name NOT IN ('session_start', 'user_engagement', 'first_visit')
        AND event_date > '20231102'
        GROUP BY 1)
SELECT a.minute_difference, ROUND(count / SUM(a.count) OVER (), 2) AS hits_share
FROM a
WHERE a.minute_difference IS NOT NULL
ORDER BY minute_difference ASC
